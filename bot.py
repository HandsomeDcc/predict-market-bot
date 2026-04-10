"""
Telegram Bot: Polymarket & predict.fun 交易量變化播報器
每小時偵測兩個平台交易量變化最高的前 10 個盤子，並發送到指定 Telegram 頻道/群組。
"""

import os
import json
import time
import logging
import asyncio
from datetime import datetime, timezone
from pathlib import Path

import httpx
from telegram import Bot
from telegram.constants import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ─── 設定 ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# 環境變數
TG_BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
TG_CHAT_ID = os.environ["TG_CHAT_ID"]  # 頻道: @channel_name 或 chat_id (數字)
PREDICT_FUN_API_KEY = os.environ.get("PREDICT_FUN_API_KEY", "")

# API 端點
POLYMARKET_GAMMA_BASE = "https://gamma-api.polymarket.com"
PREDICT_FUN_BASE = "https://api.predict.fun"

# 快照檔案路徑
SNAPSHOT_DIR = Path(os.environ.get("SNAPSHOT_DIR", "/tmp/pm-snapshots"))
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

POLY_SNAPSHOT = SNAPSHOT_DIR / "polymarket_snapshot.json"
PREDICT_SNAPSHOT = SNAPSHOT_DIR / "predict_snapshot.json"


# ─── Polymarket 資料抓取 ──────────────────────────────────
async def fetch_polymarket_markets(client: httpx.AsyncClient) -> list[dict]:
    """
    透過 Gamma API 取得 Polymarket 所有活躍市場及其交易量。
    回傳格式: [{ "id", "question", "volume", "url" }, ...]
    """
    markets = []
    offset = 0
    limit = 100

    while True:
        try:
            resp = await client.get(
                f"{POLYMARKET_GAMMA_BASE}/markets",
                params={
                    "closed": "false",
                    "archived": "false",
                    "limit": limit,
                    "offset": offset,
                    "order": "volume",
                    "ascending": "false",
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"Polymarket API 錯誤 (offset={offset}): {e}")
            break

        if not data:
            break

        for m in data:
            volume_raw = m.get("volume") or m.get("volumeNum") or 0
            try:
                volume = float(volume_raw)
            except (ValueError, TypeError):
                volume = 0.0

            markets.append({
                "id": str(m.get("id", m.get("condition_id", ""))),
                "question": m.get("question", "Unknown"),
                "volume": volume,
                "url": f"https://polymarket.com/event/{m.get('slug', '')}",
            })

        offset += limit
        # 最多抓取 2000 個市場（避免太久）
        if offset >= 2000 or len(data) < limit:
            break

    logger.info(f"Polymarket: 取得 {len(markets)} 個市場")
    return markets


# ─── predict.fun 資料抓取 ─────────────────────────────────
async def fetch_predict_fun_markets(client: httpx.AsyncClient) -> list[dict]:
    """
    透過 predict.fun API 取得活躍市場及交易量。
    需要 API Key。若無 key 則回傳空列表。
    """
    if not PREDICT_FUN_API_KEY:
        logger.warning("未設定 PREDICT_FUN_API_KEY，跳過 predict.fun")
        return []

    markets = []
    cursor = None

    while True:
        try:
            params: dict = {"limit": 100, "status": "ACTIVE"}
            if cursor:
                params["cursor"] = cursor

            resp = await client.get(
                f"{PREDICT_FUN_BASE}/v1/markets",
                params=params,
                headers={"x-api-key": PREDICT_FUN_API_KEY},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"predict.fun API 錯誤: {e}")
            break

        items = data if isinstance(data, list) else data.get("markets", data.get("data", []))

        for m in items:
            volume_raw = (
                m.get("volume")
                or m.get("volumeUsd")
                or m.get("total_volume")
                or 0
            )
            try:
                volume = float(volume_raw)
            except (ValueError, TypeError):
                volume = 0.0

            market_id = str(m.get("id", m.get("marketId", "")))
            markets.append({
                "id": market_id,
                "question": m.get("title", m.get("question", "Unknown")),
                "volume": volume,
                "url": f"https://predict.fun/market/{market_id}",
            })

        # 翻頁邏輯
        next_cursor = None
        if isinstance(data, dict):
            next_cursor = data.get("nextCursor", data.get("next_cursor"))

        if not next_cursor or len(items) < 100:
            break
        cursor = next_cursor

        if len(markets) >= 2000:
            break

    logger.info(f"predict.fun: 取得 {len(markets)} 個市場")
    return markets


# ─── 快照管理 ────────────────────────────────────────────
def save_snapshot(markets: list[dict], filepath: Path):
    """將市場資料儲存為 JSON 快照"""
    snapshot = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "markets": {m["id"]: m for m in markets},
    }
    filepath.write_text(json.dumps(snapshot, ensure_ascii=False))


def load_snapshot(filepath: Path) -> dict | None:
    """載入上一次的快照"""
    if not filepath.exists():
        return None
    try:
        return json.loads(filepath.read_text())
    except Exception:
        return None


# ─── 計算交易量變化 ──────────────────────────────────────
def compute_volume_changes(
    current: list[dict], previous_snapshot: dict | None
) -> list[dict]:
    """
    比較當前與上一次快照的交易量差異。
    回傳按變化量排序的列表。
    """
    if not previous_snapshot:
        # 首次執行，無法比較，回傳當前 volume 作為 delta
        for m in current:
            m["volume_delta"] = m["volume"]
            m["volume_prev"] = 0
            m["pct_change"] = None
        return sorted(current, key=lambda x: x["volume_delta"], reverse=True)

    prev_markets = previous_snapshot.get("markets", {})

    results = []
    for m in current:
        prev = prev_markets.get(m["id"])
        prev_vol = prev["volume"] if prev else 0
        delta = m["volume"] - prev_vol

        results.append({
            **m,
            "volume_delta": delta,
            "volume_prev": prev_vol,
            "pct_change": (
                round((delta / prev_vol) * 100, 2) if prev_vol > 0 else None
            ),
        })

    return sorted(results, key=lambda x: x["volume_delta"], reverse=True)


# ─── 格式化訊息 ──────────────────────────────────────────
def format_volume_number(n: float) -> str:
    """把數字格式化為易讀的 K/M 格式"""
    if abs(n) >= 1_000_000:
        return f"${n / 1_000_000:,.2f}M"
    elif abs(n) >= 1_000:
        return f"${n / 1_000:,.1f}K"
    else:
        return f"${n:,.0f}"


def build_report(
    poly_changes: list[dict],
    predict_changes: list[dict],
    timestamp: str,
) -> str:
    """組合最終播報訊息（Telegram MarkdownV2 格式太麻煩，用 HTML）"""

    lines = [
        f"<b>📊 預測市場交易量變化報告</b>",
        f"<i>⏰ {timestamp} UTC</i>",
        "",
    ]

    # ── Polymarket Top 10 ──
    lines.append("<b>🟣 Polymarket — 交易量變化 Top 10</b>")
    lines.append("")

    if not poly_changes:
        lines.append("<i>暫無資料（首次執行或 API 異常）</i>")
    else:
        for i, m in enumerate(poly_changes[:10], 1):
            delta_str = format_volume_number(m["volume_delta"])
            vol_str = format_volume_number(m["volume"])
            pct = f" ({m['pct_change']:+.1f}%)" if m["pct_change"] is not None else ""
            arrow = "🔺" if m["volume_delta"] > 0 else "🔻" if m["volume_delta"] < 0 else "➖"

            question = m["question"][:60]
            if len(m["question"]) > 60:
                question += "…"

            lines.append(
                f"{i}. {arrow} <b>{question}</b>\n"
                f"   變化: <code>{delta_str}</code>{pct} │ "
                f"總量: <code>{vol_str}</code>\n"
                f"   🔗 <a href=\"{m['url']}\">查看盤口</a>"
            )
            lines.append("")

    # ── predict.fun Top 10 ──
    lines.append("<b>🟢 predict.fun — 交易量變化 Top 10</b>")
    lines.append("")

    if not predict_changes:
        if not PREDICT_FUN_API_KEY:
            lines.append("<i>⚠️ 未設定 PREDICT_FUN_API_KEY，無法取得資料</i>")
        else:
            lines.append("<i>暫無資料（首次執行或 API 異常）</i>")
    else:
        for i, m in enumerate(predict_changes[:10], 1):
            delta_str = format_volume_number(m["volume_delta"])
            vol_str = format_volume_number(m["volume"])
            pct = f" ({m['pct_change']:+.1f}%)" if m["pct_change"] is not None else ""
            arrow = "🔺" if m["volume_delta"] > 0 else "🔻" if m["volume_delta"] < 0 else "➖"

            question = m["question"][:60]
            if len(m["question"]) > 60:
                question += "…"

            lines.append(
                f"{i}. {arrow} <b>{question}</b>\n"
                f"   變化: <code>{delta_str}</code>{pct} │ "
                f"總量: <code>{vol_str}</code>\n"
                f"   🔗 <a href=\"{m['url']}\">查看盤口</a>"
            )
            lines.append("")

    lines.append("<i>📡 每小時自動更新</i>")
    return "\n".join(lines)


# ─── 主要排程任務 ─────────────────────────────────────────
async def hourly_report():
    """每小時執行一次的主要任務"""
    logger.info("開始抓取市場資料...")

    async with httpx.AsyncClient() as client:
        # 同時抓取兩個平台
        poly_markets, predict_markets = await asyncio.gather(
            fetch_polymarket_markets(client),
            fetch_predict_fun_markets(client),
        )

    # 載入上次快照
    poly_prev = load_snapshot(POLY_SNAPSHOT)
    predict_prev = load_snapshot(PREDICT_SNAPSHOT)

    # 計算變化
    poly_changes = compute_volume_changes(poly_markets, poly_prev)
    predict_changes = compute_volume_changes(predict_markets, predict_prev)

    # 儲存本次快照
    save_snapshot(poly_markets, POLY_SNAPSHOT)
    save_snapshot(predict_markets, PREDICT_SNAPSHOT)

    # 組合訊息
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    message = build_report(poly_changes, predict_changes, now_str)

    # 發送到 Telegram
    bot = Bot(token=TG_BOT_TOKEN)
    try:
        await bot.send_message(
            chat_id=TG_CHAT_ID,
            text=message,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        logger.info("✅ 報告已發送到 Telegram")
    except Exception as e:
        logger.error(f"發送 Telegram 訊息失敗: {e}")


# ─── 啟動 ────────────────────────────────────────────────
async def main():
    logger.info("🚀 Bot 啟動中...")

    # 啟動時先跑一次
    await hourly_report()

    # 設定排程：每小時整點執行
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(hourly_report, "cron", minute=0)
    scheduler.start()

    logger.info("⏰ 排程已設定：每小時整點執行")

    # 保持運行
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot 已停止")
        scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
