"""
Telegram Bot: Polymarket & predict.fun 交易量變化播報器
支援私訊互動 + 每小時自動播報
指令：
  /start  — 啟動 Bot，記錄你的 Chat ID
  /report — 立即取得最新報告
  /stop   — 取消訂閱自動播報
"""

import os
import json
import logging
import asyncio
from datetime import datetime, timezone
from pathlib import Path

import httpx
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode

# ─── 設定 ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
PREDICT_FUN_API_KEY = os.environ.get("PREDICT_FUN_API_KEY", "")

if not TG_BOT_TOKEN:
    logger.error("❌ 未設定 TG_BOT_TOKEN 環境變數！")
    exit(1)

POLYMARKET_GAMMA_BASE = "https://gamma-api.polymarket.com"
PREDICT_FUN_BASE = "https://api.predict.fun"

SNAPSHOT_DIR = Path(os.environ.get("SNAPSHOT_DIR", "/tmp/pm-snapshots"))
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

POLY_SNAPSHOT = SNAPSHOT_DIR / "polymarket_snapshot.json"
PREDICT_SNAPSHOT = SNAPSHOT_DIR / "predict_snapshot.json"
SUBSCRIBERS_FILE = SNAPSHOT_DIR / "subscribers.json"


# ─── 訂閱者管理 ──────────────────────────────────────────
def load_subscribers() -> set:
    if SUBSCRIBERS_FILE.exists():
        try:
            return set(json.loads(SUBSCRIBERS_FILE.read_text()))
        except Exception:
            pass
    return set()


def save_subscribers(subs: set):
    SUBSCRIBERS_FILE.write_text(json.dumps(list(subs)))


subscribers: set = load_subscribers()


# ─── Polymarket 資料抓取 ──────────────────────────────────
def _build_polymarket_url(m: dict) -> str:
    """根據 API 回傳的欄位建構正確的 Polymarket 網址"""
    # 優先用 event slug（事件頁面）
    event_slug = m.get("event_slug") or m.get("eventSlug") or ""
    if event_slug:
        return f"https://polymarket.com/event/{event_slug}"
    # 其次用 market slug
    slug = m.get("slug") or m.get("market_slug") or ""
    if slug:
        return f"https://polymarket.com/event/{slug}"
    # 最後用 condition_id
    cid = m.get("condition_id") or m.get("conditionId") or m.get("id") or ""
    return f"https://polymarket.com/market/{cid}"


async def fetch_polymarket_markets(client: httpx.AsyncClient) -> list[dict]:
    """
    先嘗試 /events 端點（URL 最準確），若失敗再降級到 /markets。
    """
    markets = await _fetch_polymarket_events(client)
    if not markets:
        logger.info("events 端點無資料，改用 /markets")
        markets = await _fetch_polymarket_markets_fallback(client)
    return markets


async def _fetch_polymarket_events(client: httpx.AsyncClient) -> list[dict]:
    """透過 /events 端點抓取，URL 格式保證正確"""
    markets = []
    offset = 0
    limit = 100

    while True:
        try:
            resp = await client.get(
                f"{POLYMARKET_GAMMA_BASE}/events",
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
            logger.error(f"Polymarket Events API 錯誤 (offset={offset}): {e}")
            break

        if not data:
            break

        for ev in data:
            # events 端點：每個 event 可能包含多個 markets
            slug = ev.get("slug", "")
            url = f"https://polymarket.com/event/{slug}" if slug else ""

            # 取 event 層級的總交易量
            volume_raw = ev.get("volume") or ev.get("volumeNum") or 0
            try:
                volume = float(volume_raw)
            except (ValueError, TypeError):
                volume = 0.0

            title = ev.get("title") or ev.get("question") or "Unknown"

            markets.append({
                "id": str(ev.get("id", "")),
                "question": title,
                "volume": volume,
                "url": url,
            })

        offset += limit
        if offset >= 2000 or len(data) < limit:
            break

    logger.info(f"Polymarket (events): 取得 {len(markets)} 個事件")
    return markets


async def _fetch_polymarket_markets_fallback(client: httpx.AsyncClient) -> list[dict]:
    """降級方案：用 /markets 端點"""
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
            logger.error(f"Polymarket Markets API 錯誤 (offset={offset}): {e}")
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
                "url": _build_polymarket_url(m),
            })

        offset += limit
        if offset >= 2000 or len(data) < limit:
            break

    logger.info(f"Polymarket (markets fallback): 取得 {len(markets)} 個市場")
    return markets


# ─── predict.fun 資料抓取 ─────────────────────────────────
async def fetch_predict_fun_markets(client: httpx.AsyncClient) -> list[dict]:
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
    snapshot = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "markets": {m["id"]: m for m in markets},
    }
    filepath.write_text(json.dumps(snapshot, ensure_ascii=False))


def load_snapshot(filepath: Path) -> dict | None:
    if not filepath.exists():
        return None
    try:
        return json.loads(filepath.read_text())
    except Exception:
        return None


# ─── 計算交易量變化 ──────────────────────────────────────
def compute_volume_changes(current: list[dict], previous_snapshot: dict | None) -> list[dict]:
    if not previous_snapshot:
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


# ─── 格式化 ─────────────────────────────────────────────
def format_volume_number(n: float) -> str:
    if abs(n) >= 1_000_000:
        return f"${n / 1_000_000:,.2f}M"
    elif abs(n) >= 1_000:
        return f"${n / 1_000:,.1f}K"
    else:
        return f"${n:,.0f}"


def build_report(poly_changes: list[dict], predict_changes: list[dict], timestamp: str) -> str:
    lines = [
        "<b>📊 預測市場交易量變化報告</b>",
        f"<i>⏰ {timestamp} UTC</i>",
        "",
        "<b>🟣 Polymarket — 交易量變化 Top 10</b>",
        "",
    ]

    if not poly_changes:
        lines.append("<i>暫無資料（首次執行或 API 異常）</i>")
    else:
        for i, m in enumerate(poly_changes[:10], 1):
            delta_str = format_volume_number(m["volume_delta"])
            vol_str = format_volume_number(m["volume"])
            pct = f" ({m['pct_change']:+.1f}%)" if m["pct_change"] is not None else ""
            arrow = "🔺" if m["volume_delta"] > 0 else "🔻" if m["volume_delta"] < 0 else "➖"
            question = m["question"][:60] + ("…" if len(m["question"]) > 60 else "")
            lines.append(
                f"{i}. {arrow} <b>{question}</b>\n"
                f"   變化: <code>{delta_str}</code>{pct} │ "
                f"總量: <code>{vol_str}</code>\n"
                f"   🔗 <a href=\"{m['url']}\">查看盤口</a>"
            )
            lines.append("")

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
            question = m["question"][:60] + ("…" if len(m["question"]) > 60 else "")
            lines.append(
                f"{i}. {arrow} <b>{question}</b>\n"
                f"   變化: <code>{delta_str}</code>{pct} │ "
                f"總量: <code>{vol_str}</code>\n"
                f"   🔗 <a href=\"{m['url']}\">查看盤口</a>"
            )
            lines.append("")

    lines.append("<i>📡 每小時自動更新</i>")
    return "\n".join(lines)


# ─── 產生報告（共用）─────────────────────────────────────
async def generate_report() -> str:
    logger.info("開始抓取市場資料...")
    async with httpx.AsyncClient() as client:
        poly_markets, predict_markets = await asyncio.gather(
            fetch_polymarket_markets(client),
            fetch_predict_fun_markets(client),
        )

    poly_prev = load_snapshot(POLY_SNAPSHOT)
    predict_prev = load_snapshot(PREDICT_SNAPSHOT)
    poly_changes = compute_volume_changes(poly_markets, poly_prev)
    predict_changes = compute_volume_changes(predict_markets, predict_prev)
    save_snapshot(poly_markets, POLY_SNAPSHOT)
    save_snapshot(predict_markets, PREDICT_SNAPSHOT)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    return build_report(poly_changes, predict_changes, now_str)


# ─── Telegram 指令 ───────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    subscribers.add(chat_id)
    save_subscribers(subscribers)
    logger.info(f"新訂閱者: {chat_id}")

    await update.message.reply_text(
        "<b>👋 歡迎使用預測市場播報 Bot！</b>\n\n"
        "📡 每小時自動發送 Polymarket & predict.fun 交易量變化 Top 10\n\n"
        "<b>可用指令：</b>\n"
        "/report — 立即取得最新報告\n"
        "/stop — 取消訂閱自動播報\n\n"
        "⏳ 正在抓取第一份報告，請稍候...",
        parse_mode=ParseMode.HTML,
    )

    try:
        message = await generate_report()
        await update.message.reply_text(
            message, parse_mode=ParseMode.HTML, disable_web_page_preview=True,
        )
    except Exception as e:
        logger.error(f"產生報告失敗: {e}")
        await update.message.reply_text(f"⚠️ 抓取資料時發生錯誤：{e}")


async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ 正在抓取最新資料，請稍候...")
    try:
        message = await generate_report()
        await update.message.reply_text(
            message, parse_mode=ParseMode.HTML, disable_web_page_preview=True,
        )
    except Exception as e:
        logger.error(f"產生報告失敗: {e}")
        await update.message.reply_text(f"⚠️ 抓取資料時發生錯誤：{e}")


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    subscribers.discard(chat_id)
    save_subscribers(subscribers)
    await update.message.reply_text(
        "✅ 已取消訂閱，不會再收到自動播報。\n隨時可以用 /start 重新訂閱。"
    )


# ─── 排程：用 python-telegram-bot 內建 JobQueue ─────────
async def scheduled_broadcast(context: ContextTypes.DEFAULT_TYPE):
    """每小時自動播報給所有訂閱者"""
    if not subscribers:
        logger.info("目前沒有訂閱者，跳過播報")
        return

    try:
        message = await generate_report()
    except Exception as e:
        logger.error(f"排程報告產生失敗: {e}")
        return

    for chat_id in list(subscribers):
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            logger.info(f"✅ 已發送給 {chat_id}")
        except Exception as e:
            logger.error(f"發送給 {chat_id} 失敗: {e}")
            if "Forbidden" in str(e):
                subscribers.discard(chat_id)
                save_subscribers(subscribers)


# ─── 啟動 ────────────────────────────────────────────────
def main():
    logger.info("🚀 Bot 啟動中...")

    app = Application.builder().token(TG_BOT_TOKEN).build()

    # 註冊指令
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("stop", cmd_stop))

    # 用內建 JobQueue 排程，每 3600 秒（1 小時）執行一次
    app.job_queue.run_repeating(
        scheduled_broadcast,
        interval=3600,
        first=3600,  # 第一次在 1 小時後執行（用戶按 /start 時會立即收到）
    )

    logger.info("✅ Bot 已啟動，等待指令中...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
