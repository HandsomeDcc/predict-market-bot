"""
Telegram Bot: Polymarket & predict.fun 預測市場追蹤器
功能：
  1. 每小時播報交易量變化 Top 10
  2. 每 5 分鐘掃描石油/戰爭/地緣政治相關盤口，偵測新盤自動播報
指令：
  /start   — 啟動 Bot，訂閱所有播報
  /report  — 立即取得交易量變化 Top 10
  /oil     — 查看所有石油相關盤口
  /war     — 查看所有戰爭/衝突相關盤口
  /geo     — 查看所有地緣政治相關盤口
  /topics  — 查看所有追蹤主題的盤口（石油+戰爭+地緣）
  /stop    — 取消訂閱自動播報
"""

import os
import re
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
KNOWN_TOPIC_IDS_FILE = SNAPSHOT_DIR / "known_topic_ids.json"


# ═══════════════════════════════════════════════════════════
# 主題關鍵字定義
# ═══════════════════════════════════════════════════════════
TOPIC_KEYWORDS = {
    "oil": [
        "oil", "crude", "petroleum", "petrol", "gasoline", "diesel",
        "opec", "brent", "wti", "oil price", "oil production",
        "energy price", "fuel", "barrel",
    ],
    "war": [
        "war", "conflict", "military", "invasion", "troops", "army",
        "missile", "airstrike", "bombing", "combat", "ceasefire",
        "escalation", "casualties", "battle", "offensive",
        "nuclear weapon", "nato", "defense",
    ],
    "geo": [
        "iran", "russia", "ukraine", "israel", "gaza", "palestine",
        "taiwan", "china", "north korea", "south korea",
        "sanctions", "embargo", "red sea", "houthi", "hezbollah",
        "hamas", "kremlin", "putin", "zelensky", "netanyahu",
        "xi jinping", "kim jong", "strait of hormuz", "south china sea",
        "syria", "yemen", "libya", "sudan", "coup", "regime",
        "geopolit", "territorial", "annex",
    ],
}

# 預編譯正則（大小寫不敏感）
TOPIC_PATTERNS: dict[str, re.Pattern] = {}
for topic, keywords in TOPIC_KEYWORDS.items():
    # 用 word boundary 匹配，避免誤判（例如 "oil" 不會匹配 "soil"）
    pattern = "|".join(r"\b" + re.escape(kw) + r"\b" for kw in keywords)
    TOPIC_PATTERNS[topic] = re.compile(pattern, re.IGNORECASE)

TOPIC_LABELS = {
    "oil": "🛢 石油/能源",
    "war": "⚔️ 戰爭/衝突",
    "geo": "🌍 地緣政治",
}


def match_topics(text: str) -> list[str]:
    """回傳該文本匹配到的主題列表"""
    return [topic for topic, pat in TOPIC_PATTERNS.items() if pat.search(text)]


def filter_markets_by_topic(markets: list[dict], topic: str) -> list[dict]:
    """篩選特定主題的市場"""
    pat = TOPIC_PATTERNS[topic]
    return [m for m in markets if pat.search(m.get("question", ""))]


def filter_markets_all_topics(markets: list[dict]) -> list[dict]:
    """篩選所有追蹤主題的市場（去重）"""
    seen = set()
    results = []
    for m in markets:
        if any(pat.search(m.get("question", "")) for pat in TOPIC_PATTERNS.values()):
            if m["id"] not in seen:
                seen.add(m["id"])
                results.append(m)
    return results


# ═══════════════════════════════════════════════════════════
# 訂閱者管理
# ═══════════════════════════════════════════════════════════
def load_subscribers() -> set:
    if SUBSCRIBERS_FILE.exists():
        try:
            return set(json.loads(SUBSCRIBERS_FILE.read_text()))
        except Exception:
            pass
    return set()


def save_subscribers(subs: set):
    SUBSCRIBERS_FILE.write_text(json.dumps(list(subs)))


def load_known_topic_ids() -> set:
    if KNOWN_TOPIC_IDS_FILE.exists():
        try:
            return set(json.loads(KNOWN_TOPIC_IDS_FILE.read_text()))
        except Exception:
            pass
    return set()


def save_known_topic_ids(ids: set):
    KNOWN_TOPIC_IDS_FILE.write_text(json.dumps(list(ids)))


subscribers: set = load_subscribers()
known_topic_ids: set = load_known_topic_ids()


# ═══════════════════════════════════════════════════════════
# Polymarket 資料抓取
# ═══════════════════════════════════════════════════════════
def _build_polymarket_url(m: dict) -> str:
    event_slug = m.get("event_slug") or m.get("eventSlug") or ""
    if event_slug:
        return f"https://polymarket.com/event/{event_slug}"
    slug = m.get("slug") or m.get("market_slug") or ""
    if slug:
        return f"https://polymarket.com/event/{slug}"
    cid = m.get("condition_id") or m.get("conditionId") or m.get("id") or ""
    return f"https://polymarket.com/market/{cid}"


async def fetch_polymarket_markets(client: httpx.AsyncClient) -> list[dict]:
    markets = await _fetch_polymarket_events(client)
    if not markets:
        logger.info("events 端點無資料，改用 /markets")
        markets = await _fetch_polymarket_markets_fallback(client)
    return markets


async def _fetch_polymarket_events(client: httpx.AsyncClient) -> list[dict]:
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
            slug = ev.get("slug", "")
            url = f"https://polymarket.com/event/{slug}" if slug else ""
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
                "source": "polymarket",
            })

        offset += limit
        if offset >= 2000 or len(data) < limit:
            break

    logger.info(f"Polymarket (events): 取得 {len(markets)} 個事件")
    return markets


async def _fetch_polymarket_markets_fallback(client: httpx.AsyncClient) -> list[dict]:
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
                "source": "polymarket",
            })

        offset += limit
        if offset >= 2000 or len(data) < limit:
            break

    logger.info(f"Polymarket (markets fallback): 取得 {len(markets)} 個市場")
    return markets


# ═══════════════════════════════════════════════════════════
# predict.fun 資料抓取
# ═══════════════════════════════════════════════════════════
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
                "source": "predict.fun",
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


# ═══════════════════════════════════════════════════════════
# 快照 & 計算
# ═══════════════════════════════════════════════════════════
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


# ═══════════════════════════════════════════════════════════
# 抓取所有市場（共用）
# ═══════════════════════════════════════════════════════════
async def fetch_all_markets() -> tuple[list[dict], list[dict]]:
    async with httpx.AsyncClient() as client:
        poly, predict = await asyncio.gather(
            fetch_polymarket_markets(client),
            fetch_predict_fun_markets(client),
        )
    return poly, predict


# ═══════════════════════════════════════════════════════════
# 格式化訊息
# ═══════════════════════════════════════════════════════════
def format_volume_number(n: float) -> str:
    if abs(n) >= 1_000_000:
        return f"${n / 1_000_000:,.2f}M"
    elif abs(n) >= 1_000:
        return f"${n / 1_000:,.1f}K"
    else:
        return f"${n:,.0f}"


def _format_market_line(i: int, m: dict, show_source: bool = False) -> str:
    vol_str = format_volume_number(m["volume"])
    question = m["question"][:65] + ("…" if len(m["question"]) > 65 else "")
    source_tag = f" [{m.get('source', '?')}]" if show_source else ""
    topics = match_topics(m.get("question", ""))
    topic_tags = " ".join(TOPIC_LABELS.get(t, "") for t in topics)

    return (
        f"{i}. <b>{question}</b>{source_tag}\n"
        f"   💰 <code>{vol_str}</code> │ {topic_tags}\n"
        f"   🔗 <a href=\"{m['url']}\">查看盤口</a>"
    )


def build_volume_report(poly_changes: list[dict], predict_changes: list[dict], timestamp: str) -> str:
    lines = [
        "<b>📊 交易量變化 Top 10</b>",
        f"<i>⏰ {timestamp} UTC</i>",
        "",
        "<b>🟣 Polymarket</b>",
        "",
    ]

    if not poly_changes:
        lines.append("<i>暫無資料</i>")
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

    lines.append("<b>🟢 predict.fun</b>")
    lines.append("")

    if not predict_changes:
        lines.append("<i>暫無資料</i>")
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


def build_topic_report(topic: str, poly_markets: list[dict], predict_markets: list[dict]) -> str:
    """建構特定主題的盤口列表報告"""
    label = TOPIC_LABELS.get(topic, topic)
    poly_filtered = filter_markets_by_topic(poly_markets, topic)
    predict_filtered = filter_markets_by_topic(predict_markets, topic)

    # 按交易量排序
    poly_filtered.sort(key=lambda x: x["volume"], reverse=True)
    predict_filtered.sort(key=lambda x: x["volume"], reverse=True)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    lines = [
        f"<b>{label} 相關盤口</b>",
        f"<i>⏰ {now_str} UTC</i>",
        "",
        f"<b>🟣 Polymarket ({len(poly_filtered)} 個)</b>",
        "",
    ]

    if not poly_filtered:
        lines.append("<i>目前無相關盤口</i>")
    else:
        for i, m in enumerate(poly_filtered[:20], 1):
            lines.append(_format_market_line(i, m))
            lines.append("")

    lines.append(f"<b>🟢 predict.fun ({len(predict_filtered)} 個)</b>")
    lines.append("")

    if not predict_filtered:
        lines.append("<i>目前無相關盤口</i>")
    else:
        for i, m in enumerate(predict_filtered[:20], 1):
            lines.append(_format_market_line(i, m))
            lines.append("")

    total = len(poly_filtered) + len(predict_filtered)
    lines.append(f"<i>共追蹤 {total} 個 {label} 相關盤口</i>")
    return "\n".join(lines)


def build_all_topics_report(poly_markets: list[dict], predict_markets: list[dict]) -> str:
    """建構所有追蹤主題的盤口報告"""
    all_markets = poly_markets + predict_markets
    filtered = filter_markets_all_topics(all_markets)
    filtered.sort(key=lambda x: x["volume"], reverse=True)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    lines = [
        "<b>🎯 所有追蹤主題盤口總覽</b>",
        f"<i>⏰ {now_str} UTC</i>",
        "",
    ]

    # 分類統計
    topic_counts = {}
    for m in filtered:
        for t in match_topics(m["question"]):
            topic_counts[t] = topic_counts.get(t, 0) + 1

    for t, label in TOPIC_LABELS.items():
        count = topic_counts.get(t, 0)
        lines.append(f"  {label}: <b>{count}</b> 個盤口")
    lines.append("")

    # 按交易量列出前 30
    lines.append(f"<b>按交易量排序 Top 30（共 {len(filtered)} 個）</b>")
    lines.append("")

    for i, m in enumerate(filtered[:30], 1):
        lines.append(_format_market_line(i, m, show_source=True))
        lines.append("")

    return "\n".join(lines)


def build_new_markets_alert(new_markets: list[dict]) -> str:
    """建構新盤口警報訊息"""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    lines = [
        "<b>🚨 新盤口警報！</b>",
        f"<i>⏰ {now_str} UTC</i>",
        "",
        f"偵測到 <b>{len(new_markets)}</b> 個新的追蹤主題盤口：",
        "",
    ]

    for i, m in enumerate(new_markets, 1):
        topics = match_topics(m["question"])
        topic_tags = " ".join(TOPIC_LABELS.get(t, "") for t in topics)
        source = m.get("source", "?")
        vol_str = format_volume_number(m["volume"])
        question = m["question"][:65] + ("…" if len(m["question"]) > 65 else "")

        lines.append(
            f"<b>🆕 {question}</b>\n"
            f"   📍 {source} │ 💰 <code>{vol_str}</code> │ {topic_tags}\n"
            f"   🔗 <a href=\"{m['url']}\">查看盤口</a>"
        )
        lines.append("")

    lines.append("<i>🔍 每 5 分鐘自動偵測</i>")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════
# 產生交易量報告
# ═══════════════════════════════════════════════════════════
async def generate_volume_report() -> str:
    poly_markets, predict_markets = await fetch_all_markets()

    poly_prev = load_snapshot(POLY_SNAPSHOT)
    predict_prev = load_snapshot(PREDICT_SNAPSHOT)
    poly_changes = compute_volume_changes(poly_markets, poly_prev)
    predict_changes = compute_volume_changes(predict_markets, predict_prev)
    save_snapshot(poly_markets, POLY_SNAPSHOT)
    save_snapshot(predict_markets, PREDICT_SNAPSHOT)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    return build_volume_report(poly_changes, predict_changes, now_str)


# ═══════════════════════════════════════════════════════════
# Telegram 指令處理
# ═══════════════════════════════════════════════════════════
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    subscribers.add(chat_id)
    save_subscribers(subscribers)
    logger.info(f"新訂閱者: {chat_id}")

    await update.message.reply_text(
        "<b>👋 歡迎使用預測市場追蹤 Bot！</b>\n\n"
        "<b>📡 自動播報：</b>\n"
        "• 每小時 — 交易量變化 Top 10\n"
        "• 每 5 分鐘 — 石油/戰爭/地緣政治新盤口警報\n\n"
        "<b>📋 可用指令：</b>\n"
        "/report — 交易量變化 Top 10\n"
        "/oil — 所有石油/能源相關盤口\n"
        "/war — 所有戰爭/衝突相關盤口\n"
        "/geo — 所有地緣政治相關盤口\n"
        "/topics — 所有追蹤主題盤口總覽\n"
        "/stop — 取消訂閱\n\n"
        "⏳ 正在抓取資料...",
        parse_mode=ParseMode.HTML,
    )

    try:
        message = await generate_volume_report()
        await _send_long_message(update.effective_chat.id, message, context.bot)
    except Exception as e:
        logger.error(f"產生報告失敗: {e}")
        await update.message.reply_text(f"⚠️ 抓取資料時發生錯誤：{e}")


async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ 正在抓取最新資料...")
    try:
        message = await generate_volume_report()
        await _send_long_message(update.effective_chat.id, message, context.bot)
    except Exception as e:
        logger.error(f"產生報告失敗: {e}")
        await update.message.reply_text(f"⚠️ 錯誤：{e}")


async def cmd_oil(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ 正在搜尋石油/能源相關盤口...")
    try:
        poly, predict = await fetch_all_markets()
        message = build_topic_report("oil", poly, predict)
        await _send_long_message(update.effective_chat.id, message, context.bot)
    except Exception as e:
        await update.message.reply_text(f"⚠️ 錯誤：{e}")


async def cmd_war(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ 正在搜尋戰爭/衝突相關盤口...")
    try:
        poly, predict = await fetch_all_markets()
        message = build_topic_report("war", poly, predict)
        await _send_long_message(update.effective_chat.id, message, context.bot)
    except Exception as e:
        await update.message.reply_text(f"⚠️ 錯誤：{e}")


async def cmd_geo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ 正在搜尋地緣政治相關盤口...")
    try:
        poly, predict = await fetch_all_markets()
        message = build_topic_report("geo", poly, predict)
        await _send_long_message(update.effective_chat.id, message, context.bot)
    except Exception as e:
        await update.message.reply_text(f"⚠️ 錯誤：{e}")


async def cmd_topics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ 正在搜尋所有追蹤主題盤口...")
    try:
        poly, predict = await fetch_all_markets()
        message = build_all_topics_report(poly, predict)
        await _send_long_message(update.effective_chat.id, message, context.bot)
    except Exception as e:
        await update.message.reply_text(f"⚠️ 錯誤：{e}")


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    subscribers.discard(chat_id)
    save_subscribers(subscribers)
    await update.message.reply_text(
        "✅ 已取消訂閱。隨時可以用 /start 重新訂閱。"
    )


# ═══════════════════════════════════════════════════════════
# 長訊息分段發送（Telegram 限制 4096 字元）
# ═══════════════════════════════════════════════════════════
async def _send_long_message(chat_id: int, text: str, bot):
    """將超長訊息切割為多段發送"""
    MAX_LEN = 4000  # 留點空間

    if len(text) <= MAX_LEN:
        await bot.send_message(
            chat_id=chat_id, text=text,
            parse_mode=ParseMode.HTML, disable_web_page_preview=True,
        )
        return

    # 按空行分段
    parts = []
    current = ""
    for line in text.split("\n"):
        if len(current) + len(line) + 1 > MAX_LEN:
            parts.append(current)
            current = line
        else:
            current = current + "\n" + line if current else line
    if current:
        parts.append(current)

    for part in parts:
        await bot.send_message(
            chat_id=chat_id, text=part,
            parse_mode=ParseMode.HTML, disable_web_page_preview=True,
        )
        await asyncio.sleep(0.5)  # 避免 rate limit


# ═══════════════════════════════════════════════════════════
# 排程任務
# ═══════════════════════════════════════════════════════════
async def scheduled_volume_broadcast(context: ContextTypes.DEFAULT_TYPE):
    """每小時：交易量變化 Top 10"""
    if not subscribers:
        logger.info("沒有訂閱者，跳過交易量播報")
        return

    try:
        message = await generate_volume_report()
    except Exception as e:
        logger.error(f"排程交易量報告失敗: {e}")
        return

    for chat_id in list(subscribers):
        try:
            await _send_long_message(chat_id, message, context.bot)
            logger.info(f"✅ 交易量報告已發送給 {chat_id}")
        except Exception as e:
            logger.error(f"發送給 {chat_id} 失敗: {e}")
            if "Forbidden" in str(e):
                subscribers.discard(chat_id)
                save_subscribers(subscribers)


async def scheduled_topic_scan(context: ContextTypes.DEFAULT_TYPE):
    """每 5 分鐘：掃描新盤口"""
    global known_topic_ids

    if not subscribers:
        return

    try:
        poly, predict = await fetch_all_markets()
    except Exception as e:
        logger.error(f"主題掃描失敗: {e}")
        return

    all_markets = poly + predict
    topic_markets = filter_markets_all_topics(all_markets)

    current_ids = {m["id"] for m in topic_markets}

    # 首次執行：記錄所有現有盤口，不發警報
    if not known_topic_ids:
        known_topic_ids = current_ids
        save_known_topic_ids(known_topic_ids)
        logger.info(f"首次掃描：記錄 {len(known_topic_ids)} 個已知主題盤口")
        return

    # 找出新盤口
    new_ids = current_ids - known_topic_ids
    if not new_ids:
        logger.info(f"主題掃描完成：無新盤口（追蹤中 {len(current_ids)} 個）")
        return

    new_markets = [m for m in topic_markets if m["id"] in new_ids]
    logger.info(f"🚨 偵測到 {len(new_markets)} 個新主題盤口！")

    # 更新已知 ID
    known_topic_ids = current_ids
    save_known_topic_ids(known_topic_ids)

    # 發送警報
    alert = build_new_markets_alert(new_markets)
    for chat_id in list(subscribers):
        try:
            await _send_long_message(chat_id, alert, context.bot)
            logger.info(f"✅ 新盤口警報已發送給 {chat_id}")
        except Exception as e:
            logger.error(f"發送給 {chat_id} 失敗: {e}")
            if "Forbidden" in str(e):
                subscribers.discard(chat_id)
                save_subscribers(subscribers)


# ═══════════════════════════════════════════════════════════
# 啟動
# ═══════════════════════════════════════════════════════════
def main():
    logger.info("🚀 Bot 啟動中...")

    app = Application.builder().token(TG_BOT_TOKEN).build()

    # 註冊指令
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("oil", cmd_oil))
    app.add_handler(CommandHandler("war", cmd_war))
    app.add_handler(CommandHandler("geo", cmd_geo))
    app.add_handler(CommandHandler("topics", cmd_topics))
    app.add_handler(CommandHandler("stop", cmd_stop))

    # 排程 1：每小時交易量播報
    app.job_queue.run_repeating(
        scheduled_volume_broadcast,
        interval=3600,
        first=3600,
    )

    # 排程 2：每 5 分鐘主題盤口掃描
    app.job_queue.run_repeating(
        scheduled_topic_scan,
        interval=300,   # 5 分鐘
        first=60,       # 啟動 1 分鐘後開始首次掃描
    )

    logger.info("✅ Bot 已啟動")
    logger.info("⏰ 排程：交易量報告 = 每小時 │ 主題掃描 = 每 5 分鐘")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
