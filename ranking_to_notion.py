#!/usr/bin/env python3
"""Daily manga ranking → Notion DB
改訂版 2025-07-02  (JST 11 時実行 / Latest✅ / ReadTimeout 対策)

・Amazon／コミックシーモア上位 20 位を取得
・ZoneInfo で JST の今日を取得
・Checkbox プロパティ Latest を自動作成
・実行ごとに前回の Latest✅ を全解除 → 今回のみ ✅
・Notion API 429/5xx／ReadTimeout／ConnectionError を指数バックオフで再試行
・レート制限を確実に下回るよう 1 rps 前後で送信
"""

from __future__ import annotations
import os, sys, re, time, logging, datetime as dt
from typing import Dict, Iterator, List
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

# ────────────────────────── 環境変数
TOKEN = os.getenv("NOTION_TOKEN")
DB_ID = os.getenv("NOTION_DB")
if not (TOKEN and DB_ID):
    sys.exit("❌  NOTION_TOKEN / NOTION_DB が未設定です。")

# ────────────────────────── HTTP セッション & リトライ
session = requests.Session()
retry_config = Retry(
    total=5,                # 最大 5 回再試行
    backoff_factor=2,       # 2,4,8,16,32 秒
    status_forcelist=[429, 502, 503],
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry_config)
session.mount("https://", adapter)

HEAD: Dict[str, str] = {
    "Authorization": f"Bearer {TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
    "Accept": "application/json",
}
UA = {"User-Agent": "Mozilla/5.0 (compatible; rankingbot/2.2)"}

JST   = ZoneInfo("Asia/Tokyo")
TODAY = dt.datetime.now(JST).date().isoformat()
IMG_OK = re.compile(r"^https://.*\.(?:jpe?g|png|webp)$", re.I)

# ────────────────────────── Notion API ラッパ
def notion(method, url: str, **kw) -> requests.Response:
    """Notion API 呼び出し：429/5xx/ReadTimeout などを指数バックオフ再試行"""
    kw.setdefault("headers", HEAD)
    kw.setdefault("timeout", 30)          # 30 s に延長
    for retry_i in range(5):
        try:
            res = method(url, **kw)
            if res.status_code in (429, 502, 503):
                raise requests.exceptions.HTTPError
            return res
        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError) as e:
            wait = 2 ** retry_i
            logging.warning("🔄 %s — retry in %ss", type(e).__name__, wait)
            time.sleep(wait)
    raise RuntimeError("🔚 Notion API 再試行上限を超えました")

# ────────────────────────── プロパティ保証
def ensure_checkbox(name: str) -> None:
    db = notion(session.get, f"https://api.notion.com/v1/databases/{DB_ID}").json()
    if name in db["properties"]:
        return
    patch = {"properties": {name: {"checkbox": {}}}}
    notion(session.patch, f"https://api.notion.com/v1/databases/{DB_ID}", json=patch)
    print(f"🆕 Checkbox '{name}' を作成")

def ensure_select(prop: str, value: str) -> None:
    db = notion(session.get, f"https://api.notion.com/v1/databases/{DB_ID}").json()
    opts = db["properties"][prop]["select"]["options"]
    if value in {o["name"] for o in opts}:
        return
    opts.append({"name": value})
    patch = {"properties": {prop: {"select": {"options": opts}}}}
    notion(session.patch, f"https://api.notion.com/v1/databases/{DB_ID}", json=patch)

# ────────────────────────── Latest フラグを一括解除
def clear_latest() -> None:
    payload = {"filter": {"property": "Latest", "checkbox": {"equals": True}}}
    res = notion(session.post,
                 f"https://api.notion.com/v1/databases/{DB_ID}/query",
                 json=payload).json()
    hits = res.get("results", [])
    for p in hits:
        notion(session.patch,
               f"https://api.notion.com/v1/pages/{p['id']}",
               json={"properties": {"Latest": {"checkbox": False}}})
    if hits:
        print(f"↩️  前回の Latest✅ {len(hits)} 件を解除")

# ────────────────────────── 既存ページ検索
def query(store: str, cat: str, rank: int) -> List[Dict]:
    payload = {
        "filter": {
            "and": [
                {"property": "Date", "date": {"equals": TODAY}},
                {"property": "Store", "select": {"equals": store}},
                {"property": "Category", "select": {"equals": cat}},
                {"property": "Rank", "number": {"equals": rank}},
            ]
        }
    }
    res = notion(session.post,
                 f"https://api.notion.com/v1/databases/{DB_ID}/query",
                 json=payload)
    return res.json().get("results", [])

# ────────────────────────── ページ UPSERT
def upsert(row: Dict) -> None:
    ensure_select("Store", row["store"])
    ensure_select("Category", row["cat"])

    props = {
        "Date":     {"date":   {"start": TODAY}},
        "Store":    {"select": {"name": row["store"]}},
        "Category": {"select": {"name": row["cat"]}},
        "Rank":     {"number": row["rank"]},
        "Title":    {"title":  [{"text": {"content": row["title"]}}]},
        "URL":      {"url":    row["url"]},
        "Latest":   {"checkbox": True},
    }

    cover = None
    if IMG_OK.match(row["thumb"] or ""):
        props["Thumb"] = {"url": row["thumb"]}
        cover = {"type": "external", "external": {"url": row["thumb"]}}
    else:
        props["Thumb"] = {"url": None}     # ← 空文字は NG、None は OK

    body = {"properties": props}
    if cover:
        body["cover"] = cover

    hit = query(row["store"], row["cat"], row["rank"])
    if hit:
        notion(session.patch,
               f"https://api.notion.com/v1/pages/{hit[0]['id']}",
               json=body)
    else:
        body["parent"] = {"database_id": DB_ID}
        notion(session.post, "https://api.notion.com/v1/pages", json=body)

    print("✅", row["title"][:30])

# ────────────────────────── スクレイパ
def amazon_thumb(div):
    img = div.select_one("img[src]")
    return "" if not img else re.sub(r"_AC_[^_.]+_", "_SX600_", img["src"], 1)

def fetch_amazon() -> Iterator[Dict]:
    url = "https://www.amazon.co.jp/gp/bestsellers/books/2278488051"
    soup = BeautifulSoup(session.get(url, headers=UA, timeout=30).text, "html.parser")
    for rank, div in enumerate(soup.select("div.zg-grid-general-faceout")[:20], 1):
        title = div.select_one("img[alt]")["alt"].strip()
        href = urljoin("https://www.amazon.co.jp",
                       (div.find_parent("a") or div.select_one("a[href]"))["href"])
        yield {
            "store": "Amazon", "cat": "コミック売れ筋",
            "rank": rank, "title": title, "url": href,
            "thumb": amazon_thumb(div),
        }

def cmoa_thumb(li):
    img = li.select_one("img[src]")
    if not img:
        return ""
    src = img["src"]
    return "https:" + src if src.startswith("//") else src

def fetch_cmoa(cat: str, url: str) -> Iterator[Dict]:
    soup = BeautifulSoup(session.get(url, headers=UA, timeout=30).text, "html.parser")
    for rank, li in enumerate(soup.select("ul#ranking_result_list li.search_result_box")[:20], 1):
        title = li.select_one("img[alt]")["alt"].strip()
        href = urljoin("https://www.cmoa.jp", li.select_one("a.title")["href"])
        yield {
            "store": "Cmoa", "cat": cat,
            "rank": rank, "title": title, "url": href,
            "thumb": cmoa_thumb(li),
        }

CATS = [
    ("総合",      "https://www.cmoa.jp/search/purpose/ranking/all/"),
    ("少年マンガ", "https://www.cmoa.jp/search/purpose/ranking/boy/"),
    ("青年マンガ", "https://www.cmoa.jp/search/purpose/ranking/gentle/"),
    ("ライトアダルト", "https://www.cmoa.jp/search/purpose/ranking/sexy/"),
]

# ────────────────────────── Main
if __name__ == "__main__":
    print("=== START", dt.datetime.now(JST))
    ensure_checkbox("Latest")
    clear_latest()

    try:
        # Amazon
        for row in fetch_amazon():
            upsert(row)
            time.sleep(1.0)        # 1 rps

        # コミックシーモア
        for cat, url in CATS:
            for row in fetch_cmoa(cat, url):
                upsert(row)
                time.sleep(1.0)
    except Exception as e:
        print("🚨", e)
        raise
    finally:
        print("=== DONE ", dt.datetime.now(JST))
