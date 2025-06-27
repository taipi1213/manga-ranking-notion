#!/usr/bin/env python3
"""Daily manga ranking → Notion DB
改訂版 2025‑06‑28
• UA Unicode 修正
• Select オプション自動追加
• cover 用に専用の File Object を使用（name を外す）
• 400 エラー詳細を常時表示
"""

import os, time, datetime as dt, re, sys, requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import Dict, Iterator, List

# ── ENV ──────────────────────────────────
TOKEN = os.getenv("NOTION_TOKEN")
DB_ID = os.getenv("NOTION_DB")
if not (TOKEN and DB_ID):
    sys.exit("❌ NOTION_TOKEN / NOTION_DB 未設定")
DEBUG = bool(int(os.getenv("DEBUG", "0")))

# ── HEADERS ──────────────────────────────
HEAD: Dict[str, str] = {
    "Authorization": f"Bearer {TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
    "Accept": "application/json",
}
UA = {"User-Agent": "Mozilla/5.0 (compatible; rankingbot/1.1)"}

TODAY = dt.date.today().isoformat()
HTTPS_IMG = re.compile(r"^https://.*\.(?:jpe?g|png|webp)$", re.I)

# ── Notion Low‑level ─────────────────────

def notion_request(method, url: str, **kw) -> requests.Response:
    """リトライ付き HTTP。4xx/5xx は詳細表示。"""
    for retry in range(3):
        resp = method(url, headers=HEAD, timeout=10, **kw)
        if resp.status_code in (429, 502, 503):
            delay = 2 ** retry
            print(f"🔄 {resp.status_code} Retrying {delay}s …")
            time.sleep(delay)
            continue
        break

    if not resp.ok:
        print("❌", resp.status_code)
        try:
            print(resp.json())
        except Exception:
            print(resp.text)
        resp.raise_for_status()
    return resp

# ── Select オプション保証 ─────────────────

def ensure_select_option(prop: str, name: str):
    db = notion_request(requests.get, f"https://api.notion.com/v1/databases/{DB_ID}").json()
    existing = {opt["name"] for opt in db["properties"][prop]["select"]["options"]}
    if name in existing:
        return
    patch_body = {
        "properties": {
            prop: {
                "select": {"options": db["properties"][prop]["select"]["options"] + [{"name": name}]}
            }
        }
    }
    notion_request(requests.patch, f"https://api.notion.com/v1/databases/{DB_ID}", json=patch_body)
    print(f"➕ Added option '{name}' to {prop}")

# ── File object helpers ──────────────────

def file_obj(url: str) -> Dict:
    """For property type files – name required"""
    return {"type": "external", "name": url.split("/")[-1], "external": {"url": url}}


def cover_obj(url: str) -> Dict:
    """Page cover requires only type & external.url (name NG)"""
    return {"type": "external", "external": {"url": url}}

# ── Page search ──────────────────────────

def query_page(store: str, cat: str, rank: int) -> List[Dict]:
    q = {
        "filter": {
            "and": [
                {"property": "Date", "date": {"equals": TODAY}},
                {"property": "Store", "select": {"equals": store}},
                {"property": "Category", "select": {"equals": cat}},
                {"property": "Rank", "number": {"equals": rank}},
            ]
        }
    }
    r = notion_request(requests.post, f"https://api.notion.com/v1/databases/{DB_ID}/query", json=q)
    return r.json().get("results", [])

# ── Upsert ───────────────────────────────

def upsert(row: Dict):
    ensure_select_option("Store", row["store"])
    ensure_select_option("Category", row["cat"])

    img_ok = HTTPS_IMG.match(row["thumb"]) is not None
    props = {
        "Date": {"date": {"start": TODAY}},
        "Store": {"select": {"name": row["store"]}},
        "Category": {"select": {"name": row["cat"]}},
        "Rank": {"number": row["rank"]},
        "Title": {"title": [{"text": {"content": row["title"]}}]},
        "URL": {"url": row["url"]},
        "Thumb": {"files": [file_obj(row["thumb"])] if img_ok else []},
    }

    body = {"properties": props}
    if img_ok:
        body["cover"] = cover_obj(row["thumb"])

    hit = query_page(row["store"], row["cat"], row["rank"])
    if hit:
        pid = hit[0]["id"]
        notion_request(requests.patch, f"https://api.notion.com/v1/pages/{pid}", json=body)
    else:
        body["parent"] = {"database_id": DB_ID}
        notion_request(requests.post, "https://api.notion.com/v1/pages", json=body)
    print("✅", row["title"][:30])

# ── Amazon ───────────────────────────────

def amazon_thumb(div):
    img = div.select_one("img[src]")
    return "" if not img else re.sub(r"_AC_[^_.]+_", "_SX600_", img["src"], 1)


def fetch_amazon() -> Iterator[Dict]:
    base = "https://www.amazon.co.jp"
    soup = BeautifulSoup(requests.get(f"{base}/gp/bestsellers/books/2278488051", headers=UA, timeout=10).text, "html.parser")
    for rank, div in enumerate(soup.select("div.zg-grid-general-faceout")[:20], 1):
        title = div.select_one("img[alt]")["alt"].strip()
        href = urljoin(base, (div.find_parent("a") or div.select_one("a[href]"))["href"])
        yield {"store": "Amazon", "cat": "コミック売れ筋", "rank": rank, "title": title, "url": href, "thumb": amazon_thumb(div)}

# ── Cmoa ─────────────────────────────────

def cmoa_thumb(li):
    img = li.select_one("img[src]")
    if not img:
        return ""
    src = img["src"]
    return "https:" + src if src.startswith("//") else src


def fetch_cmoa(cat: str, url: str) -> Iterator[Dict]:
    soup = BeautifulSoup(requests.get(url, headers=UA, timeout=10).text, "html.parser")
    for rank, li in enumerate(soup.select("ul#ranking_result_list li.search_result_box")[:20], 1):
        title = li.select_one("img[alt]")["alt"].strip()
        href = urljoin("https://www.cmoa.jp", li.select_one("a.title")["href"])
        yield {"store": "Cmoa", "cat": cat, "rank": rank, "title": title, "url": href, "thumb": cmoa_thumb(li)}

CATS = [
    ("総合", "https://www.cmoa.jp/search/purpose/ranking/all/"),
    ("少年マンガ", "https://www.cmoa.jp/search/purpose/ranking/boy/"),
    ("青年マンガ", "https://www.cmoa.jp/search/purpose/ranking/gentle/"),
    ("ライトアダルト", "https://www.cmoa.jp/search/purpose/ranking/sexy/"),
]

# ── Main ─────────────────────────────────
if __name__ == "__main__":
    print("=== START", dt.datetime.now())
    if DEBUG:
        print(f"DB={DB_ID[:8]}… TOKEN={TOKEN[:8]}…")

    try:
        for row in fetch_amazon():
            upsert(row)
            time
