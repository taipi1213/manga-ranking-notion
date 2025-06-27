#!/usr/bin/env python3
"""Daily manga ranking → Notion DB
改訂版 2025-06-28  (UA ヘッダの Unicode 問題を修正)
"""

import os, time, datetime as dt, re, sys, json, requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import Dict, Iterator, List

# ── 環境変数 ──────────────────────────────
TOKEN = os.getenv("NOTION_TOKEN")
DB_ID = os.getenv("NOTION_DB")
if not (TOKEN and DB_ID):
    sys.exit("❌ NOTION_TOKEN / NOTION_DB が未設定です")
DEBUG = bool(int(os.getenv("DEBUG", "0")))

# ── 共通ヘッダ ────────────────────────────
HEAD: Dict[str, str] = {
    "Authorization": f"Bearer {TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# Amazon へのアクセスで UnicodeEncodeError が出るのは
# HTTP ヘッダに Latin‑1 に入らない文字が混ざっている時。
# Accept‑Language などを送らず、一番シンプルな UA だけにする。
UA = {
    "User-Agent": "Mozilla/5.0 (compatible; rankingbot/1.0)"
}

TODAY = dt.date.today().isoformat()
HTTPS_IMG = re.compile(r"^https://.*\.(?:jpe?g|png|webp)$", re.I)

# ── Notion API ラッパ ───────────────────────

def notion_request(method, url: str, **kw) -> requests.Response:
    """リトライ付きリクエスト。4xx は詳細を表示して raise。"""
    for retry in range(3):
        resp = method(url, headers=HEAD, timeout=10, **kw)
        if resp.status_code in (429, 502, 503):
            delay = 2 ** retry
            print(f"🔄 {resp.status_code} Retrying after {delay}s …")
            time.sleep(delay)
            continue
        break

    if not resp.ok:
        print("❌", resp.status_code)
        if DEBUG:
            print(resp.text)
        resp.raise_for_status()
    return resp


def file_obj(url: str) -> Dict:
    return {
        "type": "external",
        "name": url.split("/")[-1],
        "external": {"url": url},
    }


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
    r = notion_request(
        requests.post,
        f"https://api.notion.com/v1/databases/{DB_ID}/query",
        json=q,
    )
    return r.json().get("results", [])


def upsert(row: Dict):
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
        body["cover"] = file_obj(row["thumb"])

    hit = query_page(row["store"], row["cat"], row["rank"])
    if hit:
        page_id = hit[0]["id"]
        notion_request(requests.patch, f"https://api.notion.com/v1/pages/{page_id}", json=body)
    else:
        body["parent"] = {"database_id": DB_ID}
        notion_request(requests.post, "https://api.notion.com/v1/pages", json=body)

    print("✅", row["title"][:30])

# ── Amazon ──────────────────────────────────

def amazon_thumb(div):
    img = div.select_one("img[src]")
    if not img:
        return ""
    return re.sub(r"_AC_[^_.]+_", "_SX600_", img["src"])


def fetch_amazon() -> Iterator[Dict]:
    base = "https://www.amazon.co.jp"
    html = requests.get(f"{base}/gp/bestsellers/books/2278488051", headers=UA, timeout=10).text
    soup = BeautifulSoup(html, "html.parser")
    for rank, div in enumerate(soup.select("div.zg-grid-general-faceout")[:20], 1):
        title = div.select_one("img[alt]")["alt"].strip()
        href = urljoin(base, (div.find_parent("a") or div.select_one("a[href]"))["href"])
        yield {
            "store": "Amazon",
            "cat": "コミック売れ筋",
            "rank": rank,
            "title": title,
            "url": href,
            "thumb": amazon_thumb(div),
        }

# ── コミックシーモア ───────────────────────

def cmoa_thumb(li):
    img = li.select_one("img[src]")
    if not img:
        return ""
    src = img["src"]
    return "https:" + src if src.startswith("//") else src


def fetch_cmoa(cat: str, url: str) -> Iterator[Dict]:
    html = requests.get(url, headers=UA, timeout=10).text
    soup = BeautifulSoup(html, "html.parser")
    for rank, li in enumerate(soup.select("ul#ranking_result_list li.search_result_box")[:20], 1):
        title = li.select_one("img[alt]")["alt"].strip()
        href = urljoin("https://www.cmoa.jp", li.select_one("a.title")["href"])
        yield {
            "store": "Cmoa",
            "cat": cat,
            "rank": rank,
            "title": title,
            "url": href,
            "thumb": cmoa_thumb(li),
        }

CATS = [
    ("総合", "https://www.cmoa.jp/search/purpose/ranking/all/"),
    ("少年マンガ", "https://www.cmoa.jp/search/purpose/ranking/boy/"),
    ("青年マンガ", "https://www.cmoa.jp/search/purpose/ranking/gentle/"),
    ("ライトアダルト", "https://www.cmoa.jp/search/purpose/ranking/sexy/"),
]

# ── メイン ─────────────────────────────
if __name__ == "__main__":
    print("=== START", dt.datetime.now())
    if DEBUG:
        print(f"DB={DB_ID[:8]}… TOKEN={TOKEN[:8]}…")

    try:
        for row in fetch_amazon():
            upsert(row)
            time.sleep(0.5)

        for cat, url in CATS:
            for row in fetch_cmoa(cat, url):
                upsert(row)
                time.sleep(0.5)
    except Exception as e:
        print("🚨 スクリプトが異常終了:", e)
        raise
    finally:
        print("=== DONE ", dt.datetime.now())
