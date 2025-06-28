#!/usr/bin/env python3
"""Daily manga ranking → Notion DB
改訂版 2025-06-28

・Amazon／コミックシーモアから20位まで取得
・Notion Select オプションを自動追加
・cover 用 FileObject は name を含めない
・429／5xx リトライ、4xx で詳細ログ出力
"""

import os
import sys
import re
import time
import datetime as dt
from typing import Dict, Iterator, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ───────────────────────── 環境変数チェック
TOKEN = os.getenv("NOTION_TOKEN")
DB_ID = os.getenv("NOTION_DB")
if not (TOKEN and DB_ID):
    sys.exit("❌ NOTION_TOKEN / NOTION_DB が未設定です。")

DEBUG = bool(int(os.getenv("DEBUG", "0")))

# ───────────────────────── HTTP ヘッダ
HEAD: Dict[str, str] = {
    "Authorization": f"Bearer {TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
    "Accept": "application/json",
}
UA = {"User-Agent": "Mozilla/5.0 (compatible; rankingbot/1.2)"}

TODAY = dt.date.today().isoformat()
HTTPS_IMG = re.compile(r"^https://.*\.(?:jpe?g|png|webp)$", re.I)

# ───────────────────────── Notion API ラッパ
def notion(method, url: str, **kw) -> requests.Response:
    """Notion API 呼び出し：429/5xx は指数バックオフ、4xx は詳細表示"""
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
        try:
            print(resp.json())
        except Exception:
            print(resp.text)
        resp.raise_for_status()
    return resp

# ───────────────────────── Select オプション保証
def ensure_option(prop: str, name: str) -> None:
    """Select プロパティに name が無ければ動的追加"""
    db = notion(requests.get, f"https://api.notion.com/v1/databases/{DB_ID}").json()
    opts = db["properties"][prop]["select"]["options"]
    if name in {o["name"] for o in opts}:
        return
    opts.append({"name": name})
    patch = {"properties": {prop: {"select": {"options": opts}}}}
    notion(requests.patch, f"https://api.notion.com/v1/databases/{DB_ID}", json=patch)
    print(f"➕ Added option '{name}' to {prop}")

# ───────────────────────── File／Cover ヘルパ
file_obj = lambda u: {  # noqa: E731
    "type": "external",
    "name": u.split("/")[-1],
    "external": {"url": u},
}
cover_obj = lambda u: {  # noqa: E731
    "type": "external",
    "external": {"url": u},
}

# ───────────────────────── 既存ページ検索
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
    res = notion(
        requests.post,
        f"https://api.notion.com/v1/databases/{DB_ID}/query",
        json=payload,
    )
    return res.json().get("results", [])

# ───────────────────────── ページ UPSERT
def upsert(row: Dict) -> None:
    ensure_option("Store", row["store"])
    ensure_option("Category", row["cat"])

    img_ok = HTTPS_IMG.match(row["thumb"]) is not None

    props = {
        "Date":      {"date":   {"start": TODAY}},
        "Store":     {"select": {"name": row["store"]}},
        "Category":  {"select": {"name": row["cat"]}},
        "Rank":      {"number": row["rank"]},
        "Title":     {"title":  [{"text": {"content": row["title"]}}]},
        "URL":       {"url":    row["url"]},
        # ← ★ ここを URL プロパティとして渡す ★
        "Thumb":     {"url":    row["thumb"] if img_ok else ""},
    }

    body = {"properties": props}

    # page の cover は従来どおり File Object で OK
    if img_ok:
        body["cover"] = {
            "type": "external",
            "external": {"url": row["thumb"]}
        }

    hit = query(row["store"], row["cat"], row["rank"])
    if hit:
        notion(requests.patch,
               f"https://api.notion.com/v1/pages/{hit[0]['id']}",
               json=body)
    else:
        body["parent"] = {"database_id": DB_ID}
        notion(requests.post,
               "https://api.notion.com/v1/pages",
               json=body)
    print("✅", row["title"][:30])

# ───────────────────────── スクレイパ
def amazon_thumb(div):
    img = div.select_one("img[src]")
    return "" if not img else re.sub(r"_AC_[^_.]+_", "_SX600_", img["src"], 1)

def fetch_amazon() -> Iterator[Dict]:
    url = "https://www.amazon.co.jp/gp/bestsellers/books/2278488051"
    soup = BeautifulSoup(requests.get(url, headers=UA, timeout=10).text, "html.parser")
    for rank, div in enumerate(soup.select("div.zg-grid-general-faceout")[:20], 1):
        title = div.select_one("img[alt]")["alt"].strip()
        href = urljoin(
            "https://www.amazon.co.jp",
            (div.find_parent("a") or div.select_one("a[href]"))["href"],
        )
        yield {
            "store": "Amazon",
            "cat": "コミック売れ筋",
            "rank": rank,
            "title": title,
            "url": href,
            "thumb": amazon_thumb(div),
        }

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

# ───────────────────────── Main
if __name__ == "__main__":
    print("=== START", dt.datetime.now())
    try:
        # Amazon
        for row in fetch_amazon():
            upsert(row)
            time.sleep(0.4)

        # シーモア各カテゴリー
        for cat, url in CATS:
            for row in fetch_cmoa(cat, url):
                upsert(row)
                time.sleep(0.4)
    except Exception as e:
        print("🚨", e)
        raise
    finally:
        print("=== DONE ", dt.datetime.now())
