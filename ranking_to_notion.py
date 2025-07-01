#!/usr/bin/env python3
"""Daily manga ranking → Notion DB
改訂版 2025-07-01   JST 11 時実行 & Latest✅ 対応

■ 変更点
 1. JST の「今日」を ZoneInfo で取得
 2. Checkbox プロパティ `Latest` を自動追加
 3. 実行のたびに前回の Latest を一括で False にし、
    取り込む行だけ True に設定
 4. Amazon／コミックシーモア 20 位まで取得は従来どおり
"""

from __future__ import annotations

import os
import sys
import re
import time
import datetime as dt
from typing import Dict, Iterator, List
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# ───────────────────────── 環境変数チェック
TOKEN = os.getenv("NOTION_TOKEN")
DB_ID = os.getenv("NOTION_DB")
if not (TOKEN and DB_ID):
    sys.exit("❌ NOTION_TOKEN / NOTION_DB が未設定です。")

# ───────────────────────── 定数
HEAD: Dict[str, str] = {
    "Authorization": f"Bearer {TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
    "Accept": "application/json",
}
UA = {"User-Agent": "Mozilla/5.0 (compatible; rankingbot/2.0)"}

JST = ZoneInfo("Asia/Tokyo")
TODAY = dt.datetime.now(JST).date().isoformat()
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

# ───────────────────────── プロパティ保証ヘルパ
def ensure_select(prop: str, name: str) -> None:
    db = notion(requests.get, f"https://api.notion.com/v1/databases/{DB_ID}").json()
    opts = db["properties"][prop]["select"]["options"]
    if name in {o["name"] for o in opts}:
        return
    opts.append({"name": name})
    patch = {"properties": {prop: {"select": {"options": opts}}}}
    notion(requests.patch, f"https://api.notion.com/v1/databases/{DB_ID}", json=patch)
    print(f"➕ Added option '{name}' to {prop}")

def ensure_checkbox(prop: str) -> None:
    db = notion(requests.get, f"https://api.notion.com/v1/databases/{DB_ID}").json()
    if prop in db["properties"]:
        return
    patch = {"properties": {prop: {"checkbox": {}}}}
    notion(requests.patch, f"https://api.notion.com/v1/databases/{DB_ID}", json=patch)
    print(f"🆕 Checkbox '{prop}' created")

# ───────────────────────── Latest フラグ操作
def clear_latest() -> None:
    """前回付けた Latest✅ をすべて外す"""
    payload = {"filter": {"property": "Latest", "checkbox": {"equals": True}}}
    res = notion(
        requests.post,
        f"https://api.notion.com/v1/databases/{DB_ID}/query",
        json=payload,
    ).json()
    for page in res.get("results", []):
        notion(
            requests.patch,
            f"https://api.notion.com/v1/pages/{page['id']}",
            json={"properties": {"Latest": {"checkbox": False}}},
        )
    if res.get("results"):
        print(f"↩️  Cleared {len(res['results'])} Latest flags")

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
    ensure_select("Store", row["store"])
    ensure_select("Category", row["cat"])

    img_ok = HTTPS_IMG.match(row["thumb"]) is not None

    props = {
        "Date":      {"date":   {"start": TODAY}},
        "Store":     {"select": {"name": row["store"]}},
        "Category":  {"select": {"name": row["cat"]}},
        "Rank":      {"number": row["rank"]},
        "Title":     {"title":  [{"text": {"content": row["title"]}}]},
        "URL":       {"url":    row["url"]},
        "Thumb":     {"url":    row["thumb"] if img_ok else ""},
        "Latest":    {"checkbox": True},                 # ← 追加
    }

    body = {"properties": props}

    if img_ok:
        body["cover"] = {
            "type": "external",
            "external": {"url": row["thumb"]},
        }

    hit = query(row["store"], row["cat"], row["rank"])
    if hit:
        notion(
            requests.patch,
            f"https://api.notion.com/v1/pages/{hit[0]['id']}",
            json=body,
        )
    else:
        body["parent"] = {"database_id": DB_ID}
        notion(
            requests.post,
            "https://api.notion.com/v1/pages",
            json=body,
        )
    print("✅", row["title"][:30])

# ───────────────────────── スクレイパ
def amazon_thumb(div):
    img = div.select_one("img[src]")
    return "" if not img else re.sub(r"_AC_[^_.]+_", "_SX600_", img["src"], 1)

def fetch_amazon() -> Iterator[Dict]:
    url = "https://www.amazon.co.jp/gp/bestsellers/books/2278488051"
    soup = BeautifulSoup(
        requests.get(url, headers=UA, timeout=10).text, "html.parser"
    )
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
    soup = BeautifulSoup(
        requests.get(url, headers=UA, timeout=10).text, "html.parser"
    )
    for rank, li in enumerate(
        soup.select("ul#ranking_result_list li.search_result_box")[:20], 1
    ):
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
    print("=== START", dt.datetime.now(JST))

    # 1) チェックボックス列を保証
    ensure_checkbox("Latest")
    # 2) 前回の Latest✅ を全解除
    clear_latest()

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
        print("=== DONE ", dt.datetime.now(JST))
