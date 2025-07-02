#!/usr/bin/env python3
"""Daily manga ranking → Notion DB   (2025-07-02 fix)
・JST 11 時実行、Latest✅ 方式
・Thumb が無い場合は url=None を渡す                ★← NEW
"""

from __future__ import annotations
import os, sys, re, time, datetime as dt
from typing import Dict, Iterator, List
from urllib.parse import urljoin
from zoneinfo import ZoneInfo
import requests
from bs4 import BeautifulSoup

# ──────────── 環境変数
TOKEN, DB_ID = os.getenv("NOTION_TOKEN"), os.getenv("NOTION_DB")
if not (TOKEN and DB_ID):
    sys.exit("❌ NOTION_TOKEN / NOTION_DB が未設定です")

HEAD = {
    "Authorization": f"Bearer {TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
    "Accept": "application/json",
}
UA = {"User-Agent": "Mozilla/5.0 (rankingbot/2.1)"}

JST   = ZoneInfo("Asia/Tokyo")
TODAY = dt.datetime.now(JST).date().isoformat()
IMG_OK = re.compile(r"^https://.*\.(?:jpe?g|png|webp)$", re.I)

# ──────────── Notion API ヘルパ
def notion(method, url: str, **kw):
    for r in range(3):
        res = method(url, headers=HEAD, timeout=10, **kw)
        if res.status_code in (429, 502, 503):
            time.sleep(2 ** r); continue
        break
    if not res.ok:
        print("❌", res.status_code, res.text)
        res.raise_for_status()
    return res

# ──────────── プロパティ保証
def ensure_checkbox(name):
    db = notion(requests.get, f"https://api.notion.com/v1/databases/{DB_ID}").json()
    if name not in db["properties"]:
        notion(requests.patch, f"https://api.notion.com/v1/databases/{DB_ID}",
               json={"properties": {name: {"checkbox": {}}}})
        print(f"🆕 Checkbox '{name}' created")

def ensure_select(prop, val):
    db = notion(requests.get, f"https://api.notion.com/v1/databases/{DB_ID}").json()
    opts = db["properties"][prop]["select"]["options"]
    if val not in {o["name"] for o in opts}:
        opts.append({"name": val})
        notion(requests.patch, f"https://api.notion.com/v1/databases/{DB_ID}",
               json={"properties": {prop: {"select": {"options": opts}}}})

# ──────────── Latest を全解除
def clear_latest():
    q = {"filter": {"property": "Latest", "checkbox": {"equals": True}}}
    hits = notion(requests.post, f"https://api.notion.com/v1/databases/{DB_ID}/query",
                  json=q).json().get("results", [])
    for p in hits:
        notion(requests.patch, f"https://api.notion.com/v1/pages/{p['id']}",
               json={"properties": {"Latest": {"checkbox": False}}})
    if hits:
        print(f"↩️  Cleared {len(hits)} Latest flags")

# ──────────── 既存ページ検索
def query(store, cat, rank):
    q = {"filter": {"and": [
        {"property": "Date", "date": {"equals": TODAY}},
        {"property": "Store", "select": {"equals": store}},
        {"property": "Category", "select": {"equals": cat}},
        {"property": "Rank", "number": {"equals": rank}},
    ]}}
    return notion(requests.post, f"https://api.notion.com/v1/databases/{DB_ID}/query",
                  json=q).json().get("results", [])

# ──────────── UPSERT
def upsert(row):
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

    if IMG_OK.match(row["thumb"] or ""):
        props["Thumb"] = {"url": row["thumb"]}
        cover = {"type": "external", "external": {"url": row["thumb"]}}
    else:                               # ← url なしは null
        props["Thumb"] = {"url": None}
        cover = None

    body = {"properties": props}
    if cover: body["cover"] = cover

    hit = query(row["store"], row["cat"], row["rank"])
    if hit:
        notion(requests.patch, f"https://api.notion.com/v1/pages/{hit[0]['id']}",
               json=body)
    else:
        body["parent"] = {"database_id": DB_ID}
        notion(requests.post, "https://api.notion.com/v1/pages", json=body)

    print("✅", row["title"][:30])

# ──────────── スクレイパ
def amazon_thumb(div):
    img = div.select_one("img[src]")
    return "" if not img else re.sub(r"_AC_[^_.]+_", "_SX600_", img["src"], 1)

def fetch_amazon():
    soup = BeautifulSoup(requests.get(
        "https://www.amazon.co.jp/gp/bestsellers/books/2278488051",
        headers=UA, timeout=10).text, "html.parser")
    for rk, d in enumerate(soup.select("div.zg-grid-general-faceout")[:20], 1):
        yield {
            "store": "Amazon", "cat": "コミック売れ筋", "rank": rk,
            "title": d.select_one("img[alt]")["alt"].strip(),
            "url": urljoin("https://www.amazon.co.jp",
                           (d.find_parent("a") or d.select_one("a[href]"))["href"]),
            "thumb": amazon_thumb(d),
        }

def cmoa_thumb(li):
    img = li.select_one("img[src]")
    if not img: return ""
    src = img["src"]
    return "https:" + src if src.startswith("//") else src

def fetch_cmoa(cat, url):
    soup = BeautifulSoup(requests.get(url, headers=UA, timeout=10).text, "html.parser")
    for rk, li in enumerate(soup.select("ul#ranking_result_list li.search_result_box")[:20], 1):
        yield {
            "store": "Cmoa", "cat": cat, "rank": rk,
            "title": li.select_one("img[alt]")["alt"].strip(),
            "url": urljoin("https://www.cmoa.jp", li.select_one("a.title")["href"]),
            "thumb": cmoa_thumb(li),
        }

CATS = [
    ("総合", "https://www.cmoa.jp/search/purpose/ranking/all/"),
    ("少年マンガ", "https://www.cmoa.jp/search/purpose/ranking/boy/"),
    ("青年マンガ", "https://www.cmoa.jp/search/purpose/ranking/gentle/"),
    ("ライトアダルト", "https://www.cmoa.jp/search/purpose/ranking/sexy/"),
]

# ──────────── Main
if __name__ == "__main__":
    print("=== START", dt.datetime.now(JST))
    ensure_checkbox("Latest")
    clear_latest()
    try:
        for r in fetch_amazon(): upsert(r); time.sleep(0.4)
        for cat, u in CATS:
            for r in fetch_cmoa(cat, u): upsert(r); time.sleep(0.4)
    finally:
        print("=== DONE ", dt.datetime.now(JST))
