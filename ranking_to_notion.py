#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Amazon コミック売れ筋 TOP20 ＋ コミックシーモア 4カテゴリ TOP20
= 100 件を毎日 Notion に upsert。
・画像 URL が取れなくても自動で “画像なし” にフォールバック
・Notion API が 502/503 を返しても指数バックオフでリトライ
"""

import os, time, datetime as dt, re, requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# ── 環境変数 (GitHub Secrets) ──────────────────────────
TOKEN = os.environ["NOTION_TOKEN"]
DB_ID = os.environ["NOTION_DB"]

HEAD = {
    "Authorization": f"Bearer {TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}
UA = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ja-JP,ja;q=0.9"
}
TODAY = dt.date.today().isoformat()
HTTPS_IMG = re.compile(r"^https://.*\.(jpg|jpeg|png|webp)$", re.I)

# ── Notion 共通リクエスト (3 回までリトライ) ───────────
def notion_request(method, url: str, **kw):
    for retry in range(3):
        resp = method(url, headers=HEAD, timeout=10, **kw)
        if resp.status_code < 500:              # 200〜499 は返す
            return resp
        print(f"🔄  Retry {retry+1}/3 on {resp.status_code}")
        time.sleep(2 ** retry)                  # 1s → 2s → 4s
    resp.raise_for_status()                     # 3 回全部 5xx なら例外

# ── Notion ヘルパ ────────────────────────────────────
def file_obj(url: str) -> dict:
    return {
        "type": "external",
        "name": url.split("/")[-1] or "thumb.jpg",
        "external": {"url": url}
    }

def query_page(store, cat, rank):
    q = {"filter":{"and":[
        {"property":"Date","date":{"equals":TODAY}},
        {"property":"Store","select":{"equals":store}},
        {"property":"Category","select":{"equals":cat}},
        {"property":"Rank","number":{"equals":rank}}
    ]}}
    resp = notion_request(requests.post,
                          f"https://api.notion.com/v1/databases/{DB_ID}/query",
                          json=q)
    return resp.json()["results"]

def upsert(row):
    img_ok = HTTPS_IMG.match(row["thumb"]) is not None
    props = {
        "Date"    : {"date" : {"start": TODAY}},
        "Store"   : {"select": {"name": row["store"]}},
        "Category": {"select": {"name": row["cat"]}},
        "Rank"    : {"number": row["rank"]},
        "Title"   : {"title" : [{"text":{"content": row["title"]}}]},
        "URL"     : {"url"   : row["url"]},
        "Thumb"   : {"files" : [file_obj(row["thumb"])] if img_ok else []}
    }
    body = {"properties": props}
    if img_ok:
        body["cover"] = file_obj(row["thumb"])

    hit = query_page(row["store"], row["cat"], row["rank"])
    if hit:
        url  = f"https://api.notion.com/v1/pages/{hit[0]['id']}"
        resp = notion_request(requests.patch, url, json=body)
    else:
        body["parent"] = {"database_id": DB_ID}
        url  = "https://api.notion.com/v1/pages"
        resp = notion_request(requests.post, url, json=body)

    status = "with img" if img_ok else "no img "
    print(f"✅ {row['title'][:28]} ({status})")

# ── Amazon ───────────────────────────────────────────
def amazon_thumb(detail):
    try:
        s = BeautifulSoup(requests.get(detail, headers=UA, timeout=10).text,
                          "html.parser")
        og = s.find("meta", property="og:image")
        if og and "https://" in og["content"]:
            return og["content"].replace("_SX160_", "_SX600_")
    except Exception:
        pass
    return ""

def fetch_amazon(limit=20):
    base = "https://www.amazon.co.jp"
    s = BeautifulSoup(
        requests.get(f"{base}/gp/bestsellers/books/2278488051",
                     headers=UA, timeout=10).text, "html.parser")
    for rank, div in enumerate(s.select("div.zg-grid-general-faceout")[:limit], 1):
        img   = div.select_one("img[alt]")
        title = img["alt"].strip() if img else f"A-Rank{rank}"
        a_tag = div.find_parent("a") or div.select_one("a[href]")
        href  = urljoin(base, a_tag["href"]) if a_tag else base
        yield {"store":"Amazon","cat":"コミック売れ筋","rank":rank,
               "title":title,"url":href,"thumb":amazon_thumb(href)}

# ── コミックシーモア ──────────────────────────────
def cmoa_thumb(li):
    img = li.select_one("img[src]")
    if not img:
        return ""
    src = img["src"]
    return "https:" + src if src.startswith("//") else src

def fetch_cmoa(cat, url, limit=20):
    s = BeautifulSoup(requests.get(url, headers=UA, timeout=10).text,
                      "html.parser")
    for i, li in enumerate(
          s.select("ul#ranking_result_list li.search_result_box")[:limit], 1):
        title = li.select_one("img[alt]")["alt"].strip()
        href  = urljoin("https://www.cmoa.jp", li.select_one("a.title")["href"])
        yield {"store":"Cmoa","cat":cat,"rank":i,
               "title":title,"url":href,"thumb":cmoa_thumb(li)}

CATS = [
    ("総合",         "https://www.cmoa.jp/search/purpose/ranking/all/"),
    ("少年マンガ",   "https://www.cmoa.jp/search/purpose/ranking/boy/"),
    ("青年マンガ",   "https://www.cmoa.jp/search/purpose/ranking/gentle/"),
    ("ライトアダルト","https://www.cmoa.jp/search/purpose/ranking/sexy/")
]

# ── メイン ───────────────────────────────────────
if __name__ == "__main__":
    print("=== START", dt.datetime.now())
    for r in fetch_amazon():
        upsert(r); time.sleep(0.5)          # Notion 3 rps 制限
    for cat, url in CATS:
        for r in fetch_cmoa(cat, url):
            upsert(r); time.sleep(0.5)
    print("=== DONE ", dt.datetime.now())
