#!/usr/bin/env python3
"""
Amazon コミック売れ筋 TOP20 ＋ コミックシーモア 4カテゴリ TOP20
= 計 100 件を日次で Notion データベースに upsert。
Thumb 列（Files 型）と Cover にサムネを設定し、
画像が無い行は自動的に Thumb / Cover をスキップする安全設計。
"""

import os, time, datetime as dt, re, requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# ── Notion API 共通設定 ──────────────────────────────
TOKEN = os.environ["NOTION_TOKEN"]
DB_ID = os.environ["NOTION_DB"]
HEAD  = {"Authorization": f"Bearer {TOKEN}",
         "Notion-Version":"2022-06-28",
         "Content-Type":"application/json"}
UA    = {"User-Agent":"Mozilla/5.0", "Accept-Language":"ja-JP,ja;q=0.9"}
TODAY = dt.date.today().isoformat()
HTTPS_IMG = re.compile(r"^https://.+\.(jpg|jpeg|png|webp)$", re.I)

# ── Notion ヘルパ ──────────────────────────────────
def query_page(store, cat, rank):
    q = {"filter":{"and":[
        {"property":"Date","date":{"equals":TODAY}},
        {"property":"Store","select":{"equals":store}},
        {"property":"Category","select":{"equals":cat}},
        {"property":"Rank","number":{"equals":rank}}]}}
    r = requests.post(f"https://api.notion.com/v1/databases/{DB_ID}/query",
                      headers=HEAD, json=q, timeout=10)
    r.raise_for_status()
    return r.json()["results"]

def file_obj(url: str) -> dict:
    """Files 型が要求するフル属性"""
    return {"type":"external",
            "name":url.split("/")[-1] or "thumb.jpg",
            "external":{"url":url}}

def upsert(row):
    have_img = bool(HTTPS_IMG.match(row["thumb"]))
    props = {
        "Date"    : {"date"  : {"start":TODAY}},
        "Store"   : {"select": {"name":row["store"]}},
        "Category": {"select": {"name":row["cat"]}},
        "Rank"    : {"number": row["rank"]},
        "Title"   : {"title" : [{"text":{"content":row["title"]}}]},
        "URL"     : {"url"   : row["url"]},
        "Thumb"   : {"files" : [file_obj(row["thumb"])] if have_img else []}
    }
    body = {"properties":props}
    if have_img:
        body["cover"] = file_obj(row["thumb"])

    hit  = query_page(row["store"], row["cat"], row["rank"])
    req  = requests.patch if hit else requests.post
    url  = (f"https://api.notion.com/v1/pages/{hit[0]['id']}"
            if hit else "https://api.notion.com/v1/pages")
    if not hit:
        body["parent"] = {"database_id":DB_ID}

    resp = req(url, headers=HEAD, json=body, timeout=10)
    print("Notion:", resp.status_code, row["title"][:25])
    resp.raise_for_status()

# ── Amazon スクレイピング ─────────────────────────
def amazon_thumb(detail_url):
    try:
        soup = BeautifulSoup(requests.get(detail_url,headers=UA,timeout=10).text,
                             "html.parser")
        og = soup.find("meta",property="og:image")
        if og and "https://" in og["content"]:
            return og["content"].replace("_SX160_","_SX600_")
    except Exception:
        pass
    return ""

def fetch_amazon(limit=20):
    base = "https://www.amazon.co.jp"
    soup = BeautifulSoup(
        requests.get(f"{base}/gp/bestsellers/books/2278488051",
                     headers=UA,timeout=10).text,"html.parser")
    for rank, div in enumerate(soup.select("div.zg-grid-general-faceout")[:limit],1):
        img   = div.select_one("img[alt]")
        title = img["alt"].strip() if img else f"A-Rank{rank}"
        a_tag = div.find_parent("a") or div.select_one("a[href]")
        href  = urljoin(base, a_tag["href"]) if a_tag else base
        yield {"store":"Amazon","cat":"コミック売れ筋","rank":rank,
               "title":title,"url":href,"thumb":amazon_thumb(href)}

# ── コミックシーモア ─────────────────────────────
def cmoa_thumb(li):
    img = li.select_one("img[src]")
    if not img: return ""
    url = img["src"]
    return ("https:" + url) if url.startswith("//") else url

def fetch_cmoa(cat, url, limit=20):
    soup = BeautifulSoup(requests.get(url,headers=UA,timeout=10).text,"html.parser")
    for i, li in enumerate(soup.select("ul#ranking_result_list li.search_result_box")[:limit],1):
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

# ── メイン ─────────────────────────────────────
if __name__ == "__main__":
    print("=== START", dt.datetime.now())
    for r in fetch_amazon():
        upsert(r); time.sleep(0.5)           # Notion 3 rps 制限
    for cat,url in CATS:
        for r in fetch_cmoa(cat,url):
            upsert(r); time.sleep(0.5)
    print("=== DONE ", dt.datetime.now())
