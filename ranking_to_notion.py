#!/usr/bin/env python3
"""
Amazon コミック売れ筋 TOP20 ＋ コミックシーモア 4 カテゴリ各 TOP20
合計 100 件を毎日 Notion データベースへ upsert。
Thumb 列（Files 型）と Cover の両方にサムネを設定して
ギャラリー表示でも必ず画像が見えるようにしている完全版。
"""

import os, time, datetime as dt, requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# ──── シークレットから Notion 情報 ────
TOKEN = os.environ["NOTION_TOKEN"]
DB_ID = os.environ["NOTION_DB"]
HEAD  = {
    "Authorization": f"Bearer {TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type":   "application/json"
}
UA    = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "ja-JP,ja;q=0.9"
}
TODAY = dt.date.today().isoformat()

# ──── Notion ヘルパ ────
def query_page(store, cat, rank):
    payload = {
        "filter": {"and": [
            {"property":"Date","date":{"equals":TODAY}},
            {"property":"Store","select":{"equals":store}},
            {"property":"Category","select":{"equals":cat}},
            {"property":"Rank","number":{"equals":rank}}
        ]}
    }
    res = requests.post(f"https://api.notion.com/v1/databases/{DB_ID}/query",
                        headers=HEAD, json=payload, timeout=10)
    res.raise_for_status()
    return res.json()["results"]

def upsert(row):
    props = {
        "Date":     {"date":{"start":TODAY}},
        "Store":    {"select":{"name":row["store"]}},
        "Category": {"select":{"name":row["cat"]}},
        "Rank":     {"number":row["rank"]},
        "Title":    {"title":[{"text":{"content":row["title"]}}]},
        "URL":      {"url":row["url"]},
        "Thumb":    {"files":[{"type":"external","name":"thumb",
                               "external":{"url":row["thumb"]}}]}
    }
    cover = {"cover":{"type":"external","external":{"url":row["thumb"]}}}
    body  = {"properties":props, **cover}

    hit = query_page(row["store"], row["cat"], row["rank"])
    if hit:
        pid  = hit[0]["id"]
        resp = requests.patch(f"https://api.notion.com/v1/pages/{pid}",
                              headers=HEAD, json=body, timeout=10)
    else:
        body["parent"] = {"database_id":DB_ID}
        resp = requests.post("https://api.notion.com/v1/pages",
                             headers=HEAD, json=body, timeout=10)
    resp.raise_for_status()

# ──── Amazon ────
def amazon_thumb(url):
    html = requests.get(url, headers=UA, timeout=10).text
    soup = BeautifulSoup(html, "html.parser")
    og   = soup.find("meta", property="og:image")
    if og and og["content"].startswith("https://"):
        return og["content"].replace("_SX160_", "_SX800_")
    return ""

def fetch_amazon(limit=20):
    base = "https://www.amazon.co.jp"
    soup = BeautifulSoup(
        requests.get(f"{base}/gp/bestsellers/books/2278488051",
                     headers=UA, timeout=10).text, "html.parser")
    for rank, div in enumerate(soup.select("div.zg-grid-general-faceout")[:limit], 1):
        img   = div.select_one("img[alt]")
        title = img["alt"].strip() if img else f"Rank{rank}"
        href_tag = div.find_parent("a", class_="a-link-normal") or div.select_one("a[href]")
        href = urljoin(base, href_tag["href"]) if href_tag else base
        yield {"store":"Amazon", "cat":"コミック売れ筋", "rank":rank,
               "title":title, "url":href, "thumb":amazon_thumb(href)}

# ──── コミックシーモア ────
def cmoa_thumb(li):
    img = li.select_one("img[src]")
    return urljoin("https://www.cmoa.jp", img["src"]) if img else ""

def fetch_cmoa(cat, url, limit=20):
    soup = BeautifulSoup(requests.get(url, headers=UA, timeout=10).text, "html.parser")
    for i, li in enumerate(soup.select("ul#ranking_result_list li.search_result_box")[:limit], 1):
        title = li.select_one("img[alt]")["alt"].strip()
        href  = urljoin("https://www.cmoa.jp", li.select_one("a.title")["href"])
        yield {"store":"Cmoa", "cat":cat, "rank":i,
               "title":title, "url":href, "thumb":cmoa_thumb(li)}

CATS = [
    ("総合",         "https://www.cmoa.jp/search/purpose/ranking/all/"),
    ("少年マンガ",   "https://www.cmoa.jp/search/purpose/ranking/boy/"),
    ("青年マンガ",   "https://www.cmoa.jp/search/purpose/ranking/gentle/"),
    ("ライトアダルト","https://www.cmoa.jp/search/purpose/ranking/sexy/")
]

# ──── メイン ────
if __name__ == "__main__":
    print("=== START", dt.datetime.now())
    for r in fetch_amazon():
        upsert(r); time.sleep(0.5)
    for cat, url in CATS:
        for r in fetch_cmoa(cat, url):
            upsert(r); time.sleep(0.5)
    print("=== DONE ", dt.datetime.now())
