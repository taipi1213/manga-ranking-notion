#!/usr/bin/env python3
import os, time, datetime as dt, requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pprint import pformat

#──────────────────────────────
TOKEN = os.environ["NOTION_TOKEN"]
DB_ID = os.environ["NOTION_DB"]
HEAD  = {"Authorization": f"Bearer {TOKEN}",
         "Notion-Version": "2022-06-28",
         "Content-Type":   "application/json"}
UA    = {"User-Agent": "Mozilla/5.0", "Accept-Language": "ja-JP,ja;q=0.9"}
TODAY = dt.date.today().isoformat()

#─── Notion helper ─────────────────────────────────────────────
def query_page(store, cat, rank):
    q = {"filter":{"and":[
        {"property":"Date","date":{"equals":TODAY}},
        {"property":"Store","select":{"equals":store}},
        {"property":"Category","select":{"equals":cat}},
        {"property":"Rank","number":{"equals":rank}}
    ]}}
    r = requests.post(f"https://api.notion.com/v1/databases/{DB_ID}/query",
                      headers=HEAD, json=q, timeout=10)
    r.raise_for_status()
    return r.json()["results"]

def upsert(row):
    """ページ作成 / 更新 (Thumb と Cover を両方設定)"""
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
        requests.patch(f"https://api.notion.com/v1/pages/{pid}",
                       headers=HEAD, json=body, timeout=10).raise_for_status()
    else:
        body["parent"] = {"database_id": DB_ID}
        requests.post("https://api.notion.com/v1/pages",
                      headers=HEAD, json=body, timeout=10).raise_for_status()

#─── Amazon ───────────────────────────────────────────────────
def amazon_thumb(detail_url):
    html = requests.get(detail_url, headers=UA, timeout=10).text
    soup = BeautifulSoup(html, "html.parser")
    meta = soup.find("meta", property="og:image")
    if meta and meta["content"].startswith("https://"):
        # 大サイズに置換 例: ..._SX160_ → _SX800_
        return meta["content"].replace("_SX160_", "_SX800_")
    return ""

def fetch_amazon(limit=20):
    url = "https://www.amazon.co.jp/gp/bestsellers/books/2278488051"
    soup = BeautifulSoup(requests.get(url, headers=UA, timeout=10).text,"html.parser")
    for rank, div in enumerate(soup.select("div.zg-grid-general-faceout")[:limit], 1):
        a     = div.find_parent("a", class_="a-link-normal")
        href  = "https://www.amazon.co.jp" + a["href"]
        title = div.select_one("img[alt]")["alt"].strip()
        yield {"store":"Amazon","cat":"コミック売れ筋","rank":rank,
               "title":title,"url":href,"thumb":amazon_thumb(href)}

#─── Cmoa ────────────────────────────────────────────────────
def cmoa_thumb(li):
    img = li.select_one("img[src]")
    return urljoin("https://www.cmoa.jp", img["src"]) if img else ""

def fetch_cmoa(cat, url, limit=20):
    soup = BeautifulSoup(requests.get(url, headers=UA, timeout=10).text,"html.parser")
    for i, li in enumerate(soup.select("ul#ranking_result_list li.search_result_box")[:limit], 1):
        a     = li.select_one("a.title")
        title = li.select_one("img[alt]")["alt"].strip()
        href  = "https://www.cmoa.jp" + a["href"]
        yield {"store":"Cmoa","cat":cat,"rank":i,
               "title":title,"url":href,"thumb":cmoa_thumb(li)}

CATS = [("総合","https://www.cmoa.jp/search/purpose/ranking/all/"),
        ("少年マンガ","https://www.cmoa.jp/search/purpose/ranking/boy/"),
        ("青年マンガ","https://www.cmoa.jp/search/purpose/ranking/gentle/"),
        ("ライトアダルト","https://www.cmoa.jp/search/purpose/ranking/sexy/")]

#─── Main ────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=== START", dt.datetime.now())
    for r in fetch_amazon():
        upsert(r); time.sleep(0.5)
    for cat,url in CATS:
        for r in fetch_cmoa(cat,url):
            upsert(r); time.sleep(0.5)
    print("=== DONE ", dt.datetime.now())
