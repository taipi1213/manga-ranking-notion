#!/usr/bin/env python3
import os, time, datetime as dt, requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pprint import pformat

#── 環境変数 ──────────────────────────────────────────────
TOKEN = os.environ["NOTION_TOKEN"]
DB_ID = os.environ["NOTION_DB"]
HEAD  = {"Authorization": f"Bearer {TOKEN}",
         "Notion-Version": "2022-06-28",
         "Content-Type": "application/json"}
UA    = {"User-Agent":"Mozilla/5.0", "Accept-Language":"ja-JP,ja;q=0.9"}
TODAY = dt.date.today().isoformat()

#── Notion API ラッパ ──────────────────────────────────
def notion_query(store, cat, rank):
    q = {"filter":{"and":[
        {"property":"Date","date":{"equals":TODAY}},
        {"property":"Store","select":{"equals":store}},
        {"property":"Category","select":{"equals":cat}},
        {"property":"Rank","number":{"equals":rank}}]}}
    r = requests.post(f"https://api.notion.com/v1/databases/{DB_ID}/query",
                      headers=HEAD, json=q, timeout=10)
    return r.json().get("results", [])

def upsert(row):
    props = {
        "Date":     {"date":{"start":TODAY}},
        "Store":    {"select":{"name":row["store"]}},
        "Category": {"select":{"name":row["cat"]}},
        "Rank":     {"number":row["rank"]},
        "Title":    {"title":[{"text":{"content":row["title"]}}]},
        "URL":      {"url":row["url"]}
    }
    page = {"parent":{"database_id":DB_ID},
            "cover":{"type":"external","external":{"url":row["thumb"]}},
            "properties":props}
    hit = notion_query(row["store"], row["cat"], row["rank"])
    if hit:
        pid = hit[0]["id"]
        requests.patch(f"https://api.notion.com/v1/pages/{pid}",
                       headers=HEAD, json=page, timeout=10).raise_for_status()
    else:
        requests.post("https://api.notion.com/v1/pages",
                      headers=HEAD, json=page, timeout=10).raise_for_status()

#── Amazon スクレイピング ─────────────────────────────
def amazon_thumb(detail_url):
    s = BeautifulSoup(requests.get(detail_url,headers=UA,timeout=10).text,"html.parser")
    meta = s.find("meta",property="og:image")
    return meta["content"] if meta else ""

def fetch_amazon(limit=20):
    url = "https://www.amazon.co.jp/gp/bestsellers/books/2278488051"
    soup = BeautifulSoup(requests.get(url,headers=UA,timeout=10).text,"html.parser")
    for rank, face in enumerate(soup.select("div.zg-grid-general-faceout")[:limit],1):
        a     = face.find_parent("a",class_="a-link-normal")
        href  = "https://www.amazon.co.jp"+a["href"]
        title = face.select_one("img[alt]")["alt"].strip()
        yield {"store":"Amazon","cat":"コミック売れ筋","rank":rank,
               "title":title,"url":href,"thumb":amazon_thumb(href)}

#── Cmoa スクレイピング ──────────────────────────────
def cmoa_thumb(li):
    img = li.select_one("img[src]")
    return urljoin("https://www.cmoa.jp", img["src"]) if img else ""

def fetch_cmoa(cat, url, limit=20):
    s = BeautifulSoup(requests.get(url,headers=UA,timeout=10).text,"html.parser")
    for i, li in enumerate(s.select("ul#ranking_result_list li.search_result_box")[:limit],1):
        a = li.select_one("a.title")
        title = li.select_one("img[alt]")["alt"].strip()
        href  = "https://www.cmoa.jp"+a["href"]
        yield {"store":"Cmoa","cat":cat,"rank":i,
               "title":title,"url":href,"thumb":cmoa_thumb(li)}

CATS = [("総合","https://www.cmoa.jp/search/purpose/ranking/all/"),
        ("少年マンガ","https://www.cmoa.jp/search/purpose/ranking/boy/"),
        ("青年マンガ","https://www.cmoa.jp/search/purpose/ranking/gentle/"),
        ("ライトアダルト","https://www.cmoa.jp/search/purpose/ranking/sexy/")]

#── メイン ───────────────────────────────────────────
if __name__ == "__main__":
    print("START", dt.datetime.now())
    for r in fetch_amazon():                  # Amazon 20 行
        print("AMZ", r["rank"], r["title"][:15])
        upsert(r); time.sleep(0.5)
    for cat,url in CATS:                     # Cmoa 4×20 = 80 行
        for r in fetch_cmoa(cat,url):
            print(cat, r["rank"], r["title"][:15])
            upsert(r); time.sleep(0.5)
    print("DONE ", dt.datetime.now())
