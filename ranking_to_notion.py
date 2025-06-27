#!/usr/bin/env python3
import os, time, datetime as dt, requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

TOKEN = os.environ["NOTION_TOKEN"]
DB_ID = os.environ["NOTION_DB"]
HEAD  = {"Authorization": f"Bearer {TOKEN}",
         "Notion-Version":"2022-06-28",
         "Content-Type":"application/json"}
UA    = {"User-Agent":"Mozilla/5.0","Accept-Language":"ja-JP,ja;q=0.9"}
TODAY = dt.date.today().isoformat()

# ---------- Notion upsert ----------
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
    body = {"properties":props,
            "cover":{"type":"external",
                     "external":{"url":row["thumb"]}}}

    hit = query_page(row["store"], row["cat"], row["rank"])
    resp = (requests.patch if hit else requests.post)(
        f"https://api.notion.com/v1/pages/{hit[0]['id']}" if hit
        else "https://api.notion.com/v1/pages",
        headers=HEAD,
        json=body | ({} if hit else {"parent":{"database_id":DB_ID}}),
        timeout=10)
    print("Notion-API:", resp.status_code, resp.text[:120])
    resp.raise_for_status()

# ---------- Amazon ----------
def amazon_thumb(url):
    soup = BeautifulSoup(requests.get(url,headers=UA,timeout=10).text,"html.parser")
    og   = soup.find("meta",property="og:image")
    return og["content"].replace("_SX160_","_SX800_") if og else ""

def fetch_amazon(limit=20):
    soup = BeautifulSoup(
        requests.get("https://www.amazon.co.jp/gp/bestsellers/books/2278488051",
                     headers=UA,timeout=10).text,"html.parser")
    for rank, div in enumerate(soup.select("div.zg-grid-general-faceout")[:limit],1):
        img   = div.select_one("img[alt]")
        title = img["alt"].strip() if img else f"Rank{rank}"
        href  = urljoin("https://www.amazon.co.jp",
                        (div.find_parent("a") or div.select_one("a[href]"))["href"])
        yield {"store":"Amazon","cat":"コミック売れ筋","rank":rank,
               "title":title,"url":href,"thumb":amazon_thumb(href)}

# ---------- Cmoa ----------
def cmoa_thumb(li):
    img = li.select_one("img[src]")
    return urljoin("https://www.cmoa.jp", img["src"]) if img else ""

def fetch_cmoa(cat, url, limit=20):
    soup = BeautifulSoup(requests.get(url,headers=UA,timeout=10).text,"html.parser")
    for i, li in enumerate(soup.select("ul#ranking_result_list li.search_result_box")[:limit],1):
        title = li.select_one("img[alt]")["alt"].strip()
        href  = urljoin("https://www.cmoa.jp", li.select_one("a.title")["href"])
        yield {"store":"Cmoa","cat":cat,"rank":i,
               "title":title,"url":href,"thumb":cmoa_thumb(li)}

CATS=[("総合","https://www.cmoa.jp/search/purpose/ranking/all/"),
      ("少年マンガ","https://www.cmoa.jp/search/purpose/ranking/boy/"),
      ("青年マンガ","https://www.cmoa.jp/search/purpose/ranking/gentle/"),
      ("ライトアダルト","https://www.cmoa.jp/search/purpose/ranking/sexy/")]

if __name__ == "__main__":
    print("=== START", dt.datetime.now())
    for r in fetch_amazon():
        upsert(r); time.sleep(0.5)
    for cat,url in CATS:
        for r in fetch_cmoa(cat,url):
            upsert(r); time.sleep(0.5)
    print("=== DONE ", dt.datetime.now())
