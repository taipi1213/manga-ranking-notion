#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, time, datetime as dt, re, requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

TOKEN = os.environ["NOTION_TOKEN"]
DB_ID = os.environ["NOTION_DB"]
HEAD  = {"Authorization": f"Bearer {TOKEN}",
         "Notion-Version":"2022-06-28",
         "Content-Type":"application/json"}
UA    = {"User-Agent":"Mozilla/5.0","Accept-Language":"ja-JP,ja;q=0.9"}
TODAY = dt.date.today().isoformat()
HTTPS_IMG = re.compile(r"^https://.*\.(jpg|jpeg|png|webp)$", re.I)

def notion_request(method, url, **kw):
    for r in range(3):
        resp = method(url, headers=HEAD, timeout=10, **kw)
        if resp.status_code < 500: return resp
        time.sleep(2**r)
    resp.raise_for_status()

def file_obj(url): return {"type":"external","name":url.split('/')[-1],"external":{"url":url}}

def query_page(store, cat, rank):
    q={"filter":{"and":[
        {"property":"Date","date":{"equals":TODAY}},
        {"property":"Store","select":{"equals":store}},
        {"property":"Category","select":{"equals":cat}},
        {"property":"Rank","number":{"equals":rank}}]}}
    return notion_request(requests.post,f"https://api.notion.com/v1/databases/{DB_ID}/query",json=q).json()["results"]

def upsert(row):
    img_ok=HTTPS_IMG.match(row["thumb"]) is not None
    props={"Date":{"date":{"start":TODAY}},
           "Store":{"select":{"name":row["store"]}},          # ← 値を固定
           "Category":{"select":{"name":row["cat"]}},         # ← 値を固定
           "Rank":{"number":row["rank"]},
           "Title":{"title":[{"text":{"content":row["title"]}}]},
           "URL":{"url":row["url"]},
           "Thumb":{"files":[file_obj(row["thumb"])] if img_ok else []}}
    body={"properties":props}
    if img_ok: body["cover"]=file_obj(row["thumb"])
    hit=query_page(row["store"],row["cat"],row["rank"])
    if hit:
        url=f"https://api.notion.com/v1/pages/{hit[0]['id']}"
        notion_request(requests.patch,url,json=body)
    else:
        body["parent"]={"database_id":DB_ID}
        notion_request(requests.post,"https://api.notion.com/v1/pages",json=body)
    print("✅",row["title"][:30])

# --- Amazon ---------------------------------------------------
def amazon_thumb(div):
    img=div.select_one("img[src]")
    if not img: return ""
    return re.sub(r"_AC_[^_.]+_", "_SX600_", img["src"])

def fetch_amazon():
    base="https://www.amazon.co.jp"
    soup=BeautifulSoup(requests.get(f"{base}/gp/bestsellers/books/2278488051",headers=UA).text,"html.parser")
    for r,div in enumerate(soup.select("div.zg-grid-general-faceout")[:20],1):
        title=div.select_one("img[alt]")["alt"].strip()
        href=urljoin(base,(div.find_parent("a") or div.select_one("a[href]"))["href"])
        yield {"store":"Amazon","cat":"コミック売れ筋","rank":r,"title":title,"url":href,"thumb":amazon_thumb(div)}

# --- Cmoa -----------------------------------------------------
def cmoa_thumb(li):
    img=li.select_one("img[src]")
    return ("https:"+img["src"]) if img and img["src"].startswith("//") else (img["src"] if img else "")

def fetch_cmoa(cat,url):
    soup=BeautifulSoup(requests.get(url,headers=UA).text,"html.parser")
    for i,li in enumerate(soup.select("ul#ranking_result_list li.search_result_box")[:20],1):
        title=li.select_one("img[alt]")["alt"].strip()
        href=urljoin("https://www.cmoa.jp",li.select_one("a.title")["href"])
        yield {"store":"Cmoa","cat":cat,"rank":i,"title":title,"url":href,"thumb":cmoa_thumb(li)}

CATS=[("総合","https://www.cmoa.jp/search/purpose/ranking/all/"),
      ("少年マンガ","https://www.cmoa.jp/search/purpose/ranking/boy/"),
      ("青年マンガ","https://www.cmoa.jp/search/purpose/ranking/gentle/"),
      ("ライトアダルト","https://www.cmoa.jp/search/purpose/ranking/sexy/")]

if __name__=="__main__":
    print("=== START",dt.datetime.now())
    for row in fetch_amazon():       upsert(row); time.sleep(0.4)
    for cat,url in CATS:
        for row in fetch_cmoa(cat,url): upsert(row); time.sleep(0.4)
    print("=== DONE ",dt.datetime.now())
