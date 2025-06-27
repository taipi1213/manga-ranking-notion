#!/usr/bin/env python3
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

# ── Notion helpers ────────────────────────────────
def file_obj(url: str) -> dict:
    return {"type":"external",
            "name": url.split("/")[-1] or "thumb.jpg",
            "external":{"url":url}}

def query_page(store, cat, rank):
    flt = {"filter":{"and":[
        {"property":"Date","date":{"equals":TODAY}},
        {"property":"Store","select":{"equals":store}},
        {"property":"Category","select":{"equals":cat}},
        {"property":"Rank","number":{"equals":rank}}]}}
    res = requests.post(f"https://api.notion.com/v1/databases/{DB_ID}/query",
                        headers=HEAD, json=flt, timeout=10)
    res.raise_for_status()
    return res.json()["results"]

def push_to_notion(row):
    """Thumb がダメなら画像なしで自動リトライ"""
    def build_body(with_img: bool):
        props = {
            "Date":  {"date":{"start":TODAY}},
            "Store": {"select":{"name":row['store']}},
            "Category":{"select":{"name":row['cat']}},
            "Rank":  {"number":row['rank']},
            "Title": {"title":[{"text":{"content":row['title']}}]},
            "URL":   {"url":row['url']},
            "Thumb": {"files":[file_obj(row['thumb'])] if with_img else []}
        }
        body = {"properties":props}
        if with_img:
            body["cover"] = file_obj(row['thumb'])
        return body

    hit   = query_page(row['store'], row['cat'], row['rank'])
    url   = (f"https://api.notion.com/v1/pages/{hit[0]['id']}"
             if hit else "https://api.notion.com/v1/pages")
    method= requests.patch if hit else requests.post

    for attempt, with_img in enumerate((HTTPS_IMG.match(row['thumb']) is not None,
                                        False)):          # 1回目:画像付き, 2回目:画像なし
        body = build_body(with_img)
        if not hit and attempt==1:      # POST 時は parent が要る
            body["parent"] = {"database_id":DB_ID}

        resp = method(url, headers=HEAD, json=body, timeout=10)
        if resp.status_code == 200:
            print("✅", row["title"][:25], "(with img)" if with_img else "(no img)")
            return
        elif attempt == 0:              # 画像付きで失敗したら画像抜きでリトライ
            continue
        else:
            print("❌", row["title"][:25])
            print(resp.text)
            resp.raise_for_status()

# ── Amazon ────────────────────────────────────────
def amazon_thumb(detail):
    try:
        s = BeautifulSoup(requests.get(detail,headers=UA,timeout=10).text,"html.parser")
        og= s.find("meta",property="og:image")
        if og and "https://" in og["content"]:
            return og["content"].replace("_SX160_","_SX600_")
    except Exception:
        pass
    return ""

def fetch_amazon(limit=20):
    base="https://www.amazon.co.jp"
    soup=BeautifulSoup(requests.get(f"{base}/gp/bestsellers/books/2278488051",
                     headers=UA,timeout=10).text,"html.parser")
    for rank,div in enumerate(soup.select("div.zg-grid-general-faceout")[:limit],1):
        img   = div.select_one("img[alt]")
        title = img["alt"].strip() if img else f"A-Rank{rank}"
        a_tag = div.find_parent("a") or div.select_one("a[href]")
        href  = urljoin(base,a_tag["href"]) if a_tag else base
        yield {"store":"Amazon","cat":"コミック売れ筋","rank":rank,
               "title":title,"url":href,"thumb":amazon_thumb(href)}

# ── Cmoa ──────────────────────────────────────────
def cmoa_thumb(li):
    img = li.select_one("img[src]")
    if not img: return ""
    src = img["src"]
    return "https:" + src if src.startswith("//") else src

def fetch_cmoa(cat,url,limit=20):
    s=BeautifulSoup(requests.get(url,headers=UA,timeout=10).text,"html.parser")
    for i,li in enumerate(s.select("ul#ranking_result_list li.search_result_box")[:limit],1):
        title = li.select_one("img[alt]")["alt"].strip()
        href  = urljoin("https://www.cmoa.jp",li.select_one("a.title")["href"])
        yield {"store":"Cmoa","cat":cat,"rank":i,
               "title":title,"url":href,"thumb":cmoa_thumb(li)}

CATS=[("総合","https://www.cmoa.jp/search/purpose/ranking/all/"),
      ("少年マンガ","https://www.cmoa.jp/search/purpose/ranking/boy/"),
      ("青年マンガ","https://www.cmoa.jp/search/purpose/ranking/gentle/"),
      ("ライトアダルト","https://www.cmoa.jp/search/purpose/ranking/sexy/")]

# ── メイン ─────────────────────────────────────
if __name__=="__main__":
    print("=== START", dt.datetime.now())
    for r in fetch_amazon():
        push_to_notion(r); time.sleep(0.5)
    for cat,url in CATS:
        for r in fetch_cmoa(cat,url):
            push_to_notion(r); time.sleep(0.5)
    print("=== DONE ", dt.datetime.now())
