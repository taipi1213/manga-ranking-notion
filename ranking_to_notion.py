#!/usr/bin/env python3
import os, time, datetime as dt, requests
from bs4 import BeautifulSoup

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DB_ID        = os.environ["NOTION_DB"]
HEADERS      = {"Authorization": f"Bearer {NOTION_TOKEN}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json"}

TODAY = dt.date.today().isoformat()

# ---------- Notion helper -------------------------------------------------
def upsert(row):
    """Date+Store+Category+Rank が同一なら更新、無ければ新規"""
    qry = {
        "filter": {"and":[
            {"property":"Date","date":{"equals":TODAY}},
            {"property":"Store","select":{"equals":row['store']}},
            {"property":"Category","select":{"equals":row['category']}},
            {"property":"Rank","number":{"equals":row['rank']}}
        ]}
    }
    res = requests.post(f"https://api.notion.com/v1/databases/{DB_ID}/query",
                        headers=HEADERS, json=qry).json()
    payload = {"properties":{
        "Date":     {"date":{"start":TODAY}},
        "Store":    {"select":{"name":row['store']}},
        "Category": {"select":{"name":row['category']}},
        "Rank":     {"number":row['rank']},
        "Title":    {"title":[{"text":{"content":row['title']}}]},
        "URL":      {"url":row['url']}
    }}
    if res.get("results"):
        pid = res["results"][0]["id"]
        requests.patch(f"https://api.notion.com/v1/pages/{pid}",
                       headers=HEADERS, json=payload)
    else:
        payload["parent"] = {"database_id": DB_ID}
        requests.post("https://api.notion.com/v1/pages",
                      headers=HEADERS, json=payload)

# ---------- Scrapers ------------------------------------------------------
UA = {'User-Agent':'Mozilla/5.0'}

def fetch_amazon(limit=20):
    url = "https://www.amazon.co.jp/gp/bestsellers/books/2278488051"
    soup = BeautifulSoup(requests.get(url, headers=UA).text, "html.parser")
    for li in soup.select("#zg-ordered-list li")[:limit]:
        rank  = int(li.select_one(".zg-badge-text").text.strip("#"))
        title = li.select_one(".p13n-sc-truncated").get_text(strip=True)
        href  = "https://www.amazon.co.jp" + li.select_one("a.a-link-normal")["href"]
        yield {"store":"Amazon", "category":"コミック売れ筋", "rank":rank,
               "title":title, "url":href}

def fetch_cmoa(cat_name, url, limit=20):
    soup = BeautifulSoup(requests.get(url, headers=UA).text, "html.parser")
    for i, a in enumerate(soup.select("a.rank_link")[:limit], 1):
        title = a.get_text(strip=True)
        href  = "https://www.cmoa.jp" + a["href"]
        yield {"store":"Cmoa", "category":cat_name,
               "rank":i, "title":title, "url":href}

CATEGORIES = [
    ("総合",  "https://www.cmoa.jp/search/purpose/ranking/all/"),
    ("少年マンガ", "https://www.cmoa.jp/search/purpose/ranking/boy/"),
    ("青年マンガ", "https://www.cmoa.jp/search/purpose/ranking/gentle/"),
    ("ライトアダルト", "https://www.cmoa.jp/search/purpose/ranking/sexy/"),
]

if __name__ == "__main__":
    # Amazon
    for row in fetch_amazon():
        upsert(row); time.sleep(0.4)

    # Cmoa
    for cat, url in CATEGORIES:
        for row in fetch_cmoa(cat, url):
            upsert(row); time.sleep(0.4)
