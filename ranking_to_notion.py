#!/usr/bin/env python3
"""
Amazon（via ダ・ヴィンチWeb）＋コミックシーモア 4カテゴリ
=> Notion DB へ upsert。GitHub Actions 07:00 JST 専用
"""

import os, time, datetime as dt, requests, feedparser
from bs4 import BeautifulSoup
from pprint import pformat

# ── 環境変数（GitHub Secrets で渡す） ───────────────────────────
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DB_ID        = os.environ["NOTION_DB"]

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}
UA = {'User-Agent': 'Mozilla/5.0'}
TODAY = dt.date.today().isoformat()

# ── Notion ユーティリティ ──────────────────────────────────
def _query(store, category, rank):
    q = {"filter":{"and":[
        {"property":"Date","date":{"equals":TODAY}},
        {"property":"Store","select":{"equals":store}},
        {"property":"Category","select":{"equals":category}},
        {"property":"Rank","number":{"equals":rank}}]}}
    r = requests.post(f"https://api.notion.com/v1/databases/{DB_ID}/query",
                      headers=HEADERS, json=q)
    print("query", r.status_code)
    r.raise_for_status()
    return r.json().get("results", [])

def _upsert(row):
    props = {
        "Date":     {"date":{"start":TODAY}},
        "Store":    {"select":{"name":row['store']}},
        "Category": {"select":{"name":row['category']}},
        "Rank":     {"number":row['rank']},
        "Title":    {"title":[{"text":{"content":row['title']}}]},
        "URL":      {"url":row['url']}
    }
    hit = _query(row['store'], row['category'], row['rank'])
    if hit:
        pid = hit[0]['id']
        resp = requests.patch(f"https://api.notion.com/v1/pages/{pid}",
                              headers=HEADERS, json={"properties":props})
        act = "PATCH"
    else:
        resp = requests.post("https://api.notion.com/v1/pages",
                             headers=HEADERS,
                             json={"parent":{"database_id":DB_ID},
                                   "properties":props})
        act = "POST"
    print(act, resp.status_code)
    resp.raise_for_status()

# ── 1) Amazon（ダ・ヴィンチWeb 経由）───────────────────────
def fetch_amazon(limit=20):
    url = "https://ddnavi.com/ebook-ranking/amazon/"   # 公式 API 鏡サイト:contentReference[oaicite:2]{index=2}
    soup = BeautifulSoup(requests.get(url, headers=UA).text, "html.parser")
    for i, li in enumerate(soup.select(".book-list li")[:limit], 1):
        title = li.select_one(".book-title").get_text(strip=True)
        href  = li.select_one("a")["href"]
        yield {"store":"Amazon", "category":"コミック売れ筋",
               "rank":i, "title":title, "url":href}

# ── 2) コミックシーモア ──────────────────────────────────
def fetch_cmoa(cat_name, url, limit=20):
    html  = requests.get(url, headers=UA).text
    soup  = BeautifulSoup(html, "html.parser")
    for i, a in enumerate(soup.select("div.book_ranking a[href*='/title/']")[:limit], 1):
        yield {"store":"Cmoa","category":cat_name,
               "rank":i,"title":a.get_text(strip=True),"url":"https://www.cmoa.jp"+a["href"]}

CATEGORIES = [
    ("総合",         "https://www.cmoa.jp/search/purpose/ranking/all/"),
    ("少年マンガ",   "https://www.cmoa.jp/search/purpose/ranking/boy/"),
    ("青年マンガ",   "https://www.cmoa.jp/search/purpose/ranking/gentle/"),
    ("ライトアダルト","https://www.cmoa.jp/search/purpose/ranking/sexy/"),
]

# ── メイン ─────────────────────────────────────────────
if __name__ == "__main__":
    print("=== START:", dt.datetime.now(), "===")

    # Amazon
    rows = list(fetch_amazon())
    print("AMAZON rows =", len(rows))
    for r in rows:
        _upsert(r); time.sleep(0.5)

    # Cmoa
    for cat, url in CATEGORIES:
        rows = list(fetch_cmoa(cat, url))
        print(f"{cat} rows =", len(rows))
        for r in rows:
            _upsert(r); time.sleep(0.5)

    print("=== DONE:", dt.datetime.now(), "===")
