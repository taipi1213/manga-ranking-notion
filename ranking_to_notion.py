#!/usr/bin/env python3
"""
Amazon（コミック売れ筋 TOP20）＋ コミックシーモア
 ├─ 総合／少年マンガ／青年マンガ／ライトアダルト   … 各 TOP20
を毎日 Notion に upsert する完全版。

● Amazon 2025-06 時点の新 DOM に対応
   div.zg-grid-general-faceout > img[alt] からタイトルを取得
● シーモアは ul#ranking_result_list li.search_result_box を走査
   a.title と <img alt> に必ず作品名が入る
"""

import os, time, datetime as dt, requests
from bs4 import BeautifulSoup
from pprint import pformat

#───────── GitHub Secrets → 環境変数 ─────────#
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DB_ID        = os.environ["NOTION_DB"]      # データベース ID

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    # CloudFront の地域判定を回避
    "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8",
}
TODAY = dt.date.today().isoformat()

#───────── Notion utility ─────────#
def _query(store, category, rank):
    payload = {
        "filter": {
            "and": [
                {"property": "Date",     "date":   {"equals": TODAY}},
                {"property": "Store",    "select": {"equals": store}},
                {"property": "Category", "select": {"equals": category}},
                {"property": "Rank",     "number": {"equals": rank}},
            ]
        }
    }
    r = requests.post(
        f"https://api.notion.com/v1/databases/{DB_ID}/query",
        headers=HEADERS, json=payload, timeout=10
    )
    r.raise_for_status()
    return r.json().get("results", [])

def _upsert(row):
    props = {
        "Date":     {"date": {"start": TODAY}},
        "Store":    {"select": {"name": row["store"]}},
        "Category": {"select": {"name": row["category"]}},
        "Rank":     {"number": row["rank"]},
        "Title":    {"title": [{"text": {"content": row["title"]}}]},
        "URL":      {"url":   row["url"]},
    }
    hit = _query(row["store"], row["category"], row["rank"])
    if hit:
        page_id = hit[0]["id"]
        resp = requests.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            headers=HEADERS, json={"properties": props}, timeout=10
        )
    else:
        resp = requests.post(
            "https://api.notion.com/v1/pages",
            headers=HEADERS,
            json={"parent": {"database_id": DB_ID}, "properties": props},
            timeout=10
        )
    resp.raise_for_status()

#───────── 1) Amazon ─────────#
def fetch_amazon(limit=20):
    url = "https://www.amazon.co.jp/gp/bestsellers/books/2278488051"
    html = requests.get(url, headers=UA, timeout=15).text
    soup = BeautifulSoup(html, "html.parser")
    # 各作品ブロック
    for rank, face in enumerate(soup.select("div.zg-grid-general-faceout")[:limit], 1):
        a   = face.find_parent("a", class_="a-link-normal")
        href = "https://www.amazon.co.jp" + a["href"] if a else url
        img  = face.select_one("img[alt]")
        title = img["alt"].strip() if img else a.get_text(strip=True)
        yield {
            "store": "Amazon",
            "category": "コミック売れ筋",
            "rank": rank,
            "title": title,
            "url": href,
        }

#───────── 2) コミックシーモア ─────────#
def fetch_cmoa(cat_name, url, limit=20):
    html = requests.get(url, headers=UA, timeout=15).text
    soup = BeautifulSoup(html, "html.parser")
    for i, li in enumerate(
        soup.select("ul#ranking_result_list li.search_result_box")[:limit], 1
    ):
        a = li.select_one("a.title")
        img = li.select_one("img[alt]")
        title = (img["alt"] if img else a.get_text(strip=True)).strip()
        href  = "https://www.cmoa.jp" + a["href"]
        yield {
            "store": "Cmoa",
            "category": cat_name,
            "rank": i,
            "title": title,
            "url": href,
        }

CATEGORIES = [
    ("総合",          "https://www.cmoa.jp/search/purpose/ranking/all/"),
    ("少年マンガ",    "https://www.cmoa.jp/search/purpose/ranking/boy/"),
    ("青年マンガ",    "https://www.cmoa.jp/search/purpose/ranking/gentle/"),
    ("ライトアダルト", "https://www.cmoa.jp/search/purpose/ranking/sexy/"),
]

#───────── main ─────────#
if __name__ == "__main__":
    print("=== START:", dt.datetime.now(), "===")

    # Amazon
    for row in fetch_amazon():
        print("AMZ▶", pformat(row))
        _upsert(row)
        time.sleep(0.5)   # Notion 3 rps 制限

    # Cmoa ×4
    for cat, url in CATEGORIES:
        for row in fetch_cmoa(cat, url):
            print(f"{cat}▶", pformat(row))
            _upsert(row)
            time.sleep(0.5)

    print("=== DONE :", dt.datetime.now(), "===")
