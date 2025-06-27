import os, time, datetime as dt, requests, feedparser
from bs4 import BeautifulSoup
from pprint import pformat

#───────────────────── ① 環境変数（GitHub Secrets で渡す） ─────────────────────#
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DB_ID        = os.environ["NOTION_DB"]

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}
UA = {'User-Agent': 'Mozilla/5.0'}
TODAY = dt.date.today().isoformat()

#───────────────────── ② Notion ユーティリティ ─────────────────────────#
def _query_page(store, category, rank):
    """Date & Store & Category & Rank が一致するページを検索"""
    payload = {
        "filter": {
            "and": [
                {"property":"Date","date":{"equals":TODAY}},
                {"property":"Store","select":{"equals":store}},
                {"property":"Category","select":{"equals":category}},
                {"property":"Rank","number":{"equals":rank}},
            ]
        }
    }
    res = requests.post(f"https://api.notion.com/v1/databases/{DB_ID}/query",
                        headers=HEADERS, json=payload)
    print("query-status:", res.status_code, res.text[:120])
    res.raise_for_status()
    return res.json().get("results", [])

def _upsert(row):
    """存在すれば PATCH、無ければ POST"""
    base = {
        "properties":{
            "Date":{"date":{"start":TODAY}},
            "Store":{"select":{"name":row['store']}},
            "Category":{"select":{"name":row['category']}},
            "Rank":{"number":row['rank']},
            "Title":{"title":[{"text":{"content":row['title']}}]},
            "URL":{"url":row['url']}
        }
    }

    hit = _query_page(row['store'], row['category'], row['rank'])
    if hit:
        pid   = hit[0]['id']
        resp  = requests.patch(f"https://api.notion.com/v1/pages/{pid}",
                               headers=HEADERS, json=base)
        action = "PATCH"
    else:
        base["parent"] = {"database_id":DB_ID}
        resp  = requests.post("https://api.notion.com/v1/pages",
                              headers=HEADERS, json=base)
        action = "POST"

    print(f"{action} upsert-status:", resp.status_code, resp.text[:120])
    resp.raise_for_status()

#───────────────────── ③ Amazon：公式 RSS で安全取得 ───────────────────────#
def fetch_amazon(limit=20):
    feed = feedparser.parse("https://www.amazon.co.jp/gp/rss/bestsellers/books/2278488051")  # Amazon公式 RSS:contentReference[oaicite:1]{index=1}
    for rank, e in enumerate(feed.entries[:limit], 1):
        yield {"store":"Amazon","category":"コミック売れ筋",
               "rank":rank,"title":e.title,"url":e.link}

#───────────────────── ④ コミックシーモア：実 DOM に合わせたセレクタ ───────────#
def fetch_cmoa(cat_name, url, limit=20):
    html = requests.get(url, headers=UA).text           # ランキングページ HTML:contentReference[oaicite:2]{index=2}
    soup = BeautifulSoup(html, "html.parser")
    # 「#総合ランキング」等の見出し直下に a[href*='/title/'] が並ぶ構造
    selector = "div.book_ranking a[href*='/title/']"
    for i, a in enumerate(soup.select(selector)[:limit], 1):
        yield {"store":"Cmoa","category":cat_name,
               "rank":i,"title":a.get_text(strip=True),"url":"https://www.cmoa.jp"+a["href"]}

CATEGORIES = [
    ("総合",        "https://www.cmoa.jp/search/purpose/ranking/all/"),     # 総合ランキング:contentReference[oaicite:3]{index=3}
    ("少年マンガ",  "https://www.cmoa.jp/search/purpose/ranking/boy/"),    # 少年マンガ:contentReference[oaicite:4]{index=4}
    ("青年マンガ",  "https://www.cmoa.jp/search/purpose/ranking/gentle/"), # 青年マンガ:contentReference[oaicite:5]{index=5}
    ("ライトアダルト","https://www.cmoa.jp/search/purpose/ranking/sexy/") # ライトアダルト:contentReference[oaicite:6]{index=6}
]

#───────────────────── ⑤ メイン処理 ─────────────────────────────────#
if __name__ == "__main__":
    print("=== START:", dt.datetime.now(), "===")

    # Amazon
    for row in fetch_amazon():
        print("▶", pformat(row))
        _upsert(row)
        time.sleep(0.5)   # Notion 3 req/s 制限対策

    # Cmoa ×4 カテゴリ
    for cat, url in CATEGORIES:
        for row in fetch_cmoa(cat, url):
            print("▶", pformat(row))
            _upsert(row)
            time.sleep(0.5)

    print("=== DONE:", dt.datetime.now(), "===")
