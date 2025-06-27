import os
import time
import datetime as dt
import requests
from bs4 import BeautifulSoup
from pprint import pformat   # デバッグ用

# ── 環境変数 ────────────────────────────────────────────────────
NOTION_TOKEN = os.environ["NOTION_TOKEN"]     # 内部統合トークン
DB_ID        = os.environ["NOTION_DB"]        # 32桁 Database ID

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

UA = {'User-Agent': 'Mozilla/5.0'}

TODAY = dt.date.today().isoformat()           # 例: 2025-06-28

# ── Notion ヘルパ ─────────────────────────────────────────────
def notion_query(store, category, rank):
    """既存ページ有無チェック"""
    query = {
        "filter": {
            "and": [
                {"property": "Date",     "date":   {"equals": TODAY}},
                {"property": "Store",    "select": {"equals": store}},
                {"property": "Category", "select": {"equals": category}},
                {"property": "Rank",     "number": {"equals": rank}}
            ]
        }
    }
    res = requests.post(
        f"https://api.notion.com/v1/databases/{DB_ID}/query",
        headers=HEADERS, json=query)
    print("query-status:", res.status_code, res.text[:200])
    res.raise_for_status()
    return res.json().get("results", [])

def notion_upsert(row):
    """行データを upsert（存在すれば PATCH、無ければ POST）"""
    payload = {
        "properties": {
            "Date":     {"date":   {"start": TODAY}},
            "Store":    {"select": {"name": row["store"]}},
            "Category": {"select": {"name": row["category"]}},
            "Rank":     {"number": row["rank"]},
            "Title":    {"title":  [{"text": {"content": row["title"]}}]},
            "URL":      {"url": row["url"]}
        }
    }

    existing = notion_query(row["store"], row["category"], row["rank"])
    if existing:
        page_id = existing[0]["id"]
        resp = requests.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            headers=HEADERS, json=payload)
        action = "PATCH"
    else:
        payload["parent"] = {"database_id": DB_ID}
        resp = requests.post(
            "https://api.notion.com/v1/pages",
            headers=HEADERS, json=payload)
        action = "POST"

    print(f"{action} upsert-status:", resp.status_code, resp.text[:200])
    resp.raise_for_status()

# ── スクレイパ ────────────────────────────────────────────────
def fetch_amazon(limit=20):
    url = "https://www.amazon.co.jp/gp/bestsellers/books/2278488051"
    soup = BeautifulSoup(requests.get(url, headers=UA).text, "html.parser")
    for li in soup.select("#zg-ordered-list li")[:limit]:
        rank  = int(li.select_one(".zg-badge-text").text.strip("#"))
        title = li.select_one(".p13n-sc-truncated").get_text(strip=True)
        href  = "https://www.amazon.co.jp" + li.select_one("a.a-link-normal")["href"]
        yield {"store": "Amazon", "category": "コミック売れ筋",
               "rank": rank, "title": title, "url": href}

def fetch_cmoa(cat_name, url, limit=20):
    soup = BeautifulSoup(requests.get(url, headers=UA).text, "html.parser")
    for i, a in enumerate(soup.select("a.rank_link")[:limit], start=1):
        title = a.get_text(strip=True)
        href  = "https://www.cmoa.jp" + a["href"]
        yield {"store": "Cmoa", "category": cat_name,
               "rank": i, "title": title, "url": href}

CATEGORIES = [
    ("総合",         "https://www.cmoa.jp/search/purpose/ranking/all/"),
    ("少年マンガ",   "https://www.cmoa.jp/search/purpose/ranking/boy/"),
    ("青年マンガ",   "https://www.cmoa.jp/search/purpose/ranking/gentle/"),
    ("ライトアダルト", "https://www.cmoa.jp/search/purpose/ranking/sexy/"),
]

# ── メイン ───────────────────────────────────────────────────
if __name__ == "__main__":
    print("=== START:", dt.datetime.now(), "===")

    # Amazon
    for row in fetch_amazon():
        print("▶", pformat(row))
        notion_upsert(row)
        time.sleep(0.5)   # レート制限対策

    # Cmoa
    for cat, url in CATEGORIES:
        for row in fetch_cmoa(cat, url):
            print("▶", pformat(row))
            notion_upsert(row)
            time.sleep(0.5)

    print("=== DONE:", dt.datetime.now(), "===")
