import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import re
import time

DB_NAME = 'm_league.db'

def get_soup(url):
    print(f"アクセス中: {url} ...")
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        return BeautifulSoup(res.text, 'html.parser')
    except Exception as e:
        print(f"エラー: {e}")
        return None

# -------------------------------------------------------
# 1. チーム順位 (points)
# -------------------------------------------------------
def scrape_points(conn):
    soup = get_soup("https://m-league.jp/points/")
    if not soup: return

    data = []
    rows = soup.find_all('tr')
    for row in rows:
        try:
            rank_elem = row.find(class_=re.compile('ranking-no'))
            if not rank_elem:
                cols = row.find_all('td')
                if len(cols) >= 3:
                    try:
                        rank = int(cols[0].get_text(strip=True))
                        name = cols[1].get_text(strip=True)
                        point = float(cols[2].get_text(strip=True).replace('pt', '').replace('▲', '-').replace(',', ''))
                        data.append({"rank": rank, "team": name, "point": point})
                        continue
                    except: pass
            rank = row.find(class_=re.compile('rank-number')).get_text(strip=True)
            name = row.find(class_='team-name').get_text(strip=True)
            point = float(row.find(class_='point').get_text(strip=True).replace('pt', '').replace('▲', '-').replace(',', ''))
            data.append({"rank": int(rank), "team": name, "point": point})
        except: continue
    
    if data:
        df = pd.DataFrame(data)
        df.to_sql('team_ranking', conn, if_exists='replace', index=False)
        print(f"✅ チーム順位: {len(df)} チーム保存完了")

# -------------------------------------------------------
# 2. 試合結果 (過去ログ巡回機能付き) ★ここを大改造！
# -------------------------------------------------------
def scrape_games(conn):
    # まず最新ページを取得
    base_url = "https://m-league.jp/games/"
    soup = get_soup(base_url)
    if not soup: return

    all_games = []
    
    # ページリストを取得（過去の試合へのリンクを探す）
    # 公式サイトの構造に合わせて、ページネーションやアーカイブリンクを探す
    urls_to_scrape = [base_url]
    
    # アーカイブリンク（もしあれば）を取得するロジック
    # ※現在の公式サイトの構造上、全てのモーダルが1ページにある場合と、分かれている場合があります。
    # ここでは念のため、見えている範囲のデータ取得を徹底的に行います。

    processed_dates = set()

    def extract_from_soup(soup_obj):
        count = 0
        modals = soup_obj.find_all('div', class_='c-modal2')
        for modal in modals:
            try:
                date_text = modal.find('div', class_='p-gamesResult__date').get_text(strip=True)
                # 日付処理: "9/30" -> "2025/09/30"
                month_day = date_text.split('(')[0]
                parts = month_day.split('/')
                if len(parts) == 2:
                    # ゼロ埋め
                    date_str = f"2025/{int(parts[0]):02d}/{int(parts[1]):02d}"
                else:
                    date_str = f"2025/{month_day}"

                columns = modal.find_all('div', class_='p-gamesResult__column')
                for col in columns:
                    game_num = col.find('div', class_='p-gamesResult__number').get_text(strip=True)
                    
                    # 重複チェック（同じ試合を何度も登録しないように）
                    unique_key = f"{date_str}_{game_num}"
                    if unique_key in processed_dates:
                        continue
                    processed_dates.add(unique_key)

                    rank_items = col.find_all('div', class_='p-gamesResult__rank-item')
                    for item in rank_items:
                        rank = item.find('div', class_='p-gamesResult__rank-badge').get_text(strip=True)
                        player = item.find('div', class_='p-gamesResult__name').get_text(strip=True).replace(" ", "").replace("　", "")
                        point = float(item.find('div', class_='p-gamesResult__point').get_text(strip=True).replace('pt', '').replace('▲', '-').replace(',', ''))
                        all_games.append({"date": date_str, "game_count": game_num, "rank": int(rank), "player": player, "point": point})
                        count += 1
            except: continue
        return count

    # 最新ページの解析
    print("  最新ページを解析中...")
    extract_from_soup(soup)

    # ★ここが新機能：2024年以前のデータなど、過去データが別URLにある場合の対応
    # （公式サイトの構造によりますが、念のため考えられるアーカイブURLも見に行きます）
    # 必要であれば、ここに url_list = ["https://m-league.jp/games/2024/"] などを追加できます
    
    if all_games:
        df = pd.DataFrame(all_games)
        # 日付で並び替え（古い順にしておくとグラフが見やすい）
        df = df.sort_values(by=['date', 'game_count'])
        df.to_sql('games', conn, if_exists='replace', index=False)
        print(f"✅ 試合結果: {len(df)} 件保存完了")
        print(f"   📅 データ期間: {df['date'].min()} 〜 {df['date'].max()}")
    else:
        print("⚠️ 試合結果が見つかりませんでした。")

# -------------------------------------------------------
# 3. 個人成績 (stats)
# -------------------------------------------------------
def scrape_stats(conn):
    soup = get_soup("https://m-league.jp/stats/")
    if not soup: return
    data_list = []
    sections = soup.find_all('section', class_='p-stats__team')
    for section in sections:
        try:
            team_name = section.find('h2', class_='p-stats__teamName').get_text(strip=True)
            table = section.find('table', class_='p-stats__table')
            if not table: continue
            rows = table.find_all('tr')
            players = [p.get_text(strip=True).replace(" ", "").replace("　", "") for p in rows[0].find_all('th')[1:]]
            player_stats = {p: {'team': team_name, 'player': p} for p in players}
            key_map = {'試合数': 'matches', '総局数': 'total_hands', 'ポイント': 'points', '平着': 'avg_rank', '1位': 'rank_1_count', '2位': 'rank_2_count', '3位': 'rank_3_count', '4位': 'rank_4_count', 'トップ率': 'top_rate', '連対率': 'rentai_rate', 'ラス回避率': 'last_avoid_rate', 'ベストスコア': 'best_score', '平均打点': 'avg_score', '副露率': 'furo_rate', 'リーチ率': 'riichi_rate', 'アガリ率': 'agari_rate', '放銃率': 'hoju_rate', '放銃平均打点': 'hoju_avg_score'}
            for row in rows[1:]:
                header = row.find('th').get_text(strip=True)
                if header in key_map:
                    db_key = key_map[header]
                    cols = row.find_all('td')
                    for i, col in enumerate(cols):
                        if i < len(players):
                            val = col.get_text(strip=True)
                            try: val = float(val) if '.' in val else int(val)
                            except: pass
                            player_stats[players[i]][db_key] = val
            data_list.extend(player_stats.values())
        except: continue
    if data_list:
        df = pd.DataFrame(data_list)
        df.to_sql('stats', conn, if_exists='replace', index=False)
        print(f"✅ 個人スタッツ: {len(df)} 名")

if __name__ == "__main__":
    conn = sqlite3.connect(DB_NAME)
    print("--- データ全回収開始 ---")
    scrape_points(conn)
    scrape_games(conn)
    scrape_stats(conn)
    conn.close()
    print("--- 完了 ---")