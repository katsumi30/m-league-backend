import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import re

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
# 1. チーム順位
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
    
    if not data:
        soup_top = get_soup("https://m-league.jp/")
        if soup_top:
            teams = soup_top.find_all('div', class_='p-ranking__team-item')
            for team in teams:
                try:
                    rank = team.find(class_=re.compile('p-ranking__rank-number')).get_text(strip=True)
                    name = team.find(class_='p-ranking__team-name').get_text(strip=True)
                    point = float(team.find(class_='p-ranking__current-point').get_text(strip=True).replace('pt', '').replace('▲', '-').replace(',', ''))
                    data.append({"rank": int(rank), "team": name, "point": point})
                except: continue

    if data:
        df = pd.DataFrame(data)
        df.to_sql('team_ranking', conn, if_exists='replace', index=False)
        print(f"✅ チーム順位: {len(df)} チーム")

# -------------------------------------------------------
# 2. 試合結果 (★ここを修正！重複チェックを削除)
# -------------------------------------------------------
def scrape_games(conn):
    base_url = "https://m-league.jp/games/"
    soup = get_soup(base_url)
    if not soup: return

    all_games = []
    
    # 重複チェック用のセットを削除しました
    # 公式サイトのモーダルは全てユニークなIDを持っているため、単純に全部取得すればOKです

    modals = soup.find_all('div', class_='c-modal2')
    for modal in modals:
        try:
            date_text = modal.find('div', class_='p-gamesResult__date').get_text(strip=True)
            month_day = date_text.split('(')[0]
            parts = month_day.split('/')
            if len(parts) == 2:
                date_str = f"2025/{int(parts[0]):02d}/{int(parts[1]):02d}"
            else:
                date_str = f"2025/{month_day}"

            columns = modal.find_all('div', class_='p-gamesResult__column')
            for col in columns:
                game_num = col.find('div', class_='p-gamesResult__number').get_text(strip=True)
                
                # ★削除した部分: if unique_key in processed_dates: continue

                rank_items = col.find_all('div', class_='p-gamesResult__rank-item')
                for item in rank_items:
                    rank = item.find('div', class_='p-gamesResult__rank-badge').get_text(strip=True)
                    player = item.find('div', class_='p-gamesResult__name').get_text(strip=True).replace(" ", "").replace("　", "")
                    point = float(item.find('div', class_='p-gamesResult__point').get_text(strip=True).replace('pt', '').replace('▲', '-').replace(',', ''))
                    all_games.append({"date": date_str, "game_count": game_num, "rank": int(rank), "player": player, "point": point})
        except: continue

    if all_games:
        df = pd.DataFrame(all_games)
        # ソートして保存
        df = df.sort_values(by=['date', 'game_count'])
        df.to_sql('games', conn, if_exists='replace', index=False)
        print(f"✅ 試合結果: {len(df)} 件保存完了")
        print(f"   📅 データ期間: {df['date'].min()} 〜 {df['date'].max()}")
    else:
        print("⚠️ 試合結果が見つかりませんでした。")

# -------------------------------------------------------
# 3. 個人成績
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
    print("--- データ全回収開始（重複許容版） ---")
    scrape_points(conn)
    scrape_games(conn)
    scrape_stats(conn)
    conn.close()
    print("--- 完了 ---")