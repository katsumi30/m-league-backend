from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import pandas as pd
import openai
import re
import os

openai.api_key = os.getenv("OPENAI_API_KEY")

app = FastAPI()
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

DB_NAME = 'm_league.db'

# DB接続ヘルパー
def get_connection():
    return sqlite3.connect(DB_NAME)

# 起動時ロード（失敗してもOK、リクエスト時に再ロードする仕様に変更）
TEAM_VOCAB = ""
PLAYER_VOCAB = ""

class ChatRequest(BaseModel):
    message: str

# ==========================================
# ★ 追加機能: サーバー診断ページ ★
# ==========================================
@app.get("/debug")
def debug_endpoint():
    """サーバーの中身を覗き見するページ"""
    try:
        conn = get_connection()
        
        # 1. ファイルがあるか？
        if not os.path.exists(DB_NAME):
            return {"status": "CRITICAL ERROR", "message": "データベースファイル(m_league.db)がサーバーにありません！"}

        # 2. statsテーブル（個人成績）チェック
        try:
            df_stats = pd.read_sql_query("SELECT * FROM stats", conn)
            stats_count = len(df_stats)
            sample_players = df_stats['player'].head(5).tolist() if not df_stats.empty else []
            # 伊達プロチェック
            date_check = df_stats[df_stats['player'].str.contains('伊達')]
            date_exists = "いる！" if not date_check.empty else "いない..."
        except Exception as e:
            return {"status": "ERROR", "message": f"statsテーブル読み込み失敗: {e}"}

        # 3. gamesテーブル（試合結果）チェック
        try:
            df_games = pd.read_sql_query("SELECT * FROM games", conn)
            games_count = len(df_games)
            latest_date = df_games['date'].max() if not df_games.empty else "なし"
        except Exception as e:
            return {"status": "ERROR", "message": f"gamesテーブル読み込み失敗: {e}"}

        conn.close()

        return {
            "status": "OK",
            "stats_count": f"{stats_count} 件 (個人成績)",
            "sample_players": sample_players,
            "date_san_check": f"伊達プロは... {date_exists}",
            "games_count": f"{games_count} 件 (試合結果)",
            "latest_game_date": f"最新の日付: {latest_date}"
        }

    except Exception as e:
        return {"status": "SYSTEM ERROR", "error": str(e)}

# ==========================================
# チャット機能
# ==========================================
@app.post("/chat")
async def chat_endpoint(req: ChatRequest):
    try:
        # リクエストのたびに最新の辞書を読み込む（サーバー再起動なしでも反映されるように）
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT team FROM stats")
            teams = [r[0] for r in cur.fetchall() if r[0]]
            cur.execute("SELECT DISTINCT player FROM stats")
            players = [r[0] for r in cur.fetchall() if r[0]]
            global TEAM_VOCAB, PLAYER_VOCAB
            TEAM_VOCAB = ", ".join(teams)
            PLAYER_VOCAB = ", ".join(players)
        except:
            pass
        finally:
            conn.close()

        if not openai.api_key:
            return {"reply": "【エラー】APIキーが設定されていません。", "graph": None}

        user_query = req.message
        graph_data = None
        
        # 1. グラフモード
        if "推移" in user_query or "グラフ" in user_query:
            id_prompt = f"""
            ユーザーは「ポイント推移」を知りたいです。質問: "{user_query}"
            【正しい名前】チーム: {TEAM_VOCAB} 選手: {PLAYER_VOCAB}
            【指示】質問対象を特定し、LIKE検索のSQLを作成してください。
            パターンA(チーム): SELECT date, point, player FROM games WHERE player IN (SELECT player FROM stats WHERE team LIKE '%キーワード%') ORDER BY date;
            パターンB(個人): SELECT date, point, player FROM games WHERE player LIKE '%キーワード%' ORDER BY date;
            回答はSQLのみ。
            """
            res = openai.chat.completions.create(
                model="gpt-4o", messages=[{"role": "system", "content": id_prompt}], temperature=0
            )
            sql = res.choices[0].message.content.strip().replace("```sql", "").replace("```", "")
            
            conn = get_connection()
            try:
                df = pd.read_sql_query(sql, conn)
                if not df.empty:
                    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y/%m/%d')
                    df_grouped = df.groupby('date')['point'].sum().reset_index()
                    df_grouped['total_point'] = df_grouped['point'].cumsum()
                    
                    label_name = "推移"
                    if "team" in sql.lower():
                        label_name = "チーム推移"
                    elif not df.empty:
                        label_name = f"{df['player'].iloc[0]}の推移"

                    graph_data = {
                        "labels": df_grouped['date'].tolist(),
                        "data": df_grouped['total_point'].tolist(),
                        "label": label_name
                    }
                    final_prompt = f"""
                    Mリーグ実況者として解説してください。
                    質問: {user_query}
                    データ(直近): {df_grouped.tail(5).to_string()}
                    「グラフをご覧ください」と添えてください。
                    """
                    res_text = openai.chat.completions.create(
                        model="gpt-4o", messages=[{"role": "system", "content": final_prompt}], temperature=0.3
                    )
                    return {"reply": res_text.choices[0].message.content, "graph": graph_data}
            except:
                pass
            finally:
                conn.close()

        # 2. 最新結果モード
        elif "最新" in user_query or "試合結果" in user_query:
            conn = get_connection()
            try:
                sql = "SELECT date, game_count, rank, player, point FROM games ORDER BY date DESC, game_count DESC, rank ASC LIMIT 8"
                df = pd.read_sql_query(sql, conn)
                sql_rk = "SELECT rank, team, point FROM team_ranking ORDER BY rank"
                df_rk = pd.read_sql_query(sql_rk, conn)
                combined = f"【直近試合】\n{df.to_string()}\n【チーム順位】\n{df_rk.to_string()}"
                final_prompt = f"""
                Mリーグ公式リポーターとして報告してください。
                データ: {combined}
                ルール:
                - 日付ごとに第1/第2試合を分ける
                - 順位は絵文字(🥇🥈🥉4️⃣)付き
                - チーム順位も記載
                - 選手名・チーム名は太字(**)
                - マイナスは▲表記
                """
                res = openai.chat.completions.create(
                    model="gpt-4o", messages=[{"role": "system", "content": final_prompt}], temperature=0.3
                )
                return {"reply": res.choices[0].message.content, "graph": None}
            finally:
                conn.close()

        # 3. 通常モード
        sql_prompt = f"""
        あなたはMリーグのデータエンジニアです。
        質問「{user_query}」に対し、適切なSQLを作成してください。
        【正しい名前】選手: {PLAYER_VOCAB} チーム: {TEAM_VOCAB}
        【指示】ユーザー入力を上記リストの名前に変換し、LIKE検索してください。
        
        テーブル:
        1. stats (通算): player, team, points, riichi_rate, agari_rate, hoju_rate ...
        2. games (日別): date, rank, player, point
        3. team_ranking (順位): rank, team, point
        
        回答はSQLのみ。
        """
        res_sql = openai.chat.completions.create(
            model="gpt-4o", messages=[{"role": "system", "content": sql_prompt}], temperature=0
        )
        gen_sql = res_sql.choices[0].message.content.strip().replace("```sql", "").replace("```", "")
        
        conn = get_connection()
        try:
            df_result = pd.read_sql_query(gen_sql, conn)
        except:
            df_result = pd.DataFrame()
        finally:
            conn.close()

        final_prompt = f"""
        Mリーグ解説者として質問に答えてください。
        質問: {user_query}
        データ: {df_result.to_string()}
        データがない場合は「該当データが見当たりませんでした」と回答。
        """
        res_final = openai.chat.completions.create(
            model="gpt-4o", messages=[{"role": "system", "content": final_prompt}], temperature=0.3
        )
        return {"reply": res_final.choices[0].message.content, "graph": None}

    except Exception as e:
        return {"reply": f"エラー: {str(e)}", "graph": None}