from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import pandas as pd
import openai
import os

openai.api_key = os.getenv("OPENAI_API_KEY")

app = FastAPI()
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

DB_NAME = 'm_league.db'

def get_connection():
    return sqlite3.connect(DB_NAME)

# 毎回DBから最新のリストを取得する関数
def get_vocab():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT team FROM stats")
        teams = [r[0] for r in cur.fetchall() if r[0]]
        cur.execute("SELECT DISTINCT player FROM stats")
        players = [r[0] for r in cur.fetchall() if r[0]]
        conn.close()
        return ", ".join(teams), ", ".join(players)
    except:
        return "", ""

@app.get("/debug")
def debug_endpoint():
    try:
        if not os.path.exists(DB_NAME): return {"status": "ERROR", "msg": "DBなし"}
        conn = get_connection()
        df = pd.read_sql_query("SELECT * FROM stats", conn)
        conn.close()
        return {
            "status": "OK",
            "stats_rows": len(df),
            "sample_names": df['player'].head(5).tolist()
        }
    except Exception as e:
        return {"status": "ERROR", "msg": str(e)}

class ChatRequest(BaseModel):
    message: str

@app.post("/chat")
async def chat_endpoint(req: ChatRequest):
    try:
        if not openai.api_key:
            return {"reply": "APIキー設定エラー", "graph": None}

        user_query = req.message
        
        # ★ここで毎回最新の名前リストを取得してAIに渡す
        team_vocab, player_vocab = get_vocab()

        # ---------------------------------------------------------
        # 1. グラフモード
        # ---------------------------------------------------------
        if "推移" in user_query or "グラフ" in user_query:
            id_prompt = f"""
            ユーザーは「ポイント推移」を知りたいです。質問: "{user_query}"
            
            【データベース内の正式名称】
            選手: {player_vocab}
            チーム: {team_vocab}
            
            【重要】
            DB内の名前には「スペース」がありません。
            SQLを作る際は、必ず LIKE '%キーワード%' を使い、スペースを入れないでください。
            例: WHERE player LIKE '%伊達%' (〇)
            例: WHERE player = '伊達 朱里紗' (× スペース禁止)
            
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
                    else:
                        label_name = f"{df['player'].iloc[0]}の推移"

                    graph_data = {
                        "labels": df_grouped['date'].tolist(),
                        "data": df_grouped['total_point'].tolist(),
                        "label": label_name
                    }
                    final_prompt = f"""
                    Mリーグ実況者として解説。質問: {user_query}
                    データ(直近): {df_grouped.tail(5).to_string()}
                    「グラフをご覧ください」と添える。
                    """
                    res_text = openai.chat.completions.create(
                        model="gpt-4o", messages=[{"role": "system", "content": final_prompt}], temperature=0.3
                    )
                    return {"reply": res_text.choices[0].message.content, "graph": graph_data}
                else:
                    # 失敗したSQLを返してデバッグする
                    return {"reply": f"データが見つかりませんでした。\n試行したSQL: `{sql}`\n名前が合っているか確認してください。", "graph": None}
            finally:
                conn.close()

        # ---------------------------------------------------------
        # 2. 最新結果・順位モード
        # ---------------------------------------------------------
        elif "順位" in user_query or "ランキング" in user_query or "最新" in user_query or "試合結果" in user_query:
            conn = get_connection()
            try:
                sql_games = "SELECT date, game_count, rank, player, point FROM games ORDER BY date DESC, game_count DESC, rank ASC LIMIT 8"
                df_games = pd.read_sql_query(sql_games, conn)
                sql_ranking = "SELECT rank, team, point FROM team_ranking ORDER BY rank"
                df_ranking = pd.read_sql_query(sql_ranking, conn)
                combined_data = f"【直近の試合結果】\n{df_games.to_string()}\n\n【現在のチーム順位】\n{df_ranking.to_string()}"
                
                final_prompt = f"""
                あなたはMリーグ公式リポーターです。質問: {user_query}
                データ: {combined_data}
                ルール:
                - ハイフン禁止。
                - 順位は絵文字(🥇🥈🥉4️⃣🏆)付き。
                - チーム順位は「1位: **チーム名** (500.0pt)」形式。
                """
                res_final = openai.chat.completions.create(
                    model="gpt-4o", messages=[{"role": "system", "content": final_prompt}], temperature=0.3
                )
                return {"reply": res_final.choices[0].message.content, "graph": None}
            finally:
                conn.close()

        # ---------------------------------------------------------
        # 3. 通常モード
        # ---------------------------------------------------------
        sql_prompt = f"""
        あなたはMリーグのデータエンジニアです。
        質問「{user_query}」に対し、適切なSQLを作成してください。
        
        【DB内の正式名称】
        選手: {player_vocab}
        チーム: {team_vocab}
        
        【重要】
        - DB内の名前に「スペース」は含まれません（例: '伊達朱里紗'）。
        - 検索時は必ず LIKE '%キーワード%' を使ってください。
        - '伊達 朱里紗' のようなスペース入りは禁止です。
        
        テーブル:
        1. stats (通算): player, team, points, matches...
        2. games (日別): date, rank, player, point
        3. team_ranking (順位): rank, team, point
        
        回答はSQLのみ。
        """
        res_sql = openai.chat.completions.create(
            model="gpt-4o", messages=[{"role": "system", "content": sql_prompt}], temperature=0
        )
        sql = res_sql.choices[0].message.content.strip().replace("```sql", "").replace("```", "")
        
        conn = get_connection()
        try:
            df_result = pd.read_sql_query(sql, conn)
        except:
            df_result = pd.DataFrame()
        finally:
            conn.close()

        if df_result.empty:
             return {"reply": f"該当データが見当たりませんでした。\n(実行SQL: `{sql}`)", "graph": None}

        final_prompt = f"""
        Mリーグ解説者として質問に答えてください。
        質問: {user_query}
        データ: {df_result.to_string()}
        表示ルール:
        - ハイフン禁止。
        - 「項目: 値」の形式。
        """
        res_final = openai.chat.completions.create(
            model="gpt-4o", messages=[{"role": "system", "content": final_prompt}], temperature=0.3
        )
        return {"reply": res_final.choices[0].message.content, "graph": None}

    except Exception as e:
        return {"reply": f"システムエラー: {str(e)}", "graph": None}