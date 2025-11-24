from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import pandas as pd
import openai
import re
import os

# ==========================================
# ★ APIキー設定 ★
# ==========================================
openai.api_key = os.getenv("OPENAI_API_KEY")
app = FastAPI()
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

DB_NAME = 'm_league.db'

# 辞書読み込み
def get_db_vocabulary():
    try:
        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT team FROM stats")
        teams = [r[0] for r in cur.fetchall() if r[0]]
        cur.execute("SELECT DISTINCT player FROM stats")
        players = [r[0] for r in cur.fetchall() if r[0]]
        conn.close()
        return ", ".join(teams), ", ".join(players)
    except:
        return "", ""

TEAM_VOCAB, PLAYER_VOCAB = get_db_vocabulary()

class ChatRequest(BaseModel):
    message: str

@app.post("/chat")
async def chat_endpoint(req: ChatRequest):
    try:
        user_query = req.message
        graph_data = None
        
        # =========================================================
        # 1. グラフ生成モード
        # =========================================================
        if "推移" in user_query or "グラフ" in user_query:
            # ... (グラフ機能は変更なし) ...
            id_prompt = f"""
            ユーザーは「ポイント推移」を知りたがっています。
            質問: "{user_query}"
            【DB内の正しい名称】チーム: {TEAM_VOCAB} 選手: {PLAYER_VOCAB}
            【指示】質問対象を特定し、LIKE検索を使ったSQLを作成してください。
            パターンA（チーム）: SELECT date, point, player FROM games WHERE player IN (SELECT player FROM stats WHERE team LIKE '%キーワード%') ORDER BY date;
            パターンB（個人）: SELECT date, point, player FROM games WHERE player LIKE '%キーワード%' ORDER BY date;
            回答はSQLのみ出力。
            """
            res = openai.chat.completions.create(
                model="gpt-4o", messages=[{"role": "system", "content": id_prompt}], temperature=0
            )
            sql = res.choices[0].message.content.strip().replace("```sql", "").replace("```", "")
            
            conn = sqlite3.connect(DB_NAME)
            try:
                df = pd.read_sql_query(sql, conn)
                if not df.empty:
                    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y/%m/%d')
                    df_grouped = df.groupby('date')['point'].sum().reset_index()
                    df_grouped['total_point'] = df_grouped['point'].cumsum()
                    
                    label_name = "ポイント推移"
                    if "team" in sql.lower():
                        match = re.search(r"team\s*LIKE\s*'%([^']*)%'", sql, re.IGNORECASE)
                        label_name = f"{match.group(1)}のチーム推移" if match else "チーム推移"
                    else:
                        label_name = f"{df['player'].iloc[0]}の推移"

                    graph_data = {
                        "labels": df_grouped['date'].tolist(),
                        "data": df_grouped['total_point'].tolist(),
                        "label": label_name
                    }
                    final_prompt = f"""
                    Mリーグ実況者として解説してください。
                    質問: {user_query}
                    データ: {df_grouped.tail(5).to_string()}
                    「グラフをご覧ください」と添えてください。
                    """
                    res_text = openai.chat.completions.create(
                        model="gpt-4o", messages=[{"role": "system", "content": final_prompt}], temperature=0.3
                    )
                    return {"reply": res_text.choices[0].message.content, "graph": graph_data}
            except Exception as e:
                print(f"グラフエラー: {e}")
            finally:
                conn.close()

        # =========================================================
        # 2. ★「最新の結果」「試合結果」特化モード（ここを追加！）
        # =========================================================
        elif "最新" in user_query or "試合結果" in user_query or "昨日の結果" in user_query:
            conn = sqlite3.connect(DB_NAME)
            try:
                # (1) 直近の試合結果を取得（8レコード＝4人×2試合分 を目安に取得）
                # 日付の新しい順、その中で第2試合→第1試合の順、さらに着順でソート
                sql_games = "SELECT date, game_count, rank, player, point FROM games ORDER BY date DESC, game_count DESC, rank ASC LIMIT 8"
                df_games = pd.read_sql_query(sql_games, conn)
                
                # (2) 現在のチーム順位を取得
                sql_ranking = "SELECT rank, team, point FROM team_ranking ORDER BY rank"
                df_ranking = pd.read_sql_query(sql_ranking, conn)
                
                # データを合体してAIに渡す
                combined_data = f"【直近の試合結果(2試合分)】\n{df_games.to_string()}\n\n【現在のチーム順位】\n{df_ranking.to_string()}"
                
                print(f"最新結果モード実行:\n{combined_data}") # ログ確認用

                final_prompt = f"""
                あなたはMリーグの公式リポーターです。
                ユーザーの質問「{user_query}」に対し、以下のデータを元に見やすく報告してください。
                
                【データ】
                {combined_data}
                
                【出力のルール】
                1. **「直近の試合結果」** と **「現在のチーム順位」** の2つのセクションに分けてください。
                2. 試合結果は、日付ごとに「第1試合」「第2試合」を分けて、トップの選手だけでなく全順位(1位〜4位)を箇条書きで書いてください。
                3. チーム順位は1位から順に書いてください。
                4. 以下の絵文字を使ってリッチに表現してください。
                   - 📅 (日付)
                   - 🥇 🥈 🥉 4️⃣ (着順)
                   - 🏆 (チーム順位)
                5. 選手名とポイント、チーム名は **太字** にしてください。
                
                【出力例】
                📅 **11月21日の試合結果**
                
                **第1回戦**
                🥇 1位: **選手名** (+50.0)
                🥈 2位: **選手名** (+10.0)
                ...
                
                **第2回戦**
                🥇 1位: **選手名** (+60.0)
                ...
                
                🏆 **現在のチーム順位**
                1. **チームA** (500.0pt)
                2. **チームB** (300.0pt)
                ...
                """
                
                res_final = openai.chat.completions.create(
                    model="gpt-4o", messages=[{"role": "system", "content": final_prompt}], temperature=0.3
                )
                return {"reply": res_final.choices[0].message.content, "graph": None}

            except Exception as e:
                return {"reply": f"データ取得エラー: {e}", "graph": None}
            finally:
                conn.close()

        # =========================================================
        # 3. 通常モード（その他の質問）
        # =========================================================
        sql_prompt = f"""
        あなたはMリーグのデータエンジニアです。
        質問「{user_query}」に対し、適切なSQLを作成してください。
        
        【テーブル定義】
        1. stats (個人通算): player, team, points...
        2. games (日別試合結果): date, game_count, rank, player, point
        3. team_ranking (順位): rank, team, point
        
        【ルール】
        - 名前は LIKE '%キーワード%' で検索。
        - 「順位」だけ聞かれたら team_ranking。
        
        回答はSQLのみ。
        """
        res_sql = openai.chat.completions.create(
            model="gpt-4o", messages=[{"role": "system", "content": sql_prompt}], temperature=0
        )
        gen_sql = res_sql.choices[0].message.content.strip().replace("```sql", "").replace("```", "")
        print(f"通常SQL: {gen_sql}")
        
        conn = sqlite3.connect(DB_NAME)
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
        データがない場合は「該当データが見当たりませんでした」と答えてください。
        """
        res_final = openai.chat.completions.create(
            model="gpt-4o", messages=[{"role": "system", "content": final_prompt}], temperature=0.3
        )
        
        return {"reply": res_final.choices[0].message.content, "graph": None}

    except Exception as e:
        return {"reply": f"エラー: {str(e)}", "graph": None}