from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import pandas as pd
import openai
import re
import os

# ==========================================
# ★ APIキー設定 (本番用安全仕様) ★
# ==========================================
# サーバー(Render)の設定画面にある "OPENAI_API_KEY" を読み込みます
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
        # もしAPIキーが設定されていなければエラーを返す（安全装置）
        if not openai.api_key:
            return {"reply": "【エラー】APIキーが設定されていません。RenderのEnvironment Variablesを確認してください。", "graph": None}

        user_query = req.message
        graph_data = None
        
        # =========================================================
        # 1. グラフ生成モード
        # =========================================================
        if "推移" in user_query or "グラフ" in user_query:
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
        # 2. 最新結果・順位モード
        # =========================================================
        elif "順位" in user_query or "ランキング" in user_query or "最新" in user_query or "試合結果" in user_query:
            conn = sqlite3.connect(DB_NAME)
            try:
                sql_games = "SELECT date, game_count, rank, player, point FROM games ORDER BY date DESC, game_count DESC, rank ASC LIMIT 8"
                df_games = pd.read_sql_query(sql_games, conn)
                sql_ranking = "SELECT rank, team, point FROM team_ranking ORDER BY rank"
                df_ranking = pd.read_sql_query(sql_ranking, conn)
                combined_data = f"【直近の試合結果】\n{df_games.to_string()}\n\n【現在のチーム順位】\n{df_ranking.to_string()}"
                
                final_prompt = f"""
                あなたはMリーグの公式リポーターです。
                質問「{user_query}」に対し、以下のデータを元に見やすく報告してください。
                【データ】{combined_data}
                【重要：表示ルールの厳守】
                1. ハイフン「-」を区切り文字に使わないでください。
                2. チーム順位は「1位: **チーム名** (540.0pt)」の形式で。
                3. マイナスのポイントは `▲` または `-` を数字の直前につけてください。プラスの場合は記号なし。
                4. 順位に応じた絵文字(🥇,🥈,🥉,4️⃣,🏆)を使用。
                5. チーム名や選手名は **太字** にする。
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
        # 3. 通常モード
        # =========================================================
        sql_prompt = f"""
        あなたはMリーグのデータエンジニアです。
        質問「{user_query}」に対し、適切なSQLを作成してください。
        【DB内の正しい名前リスト】チーム: {TEAM_VOCAB} 選手: {PLAYER_VOCAB}
        【指示】ユーザーの入力を上記リストの正しい名前に脳内変換し、LIKE検索を使ってください。
        回答はSQLのみ。
        """
        res_sql = openai.chat.completions.create(
            model="gpt-4o", messages=[{"role": "system", "content": sql_prompt}], temperature=0
        )
        gen_sql = res_sql.choices[0].message.content.strip().replace("```sql", "").replace("```", "")
        
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
        【表示ルール】
        - 区切り文字としてハイフン「-」は絶対に使わないでください。
        - 「項目名: 値」の形式を使ってください。
        - データが見当たらない場合は正直に伝えてください。
        """
        res_final = openai.chat.completions.create(
            model="gpt-4o", messages=[{"role": "system", "content": final_prompt}], temperature=0.3
        )
        
        return {"reply": res_final.choices[0].message.content, "graph": None}

    except Exception as e:
        return {"reply": f"エラー: {str(e)}", "graph": None}