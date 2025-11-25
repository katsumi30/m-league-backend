from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import pandas as pd
import openai
import re
import os

# ==========================================
# ★ APIキー設定
# ==========================================
openai.api_key = os.getenv("OPENAI_API_KEY")

app = FastAPI()
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

DB_NAME = 'm_league.db'

# DB接続ヘルパー
def get_connection():
    return sqlite3.connect(DB_NAME)

# 起動時にDBから名前リストを読み込む
def get_db_vocabulary():
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

TEAM_VOCAB, PLAYER_VOCAB = get_db_vocabulary()

# サーバー診断ページ (/debug)
@app.get("/debug")
def debug_endpoint():
    try:
        if not os.path.exists(DB_NAME):
            return {"status": "ERROR", "message": "DBファイルがありません"}
        conn = get_connection()
        df_stats = pd.read_sql_query("SELECT * FROM stats", conn)
        df_games = pd.read_sql_query("SELECT * FROM games", conn)
        conn.close()
        return {
            "status": "OK",
            "stats_count": len(df_stats),
            "games_count": len(df_games),
            "latest_date": df_games['date'].max() if not df_games.empty else "なし"
        }
    except Exception as e:
        return {"status": "ERROR", "detail": str(e)}

class ChatRequest(BaseModel):
    message: str

@app.post("/chat")
async def chat_endpoint(req: ChatRequest):
    try:
        if not openai.api_key:
            return {"reply": "【エラー】APIキーが設定されていません。", "graph": None}

        user_query = req.message
        graph_data = None
        
        # 毎回最新の辞書を取得
        team_vocab, player_vocab = get_vocab()

        # ---------------------------------------------------------
        # 1. グラフ生成モード
        # ---------------------------------------------------------
        if "推移" in user_query or "グラフ" in user_query:
            id_prompt = f"""
            ユーザーは「ポイント推移」を知りたいです。質問: "{user_query}"
            【正しい名前】チーム: {team_vocab} 選手: {player_vocab}
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
                    データ: {df_grouped.tail(5).to_string()}
                    「グラフをご覧ください」と添えてください。
                    """
                    res_text = openai.chat.completions.create(
                        model="gpt-4o", messages=[{"role": "system", "content": final_prompt}], temperature=0.3
                    )
                    return {"reply": res_text.choices[0].message.content, "graph": graph_data}
                else:
                     return {"reply": f"データが見つかりませんでした。\n試行したSQL: `{sql}`", "graph": None}
            except Exception as e:
                print(f"グラフエラー: {e}")
            finally:
                conn.close()

        # ---------------------------------------------------------
        # 2. アナリストモード（勝敗予想・対戦成績）
        # ---------------------------------------------------------
        elif "予想" in user_query or "対戦" in user_query or "相性" in user_query or "vs" in user_query.lower():
            extract_prompt = f"""
            ユーザーの質問から、分析対象となる「選手名」を全て抽出してください。
            質問: "{user_query}"
            【選手名簿】{player_vocab}
            回答は選手名をカンマ区切りで出すだけ。
            """
            res_names = openai.chat.completions.create(
                model="gpt-4o", messages=[{"role": "system", "content": extract_prompt}], temperature=0
            )
            target_names = [n.strip() for n in res_names.choices[0].message.content.split(',') if n.strip()]
            
            if not target_names:
                return {"reply": "分析対象の選手名が特定できませんでした。", "graph": None}

            conn = get_connection()
            try:
                placeholders = ",".join(["?"] * len(target_names))
                sql_stats = f"SELECT * FROM stats WHERE player IN ({placeholders})"
                df_stats = pd.read_sql_query(sql_stats, conn, params=target_names)
                
                recent_data_text = ""
                for p in target_names:
                    sql_recent = "SELECT date, rank, point FROM games WHERE player = ? ORDER BY date DESC LIMIT 5"
                    df_recent = pd.read_sql_query(sql_recent, conn, params=[p])
                    if not df_recent.empty:
                        recent_data_text += f"\n【{p}の直近5戦】\n{df_recent.to_string(index=False)}\n"

                final_prompt = f"""
                あなたはMリーグのプロアナリストです。
                質問: "{user_query}"
                
                【今期スタッツ】{df_stats.to_string(index=False)}
                【直近成績】{recent_data_text}
                
                【指示】
                - スタッツと勢いを総合して、論理的に分析してください。
                - 確率(0.x)は%に変換して話してください (例: 0.25 -> 25%)
                - 「※データに基づく予想です」と注釈を入れてください。
                """
                res_final = openai.chat.completions.create(
                    model="gpt-4o", messages=[{"role": "system", "content": final_prompt}], temperature=0.7
                )
                return {"reply": res_final.choices[0].message.content, "graph": None}
            finally:
                conn.close()

        # ---------------------------------------------------------
        # 3. 最新結果・順位モード
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
                Mリーグ公式リポーターとして報告してください。
                データ: {combined_data}
                ルール:
                - ハイフン区切り禁止。
                - 順位は絵文字(🥇🥈🥉4️⃣🏆)付き。
                - チーム順位は「1位: **チーム名** (500.0pt)」形式。
                - マイナスは「▲」を使用。
                """
                res_final = openai.chat.completions.create(
                    model="gpt-4o", messages=[{"role": "system", "content": final_prompt}], temperature=0.3
                )
                return {"reply": res_final.choices[0].message.content, "graph": None}
            finally:
                conn.close()

        # ---------------------------------------------------------
        # 4. 通常モード（★ここを修正！）
        # ---------------------------------------------------------
        sql_prompt = f"""
        MリーグデータエンジニアとしてSQLを作成してください。
        質問: {user_query}
        【正しい名前】選手: {player_vocab} チーム: {team_vocab}
        
        指示:
        - ユーザー入力をリストの名前に変換し、LIKE検索を使用。
        - 「スタッツ」などの漠然とした質問なら SELECT * で全カラム取得。
        - 「放銃率」「リーチ率」など特定の指標なら、それを含む SELECT 文。
        
        テーブル:
        1. stats (通算): riichi_rate(リーチ率), agari_rate(和了率), hoju_rate(放銃率)...
        2. games (日別): date, rank, player, point
        3. team_ranking (順位)
        
        回答はSQLのみ。
        """
        res_sql = openai.chat.completions.create(
            model="gpt-4o", messages=[{"role": "system", "content": sql_prompt}], temperature=0
        )
        gen_sql = res_sql.choices[0].message.content.strip().replace("```sql", "").replace("```", "")
        print(f"💬 通常SQL: {gen_sql}")
        
        conn = get_connection()
        try:
            df_result = pd.read_sql_query(gen_sql, conn)
        except:
            df_result = pd.DataFrame()
        finally:
            conn.close()

        if df_result.empty:
             return {"reply": f"該当データが見当たりませんでした。\n(実行SQL: `{gen_sql}`)", "graph": None}

        # ★ここが修正ポイント：数値の読み方を徹底指導★
        final_prompt = f"""
        Mリーグ解説者として質問に答えてください。
        質問: {user_query}
        データ: {df_result.to_string()}
        
        【重要：数値の読み方ルール】
        1. DB内の「率（レート）」は小数で保存されています（例: 0.25）。
        2. 回答する際は、必ず **100倍してパーセント表記** に直してください。
           - 0.25 -> 25%
           - 0.08 -> 8%
           - 0.0 -> 0%
        3. 小数をそのまま「0.08です」や、丸めて「0です」と答えるのは禁止です。
        4. ハイフン「-」区切り禁止。「項目: 値」の形式で。
        """
        res_final = openai.chat.completions.create(
            model="gpt-4o", messages=[{"role": "system", "content": final_prompt}], temperature=0.3
        )
        return {"reply": res_final.choices[0].message.content, "graph": None}

    except Exception as e:
        return {"reply": f"エラー: {str(e)}", "graph": None}