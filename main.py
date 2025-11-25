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
    print("--- データベース読込開始 ---")
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT team FROM stats")
        teams = [r[0] for r in cur.fetchall() if r[0]]
        cur.execute("SELECT DISTINCT player FROM stats")
        players = [r[0] for r in cur.fetchall() if r[0]]
        conn.close()
        print(f"✅ 読み込み完了: 選手{len(players)}名, チーム{len(teams)}チーム")
        return ", ".join(teams), ", ".join(players)
    except Exception as e:
        print(f"❌ 読み込みエラー: {e}")
        return "", ""

TEAM_VOCAB, PLAYER_VOCAB = get_db_vocabulary()
print("--- データベース読込完了 ---")

# ★ サーバー診断ページ (/debug)
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

# ==========================================
# ★ チャット機能 (/chat) ★
# ==========================================
@app.post("/chat")
async def chat_endpoint(req: ChatRequest):
    try:
        if not openai.api_key:
            return {"reply": "【エラー】APIキーが設定されていません。Renderの環境変数を確認してください。", "graph": None}

        user_query = req.message
        graph_data = None
        
        # ---------------------------------------------------------
        # 1. グラフ生成モード
        # ---------------------------------------------------------
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
            print(f"📊 グラフSQL: {sql}")
            
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
            except Exception as e:
                print(f"グラフエラー: {e}")
            finally:
                conn.close()

        # ---------------------------------------------------------
        # 2. アナリストモード（勝敗予想・対戦成績） ★ここに追加しました！
        # ---------------------------------------------------------
        elif "予想" in user_query or "対戦" in user_query or "相性" in user_query or "vs" in user_query.lower():
            # Step A: 質問に含まれる選手名を特定する
            extract_prompt = f"""
            ユーザーの質問から、分析対象となる「選手名」を全て抽出してください。
            質問: "{user_query}"
            【選手名簿】{PLAYER_VOCAB}
            回答は選手名をカンマ区切りで出すだけ。（例: 多井隆晴, 伊達朱里紗）
            もしチーム名が書かれていたら、そのチームの代表的な選手を1名選んでください。
            """
            res_names = openai.chat.completions.create(
                model="gpt-4o", messages=[{"role": "system", "content": extract_prompt}], temperature=0
            )
            target_names = [n.strip() for n in res_names.choices[0].message.content.split(',') if n.strip()]
            
            if not target_names:
                return {"reply": "分析対象の選手名が特定できませんでした。選手名を入れて質問してください。", "graph": None}

            conn = get_connection()
            try:
                # Step B: 各選手の「通算スタッツ」を取得
                placeholders = ",".join(["?"] * len(target_names))
                sql_stats = f"SELECT * FROM stats WHERE player IN ({placeholders})"
                df_stats = pd.read_sql_query(sql_stats, conn, params=target_names)
                
                # Step C: 各選手の「直近5試合」を取得（調子を見るため）
                recent_data_text = ""
                for p in target_names:
                    sql_recent = "SELECT date, rank, point FROM games WHERE player = ? ORDER BY date DESC LIMIT 5"
                    df_recent = pd.read_sql_query(sql_recent, conn, params=[p])
                    if not df_recent.empty:
                        recent_data_text += f"\n【{p}の直近5戦】\n{df_recent.to_string(index=False)}\n"

                # Step D: アナリストAIに分析させる
                final_prompt = f"""
                あなたはMリーグのプロアナリストです。
                ユーザーの質問: "{user_query}"
                
                以下の「客観的なデータ」を元に、論理的な分析・予想を行ってください。
                
                【対象選手の今期スタッツ】
                {df_stats.to_string(index=False)}
                
                【対象選手の直近成績（勢い）】
                {recent_data_text}
                
                【指示】
                - 「勝敗予想」の場合は、スタッツ（平均着順やポイント）と直近の勢いを総合して、最も勝率が高そうな選手を1名挙げ、理由を解説してください。
                - 「対戦成績・相性」の場合は、それぞれのデータの強み（攻撃型か守備型かなど）を比較してください。
                - 最後に必ず「※データに基づく予想であり、結果を保証するものではありません」と注釈を入れてください。
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
                あなたはMリーグの公式リポーターです。
                質問「{user_query}」に対し、以下のデータを元に見やすく報告してください。
                【データ】{combined_data}
                【重要：表示ルールの厳守】
                1. **ハイフン「-」を区切り文字に使わないでください**。
                2. チーム順位は「1位: **チーム名** (540.0pt)」の形式で。
                3. マイナスのポイントは `▲` または `-` を数字の直前につけてください。
                4. 順位に応じた絵文字(🥇,🥈,🥉,4️⃣,🏆)を使ってください。
                5. チーム名や選手名は **太字** にしてください。
                """
                res_final = openai.chat.completions.create(
                    model="gpt-4o", messages=[{"role": "system", "content": final_prompt}], temperature=0.3
                )
                return {"reply": res_final.choices[0].message.content, "graph": None}
            except Exception as e:
                return {"reply": f"データ取得エラー: {e}", "graph": None}
            finally:
                conn.close()

        # ---------------------------------------------------------
        # 4. 通常モード
        # ---------------------------------------------------------
        sql_prompt = f"""
        あなたはMリーグのデータエンジニアです。
        質問「{user_query}」に対し、適切なSQLを作成してください。
        【正しい名前】選手: {PLAYER_VOCAB} チーム: {TEAM_VOCAB}
        【指示】ユーザー入力を上記リストの名前に変換し、LIKE検索してください。
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