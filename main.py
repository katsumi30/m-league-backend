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
        elif "予想" in user_query or "成績" in user_query or "相性" in user_query or "vs" in user_query.lower():
            extract_prompt = f"""
            ユーザーの質問から、分析対象となる「選手名」を全て抽出してください。
            質問: "{user_query}"
            【選手名簿】{player_vocab}
            回答は選手名をカンマ区切りで出すだけ。（例: 多井隆晴, 伊達朱里紗）
            もしチーム名が書かれていたら、そのチームの代表的な選手を1名選んでください。
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
                2. チーム順位は以下の形式で書いてください：
                   1位: **チーム名** (540.0pt)
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
        # 4. ★直接対決・全記録モード（ここを追加・強化！）
        # ---------------------------------------------------------
        elif "対戦" in user_query and ("と" in user_query or "vs" in user_query.lower()):
            
            # Step A: 対戦する2名を特定
            extract_prompt = f"""
            ユーザーの質問から「対戦成績を比較したい2名の選手名」を抽出してください。
            
            質問: "{user_query}"
            【選手名簿】{player_vocab}
            
            回答は選手名をカンマ区切りで出すだけ。（例: 多井隆晴, 鈴木優）
            """
            res_names = openai.chat.completions.create(
                model="gpt-4o", messages=[{"role": "system", "content": extract_prompt}], temperature=0
            )
            names = [n.strip() for n in res_names.choices[0].message.content.split(',') if n.strip()]
            
            if len(names) < 2:
                return {"reply": "対戦する2名の選手名が見つかりませんでした。「多井隆晴と鈴木優の対戦成績」のように聞いてみてください。", "graph": None}

            p1_name = names[0]
            p2_name = names[1]

            conn = get_connection()
            try:
                # Step B: 「二人が同卓した試合」を特定する高度なSQL
                # (gamesテーブルを自己結合して、同じ日付・同じ回戦に両者がいるレコードを探す)
                sql_matchup = f"""
                SELECT 
                    T1.date as 日付,
                    T1.game_count as 回戦,
                    T1.player as 選手A, T1.rank as 着順A, T1.point as PtA,
                    T2.player as 選手B, T2.rank as 着順B, T2.point as PtB
                FROM games T1
                JOIN games T2 ON T1.date = T2.date AND T1.game_count = T2.game_count
                WHERE T1.player LIKE '%{p1_name}%' 
                  AND T2.player LIKE '%{p2_name}%'
                ORDER BY T1.date DESC
                """
                
                df_match = pd.read_sql_query(sql_matchup, conn)
                
                if df_match.empty:
                     return {"reply": f"データ上、{p1_name}選手と{p2_name}選手の直接対決は見つかりませんでした。", "graph": None}

                # Step C: 結果をAIに解説させる
                final_prompt = f"""
                あなたはMリーグのデータアナリストです。
                ユーザーの質問「{user_query}」に対し、以下の「直接対決の全記録」を元に解説してください。
                
                【直接対決データ ({len(df_match)}戦)】
                {df_match.to_string(index=False)}
                
                【出力ルール】
                1. **「トータルでどちらが勝ち越しているか（先着数など）」** をまず結論として述べてください。
                2. その後、**対戦履歴のリスト** を見やすく表示してください。
                   例: 
                   📅 11/21 第1試合
                   👊 **多井** (1位 +50.0) vs **鈴木** (3位 -20.0)
                3. 最後に「どちらが得意としているか」の相性分析を添えてください。
                """
                
                res_final = openai.chat.completions.create(
                    model="gpt-4o", messages=[{"role": "system", "content": final_prompt}], temperature=0.5
                )
                return {"reply": res_final.choices[0].message.content, "graph": None}
            
            finally:
                conn.close()

        # ---------------------------------------------------------
        # 5. 通常モード（★ここを最強の有能AIに改造しました！）
        # ---------------------------------------------------------
        table_info = """
        【テーブル定義書】
        1. stats (個人通算成績)
           - player: 選手名
           - team: チーム名
           - points: 通算ポイント (重要指標)
           - matches: 試合数
           - avg_rank: 平均着順 (2.5より小さければ優秀)
           - rank_1_count: 1位回数
           - top_rate: トップ率
           - last_avoid_rate: ラス回避率 (高いほど守備的)
           - best_score: 最高スコア
           - avg_score: 平均打点
           - riichi_rate: リーチ率
           - agari_rate: 和了率
           - hoju_rate: 放銃率 (低いほど守備的)
           - furo_rate: 副露率 (鳴き率)
        """

        sql_prompt = f"""
        あなたは世界一のMリーグデータアナリストです。
        質問「{user_query}」に対し、最も分析に適したデータを抽出するSQLを作成してください。
        
        【正しい名前リスト】
        選手: {player_vocab}
        チーム: {team_vocab}
        
        {table_info}
        
        【SQL作成の極意】
        1. ユーザーの入力をリストの名前に脳内変換し、必ず LIKE 検索を使ってください。
        2. 「スタッツ」や「成績」と聞かれたら、ケチらずに主要な指標（points, avg_rank, agari_rate, hoju_rate, riichi_rate, furo_rate, avg_score）を全てSELECTしてください。
        3. 「強いのは誰？」のような抽象的な質問なら、points や avg_rank でソートして上位5名を出してください。
        
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

        final_prompt = f"""
        あなたは熱狂的かつ知的なMリーグ実況解説者です。
        質問: {user_query}
        データ: {df_result.to_string()}
        
        【解説のルール】
        1. **数値を読むだけの実況は二流です。** その数値が何を意味するかを熱く語ってください。
           - 例: 「放銃率0.08」→「放銃率はわずか8%！これは驚異的な守備力、まさに鉄壁ですね！」
           - 例: 「平均着順2.1」→「2.1という数字は、圧倒的な強さの証明です。」
        
        2. **見やすさは命です。**
           - 重要な数字は **太字** に。
           - 項目ごとに改行し、箇条書き(・)を使ってください。
           - 絵文字（🀄, 🔥, 🛡️, 📊, ⚡）を適度に使って雰囲気を盛り上げてください。
        
        3. **数値の変換**
           - 率(rate)のデータは小数(0.25など)なので、必ず **100倍して%表記(25%)** に直してください。
           - ポイントのマイナスは「▲」を使ってください。
        """
        res_final = openai.chat.completions.create(
            model="gpt-4o", messages=[{"role": "system", "content": final_prompt}], temperature=0.5
        )
        return {"reply": res_final.choices[0].message.content, "graph": None}

    except Exception as e:
        return {"reply": f"エラー: {str(e)}", "graph": None}