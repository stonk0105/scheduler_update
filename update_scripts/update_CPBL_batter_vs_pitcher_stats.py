import sys
import os
import logging
import traceback
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text
import pymysql
from dotenv import load_dotenv
from tqdm import tqdm
import warnings

DB_HOST = "database-test.c4zrhmao4pj4.ap-northeast-1.rds.amazonaws.com"
DB_USER = "lshyu0520"
DB_PASS = "O1ueufpkd5ivf"
DB_NAME = "test_ERP_Modules"
DB_PORT = 38064

# 載入環境變數
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path, override=True)

# 導入 db_connection
try:
    from db_connection import get_db_engine
except ImportError:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from db_connection import get_db_engine

# 導入 Toolbox
try:
    from Toolbox import *
except ImportError:
    # 如果無法導入 Toolbox，嘗試從父目錄導入
    parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    from Toolbox import *

warnings.filterwarnings('ignore')

# 設定日誌
log_file_path = os.path.join(os.path.dirname(__file__), 'update_CPBL_batter_vs_pitcher_stats.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def update_specific_table():
    logger.info("開始更新 test_CPBL_batter_vs_pitcher_stats 測試資料表...")
    
    try:
        # 建立資料庫連線 (使用原本 notebook 的連線資訊)
        engine = create_engine("mysql+pymysql://lshyu0520:O1ueufpkd5ivf@database-test.c4zrhmao4pj4.ap-northeast-1.rds.amazonaws.com:38064/test_ERP_Modules")
        
        logger.info("成功建立資料庫連線")
        
        # 讀取 bb_BallsStat_CPBL 資料
        logger.info("讀取 bb_BallsStat_CPBL 資料...")
        with engine.connect() as con:
            cpbldata = pd.read_sql(
            """
            SELECT
                Batter,
                Batterid,
                Pitcher,
                PitcherTeam,
                PA_Result,
                PlayResult,
                KorBB,
                plate_id
            FROM bb_BallsStat_CPBL
            """,
            con,
        )
        
        logger.info(f"讀取到 {len(cpbldata)} 筆 bb_BallsStat_CPBL 記錄")
        
        logger.info("讀取 bb_PlateRecord_Backup 讀取 RBI 資料...")
        with engine.connect() as con:
            plate_rec = pd.read_sql(
                """
                SELECT
                    plate_id,
                    record_type,
                    action_code
                FROM bb_PlateRecord_Backup
                WHERE record_type = 'earned'
                AND action_code = 'RBI'
                """,
                con,
            )
        logger.info(f"讀取到 {len(plate_rec)} 筆 bb_PlateRecord_Backup 記錄")
        
        logger.info("🛠 清理欄位 ...")
        for c in ["PA_Result", "PlayResult", "KorBB", "plate_id"]:
            if c not in cpbldata.columns:
                cpbldata[c] = np.nan

        cpbldata["PA_Result"] = cpbldata["PA_Result"].fillna("").astype(str)
        cpbldata["PlayResult"] = cpbldata["PlayResult"].fillna("").astype(str)
        cpbldata["KorBB"] = cpbldata["KorBB"].fillna("").astype(str)
        cpbldata["plate_id"] = cpbldata["plate_id"].fillna("").astype(str)

        cpbldata["PA_UP"] = cpbldata["PA_Result"].str.upper()
        cpbldata["PLAY_UP"] = cpbldata["PlayResult"].str.upper()
        cpbldata["KORBB_UP"] = cpbldata["KorBB"].str.upper()

        logger.info("✅ 欄位清理完成")

        # --------------------------
        # 定義判斷集合
        # --------------------------
        hit_results = ['1B', '2B', '3B', 'HR', 'IHR']
        so_results  = ['K', 'Ks', 'K-DO', 'K-BS', 'K-DS', 'K-SF', 'K-P']
        bb_results  = ['BB', 'BB-I', 'BB-IL', 'IBB', 'BB-P']

        def count_pa(g):
            s = g["plate_id"].astype(str)
            s = s[(s != "") & (s != "0")]
            return s.nunique() if len(s) > 0 else len(g)

        # --------------------------
        # 建立 plate_id → 有無RBI 的集合（字串形式）
        # --------------------------
        rbi_ids = set(plate_rec['plate_id'].astype(str).unique())
        logger.debug(f"📌 Debug: 共找到 {len(rbi_ids)} 個有 RBI 的 plate_id (Backup)")

        # 檢查 bb_BallsStat_CPBL 中有多少筆 row 的 plate_id 屬於 rbi_ids（確認 mapping coverage）
        matched_mask = cpbldata['plate_id'].astype(str).isin(rbi_ids)
        matched_count = int(matched_mask.sum())
        logger.debug(f"📌 Debug: bb_BallsStat_CPBL 中對應到的 row 數 = {matched_count}")
        if matched_count > 0:
            logger.debug("  🔎 範例 (bb_BallsStat_CPBL 中對應到的前 3 筆)：")
            logger.debug(f"{cpbldata[matched_mask].head(3).to_dict(orient='records')}")

        # --------------------------
        # 分組計算 Batter vs Pitcher (不計 ER)
        # --------------------------
        logger.info("🔄 開始 groupby 計算 ...")
        group_cols = ["Batter", "PitcherTeam", "Pitcher"]
        grouped = cpbldata.groupby(group_cols, dropna=False)

        rows = []
        for idx, ((batter, pteam, pitcher), g) in enumerate(grouped, 1):
            if idx % 500 == 0:
                logger.info(f"  ⏳ 已處理 {idx} 個 batter-pitcher 組合...")

            pa = int(count_pa(g))
            pa_up = g["PA_UP"]

            hits = int(pa_up.isin(hit_results).sum())
            bb   = int(pa_up.isin(bb_results).sum())
            so   = int(pa_up.isin(so_results).sum())
            hbp  = int((pa_up == "HBP").sum())

            # AB = PA - BB - HBP
            ab = max(pa - bb - hbp, 0)
            avg = round((hits / ab) if ab > 0 else 0.0, 3)

            # === 計算 RBI（只要該組合的任何 plate_id 在 rbi_ids 裡就算一次）
            # 注意：若同一組合在不同 plate_id 各有 RBI，會加總
            g_plate_ids = set(g['plate_id'].astype(str))
            rbi = sum(1 for pid in g_plate_ids if pid in rbi_ids)

            rows.append({
                "Batter": batter,
                "PitcherTeam": pteam,
                "Pitcher": pitcher,
                "Pa": pa,
                "Ab": ab,
                "Rbi": int(rbi),
                "Xb": int(hits),   # Xb 用 hits 作為 total bases placeholder（若需要可改）
                "So": int(so),
                "Hit": int(hits),
                "Hbp": int(hbp),
                "Bb": int(bb),
                "Avg": float(avg),
            })

        logger.info(f"✅ groupby 計算完成，共得到 {len(rows)} 筆紀錄")

        # --------------------------
        # DataFrame + _id
        # --------------------------
        logger.info("📚 建立 DataFrame ...")
        stats_df = pd.DataFrame(rows, columns=[
            "Batter","PitcherTeam","Pitcher","Pa","Ab","Rbi","Xb","So","Hit","Hbp","Bb","Avg"
        ])
        stats_df.insert(0, "_id", range(1, len(stats_df) + 1))
        logger.info(f"✅ DataFrame 建立完成，shape={stats_df.shape}")

        # 顯示一些有RBI的範例，方便你檢查
        rbi_positive = stats_df[stats_df['Rbi'] > 0]
        logger.debug(f"🔎 Debug: 共 {len(rbi_positive)} 個 batter-pitcher 組合有 Rbi>0 (示範 5 筆):")
        if len(rbi_positive) > 0:
            logger.debug(f"{rbi_positive.head(5).to_dict(orient='records')}")
        
        # 定義創建表的 SQL 語句
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_CPBL_batter_vs_pitcher_stats (
            _id INT AUTO_INCREMENT PRIMARY KEY,
            Batter VARCHAR(255),
            PitcherTeam VARCHAR(255),
            Pitcher VARCHAR(255),
            Pa INT,
            Ab INT,
            Rbi INT,
            Xb INT,
            So INT,
            Hit INT,
            Hbp INT,
            Bb INT,
            Avg DECIMAL(5, 3),
            INDEX idx_batter (Batter),
            INDEX idx_pitcher (Pitcher),
            INDEX idx_pitcher_team (PitcherTeam)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        # 建立 pymysql 連線用於資料更新
        db = pymysql.connect(
            host="database-test.c4zrhmao4pj4.ap-northeast-1.rds.amazonaws.com",
            user="lshyu0520",
            password="O1ueufpkd5ivf",
            database="test_ERP_Modules",
            port=38064
        )
        cursor = db.cursor()
        
        try:
            # 檢查測試表是否存在，如果存在則清空資料，否則創建新表
            logger.info("檢查 test_CPBL_batter_vs_pitcher_stats 測試表...")
            cursor.execute("SHOW TABLES LIKE 'test_CPBL_batter_vs_pitcher_stats'")
            table_exists = cursor.fetchone()
            
            if table_exists:
                logger.info("測試表已存在，清空原有資料...")
                cursor.execute("DELETE FROM test_CPBL_batter_vs_pitcher_stats")
                db.commit()
                logger.info("測試表資料清空完成")
            else:
                logger.info("測試表不存在，將創建新表...")
                cursor.execute(create_table_sql)
                db.commit()
                logger.info("測試表創建完成")
            
            # 插入新資料到測試表
            logger.info("開始插入新的統計數據到測試表...")
            with engine.connect() as con:
                stats_df.to_sql(
                    "test_CPBL_batter_vs_pitcher_stats", 
                    con, 
                    index=False, 
                    if_exists="append", 
                    chunksize=10000
                )
            
            logger.info("測試表資料插入完成")
            logger.info(f"成功更新 test_CPBL_batter_vs_pitcher_stats 測試資料表，共 {len(stats_df)} 筆記錄")
            
            # 輸出 CSV 檔案
            csv_filename = f"test_CPBL_batter_vs_pitcher_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            csv_path = os.path.join(os.path.dirname(__file__), csv_filename)
            stats_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            logger.info(f"CSV 檔案已輸出至: {csv_path}")
            
        except Exception as e:
            logger.error(f"資料庫操作時發生錯誤: {e}")
            db.rollback()
            raise
        finally:
            cursor.close()
            db.close()
            
    except Exception as e:
        logger.error(f"更新 test_CPBL_batter_vs_pitcher_stats 測試資料表時發生錯誤: {e}")
        logger.error(f"錯誤詳情: {traceback.format_exc()}")
        raise

def main():
    """
    主函數，用於直接執行腳本時調用
    """
    update_specific_table()

if __name__ == "__main__":
    main()