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
log_file_path = os.path.join(os.path.dirname(__file__), 'update_CPBL_pitcher_vs_batter_stats.log')
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
    logger.info("開始更新 test_CPBL_pitcher_vs_batter_stats 測試資料表...")
    
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
                    Pitcherid,
                    PitcherTeam,
                    BatterTeam,
                    PA_Result,
                    PlayResult,
                    KorBB,
                    plate_id
                FROM bb_BallsStat_CPBL
                """,
                con,
            )
        
        logger.info(f"讀取到 {len(cpbldata)} 筆 bb_BallsStat_CPBL 記錄")
        
        
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

        LIST_AB      = ['1B', '2B', '3B', 'HR', 'IHR', 'DP', 'E-C', 'E-T', 'F', 'FC', 'FOT', 'G', 'G-', 'GT', 'IF', 'INT',
                        'K', 'K-BF', 'K-DO', 'K-BS', 'K-DS', 'Ks', 'K-SF', 'LO', 'K-P']
        LIST_NON_AB  = ['BB', 'BB-I', 'BB-IL', 'IBB', 'SH', 'SF', 'E-SF', 'FSH', 'E-SHC', 'E-SHT', 'HBP', 'OBC']

        def count_pa(g):
            s = g["plate_id"].astype(str)
            s = s[(s != "") & (s != "0")]
            return s.nunique() if len(s) > 0 else len(g)

        # --------------------------
        # 分組計算
        # --------------------------
        logger.info("🔄 開始 groupby 計算 ...")
        group_cols = ["Pitcher", "BatterTeam", "Batter"]
        grouped = cpbldata.groupby(group_cols, dropna=False)

        rows = []
        for idx, ((pitcher, bteam, batter), g) in enumerate(grouped, 1):
            if idx % 500 == 0:
                logger.info(f"  ⏳ 已處理 {idx} 個 pitcher-batter 組合...")

            pa = int(count_pa(g))
            pa_up = g["PA_UP"]

            hits = int(pa_up.isin(hit_results).sum())
            bb   = int(pa_up.isin(bb_results).sum())
            so   = int(pa_up.isin(so_results).sum())
            hbp  = int((pa_up == "HBP").sum())

            ab = max(pa - bb - hbp, 0)
            avg = round((hits / ab) if ab > 0 else 0.0, 3)

            rows.append({
                "Pitcher": pitcher,
                "BatterTeam": bteam,
                "Batter": batter,
                "Pa": pa,
                "Hit": hits,
                "Hbp": hbp,
                "Bb": bb,
                "Avg": avg,
            })

        logger.info(f"✅ groupby 計算完成，共得到 {len(rows)} 筆紀錄")

        # --------------------------
        # DataFrame + _id
        # --------------------------
        logger.info("📚 建立 DataFrame ...")
        stats_df = pd.DataFrame(rows, columns=[
            "Pitcher","BatterTeam","Batter","Pa","Hit","Hbp","Bb","Avg"
        ])
        stats_df.insert(0, "_id", range(1, len(stats_df) + 1))
        logger.info(f"✅ DataFrame 建立完成，shape={stats_df.shape}")
        
        # 定義創建表的 SQL 語句
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_CPBL_pitcher_vs_batter_stats (
            _id INT AUTO_INCREMENT PRIMARY KEY,
            Pitcher VARCHAR(255),
            BatterTeam VARCHAR(255),
            Batter VARCHAR(255),
            Pa INT,
            Hit INT,
            Hbp INT,
            Bb INT,
            Avg DECIMAL(5, 3),
            INDEX idx_pitcher (Pitcher),
            INDEX idx_batter_team (BatterTeam),
            INDEX idx_batter (Batter)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            port=DB_PORT,
            charset="utf8mb4",
            autocommit=False,
        )
        cursor = conn.cursor()
        try:
            # 檢查測試表是否存在，如果存在則清空資料，否則創建新表
            logger.info("檢查 test_CPBL_pitcher_vs_batter_stats 測試表...")
            cursor.execute("SHOW TABLES LIKE 'test_CPBL_pitcher_vs_batter_stats'")
            table_exists = cursor.fetchone()
            
            if table_exists:
                logger.info("測試表已存在，清空原有資料...")
                cursor.execute("DELETE FROM test_CPBL_pitcher_vs_batter_stats;")
                conn.commit()
                logger.info("測試表資料清空完成")
            else:
                logger.info("測試表不存在，將創建新表...")
                cursor.execute(create_table_sql)
                conn.commit()
                logger.info("測試表創建完成")

            insert_sql = """
                INSERT INTO test_CPBL_pitcher_vs_batter_stats
                (_id, Pitcher, BatterTeam, Batter, Pa, Hit, Hbp, Bb, Avg)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            values = []
            for _, r in stats_df.iterrows():
                values.append((
                    int(r["_id"]),
                    r["Pitcher"],
                    r["BatterTeam"],
                    r["Batter"],
                    int(r["Pa"]),
                    int(r["Hit"]),
                    int(r["Hbp"]),
                    int(r["Bb"]),
                    float(r["Avg"]),
                ))

            batch_size = 1000
            for i in range(0, len(values), batch_size):
                chunk = values[i:i + batch_size]
                cursor.executemany(insert_sql, chunk)
                conn.commit()
                logger.info(f"  ✅ 已寫入 {i + len(chunk)} 筆資料")

            logger.info(f"🎉 全部完成，共寫入 {len(values)} 筆")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ 發生錯誤，已 rollback：{e}")
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"更新 test_CPBL_pitcher_vs_batter_stats 測試資料表時發生錯誤: {e}")
        logger.error(f"錯誤詳情: {traceback.format_exc()}")
        raise

def main():
    """
    主函數，用於直接執行腳本時調用
    """
    update_specific_table()

if __name__ == "__main__":
    main()