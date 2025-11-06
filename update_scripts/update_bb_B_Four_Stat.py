"""
update_bb_B_Four_Stat.py
基於 bb_B_Four_Stat.py 重構的打者四項統計更新腳本
更新 test_bb_B_Four_Stat 測試資料表
"""

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
log_file_path = os.path.join(os.path.dirname(__file__), 'update_bb_bb_B_Four_Stat.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
        
def calc_zone_swing(df):
    """
    Zone Swing:
    好球帶 Zone==1 中，PitchCode 是 Strk-S|In-Play|Foul|PP 的比例
    """
    zone1 = df[df["Zone"] == 1]
    if len(zone1) == 0:
        return None
    swings = zone1["PitchCode"].str.contains(r"Strk-S|In-Play|Foul|PP", regex=True)
    return float(swings.mean())  # 回傳百分比

def calc_zone_contact(df):
    """
    Zone Contact:
    Zone==1 且 PitchCode 是 Strk-S|In-Play|Foul|PP 的球中，
    有多少比例是 In-Play|Foul|PP
    """
    zone1_sw = df[(df["Zone"] == 1) & df["PitchCode"].str.contains(r"Strk-S|In-Play|Foul|PP", regex=True)]
    if len(zone1_sw) == 0:
        return None
    contact = zone1_sw["PitchCode"].str.contains(r"In-Play|Foul|PP", regex=True)
    return float(contact.mean())

def calc_chase(df):
    """
    Chase:
    壞球帶 Zone==0 中，PitchCode 是 Strk-S|In-Play|Foul|PP 的比例
    """
    zone0 = df[df["Zone"] == 0]
    if len(zone0) == 0:
        return None
    swings = zone0["PitchCode"].str.contains(r"Strk-S|In-Play|Foul|PP", regex=True)
    return float(swings.mean())



def to_float_or_none(x, ndigits=None):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    try:
        v = float(x)
        return round(v, ndigits) if ndigits is not None else v
    except:
        return None

def update_specific_table():
    """
    更新 bb_B_Four_Stat 資料表的主要函數
    """
    logger.info("開始更新 test_B_Four_Stat 測試資料表...")
    
    try:
        # 建立資料庫連線 (使用原本 notebook 的連線資訊)
        engine = create_engine("mysql+pymysql://lshyu0520:O1ueufpkd5ivf@database-test.c4zrhmao4pj4.ap-northeast-1.rds.amazonaws.com:38064/test_ERP_Modules")
        
        logger.info("成功建立資料庫連線")
        
        # 讀取 bb_B_Four_Stat 資料
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
        
        logger.info(f"讀取到 {len(df)} 筆 bb_BallsStat_CPBL 記錄")
        
        df = df.dropna(subset=["Date"]).copy()
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df[~df["Date"].isna()].copy()
        df["Year"] = df["Date"].dt.year.astype(int)
        df["Zone"] = pd.to_numeric(df["Zone"], errors="coerce").fillna(0).astype(int)

        # --------------------------
        # half_month 計算
        # --------------------------
        first_day_per_year = df.groupby("Year")["Date"].min().to_dict()
        def compute_half_month(row):
            year_start = first_day_per_year[row["Year"]]
            delta_days = (row["Date"] - year_start).days
            return delta_days // 15 + 1
        df["half_month"] = df.apply(compute_half_month, axis=1)
        
        group_cols = ["Batterid", "Batter", "Year", "half_month"]
        grouped = df.groupby(group_cols, dropna=False)

        values = []
        for idx, ((bid, bname, year, hm), g) in enumerate(grouped, start=1):
            avg = to_float_or_none(AVG(g), 3)
            obp = to_float_or_none(OBP(g), 3)
            ops = to_float_or_none(OPS(g), 3)
            iso = to_float_or_none(ISO(g), 3)
            woba = to_float_or_none(wOBA(g), 3)
            babip = to_float_or_none(BABIP(g), 3)
            whiff = to_float_or_none(Whiff_P(g), 1)
            k_p = to_float_or_none(K_P(g), 1)
            bb_p = to_float_or_none(BB_P(g), 1)

            z_sw_raw = calc_zone_swing(g)
            z_con_raw = calc_zone_contact(g)
            chase_raw = calc_chase(g)
            z_sw = None if z_sw_raw is None else round(z_sw_raw,3)
            z_con = None if z_con_raw is None else round(z_con_raw,3)
            chase = None if chase_raw is None else round(chase_raw,3)

            values.append((
                idx, bid, bname, int(year), int(hm),
                avg, obp, ops, iso, woba, babip, z_con, z_sw, chase, k_p, bb_p, whiff
            ))
        
        logger.info(f"計算完成，共 {len(values)} 筆")
        
        # 將 values 轉換為 DataFrame
        bb_bb_B_Four_Stat = pd.DataFrame(values, columns=[
            "_id", "batterid", "battername", "Year", "half_month",
            "Avg", "OBP", "OPS", "ISO", "wOBA", "BABIP", "Z_Con", "Z_Sw", "Chase",
            "K_P", "BB_P", "Whiff"
        ])
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS `test_bb_B_Four_Stat` (
        `_id` INT NOT NULL PRIMARY KEY,
        `batterid` VARCHAR(255),
        `battername` VARCHAR(255),
        `Year` INT,
        `half_month` INT,
        `Avg` DOUBLE,
        `OBP` DOUBLE,
        `OPS` DOUBLE,
        `ISO` DOUBLE,
        `wOBA` DOUBLE,
        `BABIP` DOUBLE,
        `Z_Con` DOUBLE,
        `Z_Sw` DOUBLE,
        `Chase` DOUBLE,
        `K_P` DOUBLE,
        `BB_P` DOUBLE,
        `Whiff` DOUBLE
        ) CHARACTER SET = utf8mb4;
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
            logger.info("檢查 test_bb_B_Four_Stat 測試表...")
            cursor.execute("SHOW TABLES LIKE 'test_bb_B_Four_Stat'")
            table_exists = cursor.fetchone()
            
            if table_exists:
                logger.info("測試表已存在，清空原有資料...")
                cursor.execute("DELETE FROM test_bb_B_Four_Stat")
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
                bb_bb_B_Four_Stat.to_sql(
                    "test_bb_B_Four_Stat", 
                    con, 
                    index=False, 
                    if_exists="append", 
                    chunksize=10000
                )
            
            logger.info("測試表資料插入完成")
            logger.info(f"成功更新 test_bb_B_Four_Stat 測試資料表，共 {len(bb_bb_B_Four_Stat)} 筆記錄")
            
            # 輸出 CSV 檔案
            csv_filename = f"test_bb_B_Four_Stat_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            csv_path = os.path.join(os.path.dirname(__file__), csv_filename)
            bb_bb_B_Four_Stat.to_csv(csv_path, index=False, encoding='utf-8-sig')
            logger.info(f"CSV 檔案已輸出至: {csv_path}")
            print(f"✅ CSV 檔案已輸出至: {csv_path}")
            
        except Exception as e:
            logger.error(f"資料庫操作時發生錯誤: {e}")
            db.rollback()
            raise
        finally:
            cursor.close()
            db.close()
            
    except Exception as e:
        logger.error(f"更新 test_bb_B_Four_Stat 測試資料表時發生錯誤: {e}")
        logger.error(f"錯誤詳情: {traceback.format_exc()}")
        raise

def main():
    """
    主函數，用於直接執行腳本時調用
    """
    update_specific_table()

if __name__ == "__main__":
    main()
