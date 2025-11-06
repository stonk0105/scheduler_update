"""
update_bb_Pitcher_advance_stat.py
基於 bb_BallsStat_Batted Ball_P.ipynb 重構的投手擊球統計更新腳本
更新 bb_Pitcher_advance_stat 資料表
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
log_file_path = os.path.join(os.path.dirname(__file__), 'update_bb_Pitcher_advance_stat.log')
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
    """
    更新 bb_Pitcher_advance_stat 資料表的主要函數
    """
    logger.info("開始更新 test_BallsStat_BattedBall_P 測試資料表...")
    
    try:
        # 建立資料庫連線 (使用原本 notebook 的連線資訊)
        engine =create_engine("mysql+pymysql://cloudeep:iEEsgOxVpU4RIGMo@database-test.c4zrhmao4pj4.ap-northeast-1.rds.amazonaws.com:38064/test_ERP_Modules")
        
        logger.info("成功建立資料庫連線")
        
        # 讀取 bb_BallsStat_CPBL 資料
        logger.info("讀取 bb_BallsStat_CPBL 資料...")
        with engine.connect() as con:
            stock = pd.read_sql(f"SELECT * FROM bb_BallsStat_CPBL WHERE Year IN (2022, 2023, 2024,2025) AND GameNo BETWEEN 1 AND 360 ", con)
        stock.Theta = pd.to_numeric(stock.Theta)
        
        logger.info(f"讀取到 {len(stock)} 筆 bb_BallsStat_CPBL 記錄")
        
        logger.info("讀取 bb_P_Lead 資料...")
        with engine.connect() as con:
            P_lead = pd.read_sql(f"SELECT * FROM bb_P_Lead WHERE year IN (2022, 2023, 2024, 2025) AND game_No BETWEEN 1 AND 360 ", con)
        
        logger.info(f"讀取到 {len(P_lead)} 筆 bb_P_Lead 記錄")
        
        player = list(set(stock['Pitcher']))
        rows = []
        League_wOBA = dict()
        League_OBP = dict()
        League_wOBA_scale = dict()
        League_PA = dict()
        Total_R = dict( )
        League_SLG = dict()
        League_wRC = dict()

        for i in range(2022, 2026):
            df = stock[stock['Year'] == i].reset_index(drop=True)
            League_wOBA[i] = wOBA(df)
            League_OBP[i] = OBP(df)
            League_wOBA_scale[i] = float(League_wOBA[i]) / float(League_OBP[i])
            print(f"{i}年聯盟wOBA: {League_wOBA[i]}, OBP: {League_OBP[i]}, wOBA Scale: {League_wOBA_scale[i]}")
            League_PA[i] = PA_N(df)
            League_SLG[i] = SLG(df)
            
        for i in range(2022, 2026):
            df = P_lead[P_lead['year'] == i].reset_index(drop=True)
            Total_R[i]= int(df.R.sum())
            
        for i in range(2022, 2026):
            df = stock[stock['Year'] == i].reset_index(drop=True)
            League_wRC[i] = (((float(wOBA(df)) - float(League_wOBA[i])) / float(League_wOBA_scale[i])) + (Total_R[i] / League_PA[i])) * PA_N(df)

        for i in tqdm(range(0, len(player))):
            for year in range(2022, 2026):
                TagP=[['FB','FT','SL','CT','CB','SC','CH','SP','SFF','KN','OT','?'],['FB','FT'],['SL'],['CT'],['CB','SC'],['CH'],['SP','SFF'],['KN']]
                TagPC=['全部','速球','滑球','卡特','曲球','變速','指叉','蝴蝶']
                for j in range(len(TagP)):
                    df = stock[(stock['Pitcher'] == player[i])&(stock['Year'] == year)&(stock['TaggedPitchType'].isin(TagP[j]))].reset_index(drop=True)
                    try:
                        wRAA = (float(wOBA(df)) - float(League_wOBA[year])) / float(League_wOBA_scale[year]) * float(PA_N(df))
                    except:
                        wRAA = '---'
                        
                    try:
                        wRC = (((float(wOBA(df)) - float(League_wOBA[year])) / float(League_wOBA_scale[year])) + (Total_R[year] / League_PA[year])) * PA_N(df)   # wRC = (((wOBA-League wOBA)/wOBA Scale)+(League R/PA))*PA
                    except:
                        wRC = '---'
                        
                    try:
                        wRC_plus = (wRAA/PA_N(df) + Total_R[year] / League_PA[year]) / (League_wRC[year] / League_PA[year]) * 100
                    except:
                        wRC_plus = '---'
                        
                    try:
                        new_row = {
                        '_id': ' ',
                        'Year': year,
                        'Pitcher': player[i],
                        'Pitcherid': str(df.at[0, 'Pitcherid']),
                        'TaggedPitchType':TagPC[j],
                        'AVG':AVG(df),
                        'OBP':OBP(df),
                        'SLG':SLG(df),
                        'OPS':OPS(df),
                        'ISO':ISO(df),
                        'BABIP':BABIP(df),
                        'K%':K_P(df),
                        'BB%':BB_P(df),   
                        'wOBA':wOBA(df),
                        'wRAA':wRAA,   # ((wOBA-League wOBA)/wOBA Scale)*PA = wRAA
                        'wRC':wRC,   # wRC = (((wOBA-League wOBA)/wOBA Scale)+(League R/PA))*PA
                        'wRC+':wRC_plus,
                        'OPS+':100 * (float(OBP(df)) / float(League_OBP[year]) + float(SLG(df)) / float(League_SLG[year]) - 1), # (上壘率/聯盟平均上壘率)+(長打率/聯盟平均長打率)-1
                        }
                        rows.append(new_row)
                    except:
                        # print(player[i])
                        continue

        bb_Pitcher_advance_stat = pd.DataFrame(rows)

        
        # 建立 pymysql 連線用於資料更新
        db = pymysql.connect(
            host="database-test.c4zrhmao4pj4.ap-northeast-1.rds.amazonaws.com",
            user="lshyu0520",
            password="O1ueufpkd5ivf",
            database="test_ERP_Modules",
            port=38064
        )
        cursor = db.cursor()
        
        # 建立有寫入權限的 engine（使用 lshyu0520 用戶）
        write_engine = create_engine("mysql+pymysql://lshyu0520:O1ueufpkd5ivf@database-test.c4zrhmao4pj4.ap-northeast-1.rds.amazonaws.com:38064/test_ERP_Modules")
        
        try:
            # 檢查測試表是否存在，如果存在則清空資料，否則創建新表
            logger.info("檢查 test_bb_Pitcher_advance_stat 測試表...")
            cursor.execute("SHOW TABLES LIKE 'test_bb_Pitcher_advance_stat'")
            table_exists = cursor.fetchone()
            
            if table_exists:
                logger.info("測試表已存在，清空原有資料...")
                cursor.execute("DELETE FROM test_bb_Pitcher_advance_stat")
                db.commit()
                logger.info("測試表資料清空完成")
            else:
                logger.info("測試表不存在，將創建新表...")
            
            # 插入新資料到測試表（使用有寫入權限的 engine）
            logger.info("開始插入新的統計數據到測試表...")
            with write_engine.connect() as con:
                bb_Pitcher_advance_stat.to_sql(
                    "test_bb_Pitcher_advance_stat", 
                    con, 
                    index=False, 
                    if_exists="append", 
                    chunksize=10000
                )
            
            logger.info("測試表資料插入完成")
            logger.info(f"成功更新 test_bb_Pitcher_advance_stat 測試資料表，共 {len(bb_Pitcher_advance_stat)} 筆記錄")
            
            # 輸出 CSV 檔案
            csv_filename = f"test_bb_Pitcher_advance_stat_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            csv_path = os.path.join(os.path.dirname(__file__), csv_filename)
            bb_Pitcher_advance_stat.to_csv(csv_path, index=False, encoding='utf-8-sig')
            logger.info(f"CSV 檔案已輸出至: {csv_path}")
            
        except Exception as e:
            logger.error(f"資料庫操作時發生錯誤: {e}")
            db.rollback()
            raise
        finally:
            cursor.close()
            db.close()
            
    except Exception as e:
        logger.error(f"更新 test_bb_Pitcher_advance_stat 測試資料表時發生錯誤: {e}")
        logger.error(f"錯誤詳情: {traceback.format_exc()}")
        raise

def main():
    """
    主函數，用於直接執行腳本時調用
    """
    update_specific_table()

if __name__ == "__main__":
    main()
