"""
update_bb_BallsStat_BattedBall_B.py
基於 bb_BallsStat_Batted Ball_B.ipynb 重構的打者擊球統計更新腳本
更新 bb_BallsStat_BattedBall_B 資料表
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
log_file_path = os.path.join(os.path.dirname(__file__), 'update_bb_BallsStat_BattedBall_B.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def calculate_batter_yearly_stats(stock):
    """
    計算打者各年度和球種的擊球統計數據
    """
    player = list(set(stock['Batter']))
    rows = []

    for i in tqdm(range(0, len(player))):
        for year in range(2022, 2026):
            TagP=[['FB','FT','SL','CT','CB','SC','CH','SP','SFF','KN','OT','?'],['FB','FT'],['SL'],['CT'],['CB','SC'],['CH'],['SP','SFF'],['KN']]
            TagPC=['全部','速球','滑球','卡特','曲球','變速','指叉','蝴蝶']
            for j in range(len(TagP)):
                df = stock[(stock['Batter'] == player[i])&(stock['Year'] == year)&(stock['TaggedPitchType'].isin(TagP[j]))].reset_index(drop=True)
                try:
                    # print(len(df))
                    new_row = {
                        '_id': ' ',
                        'Batter': player[i],
                        'Batterid': str(df.at[0, 'Batterid']),
                        'TaggedPitchType':TagPC[j],
                        'Year': year,
                        'GB/FB':GB_FB(df),
                        'GB%':GB_P(df),
                        'LD%':LD_P(df),
                        'FB%':FB_P(df),
                        'IFFB%':IFFB_P(df),
                        'HR/FB':HR_FB_P(df),
                        'IFH%':Infield_Hit_P(df),
                        'BUH%':Bunt_Hit_P(df),
                        'Soft%':Soft_P(df),
                        'Med%':Med_P(df),
                        'Hard%':Hard_P(df),
                        'Pull%':Pull_P(df),
                        'Cent%':Cent_P(df),
                        'Oppo%':Oppo_P(df),
                        'In-Play':len(df[(df['PitchCode'] == 'In-Play')]),
                        'GB':len(df[df['HitType'] == 'GROUND']),
                        'FB':len(df[df['HitType'].isin(['FLYB', 'POPB'])]),
                        'LD':len(df[df['HitType'] == 'LINE']),
                        'IFFB':len(df[df['HitType'] == 'POPB']),
                        'IFH':len(df[df['HitTag'] == 'INFH']),
                        'BU':len(df[(df['PitchCode'] == 'In-Play') & (df['BuntTag'].isin(['SBUNT', 'BUNT']))]),
                        'BUH':len(df[(df['BuntTag'].isin(['SBUNT', 'BUNT'])) & (df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR']))]),
                        'EV': '',
                        'Max EV': '',
                        'LA':''           
                    }
                    rows.append(new_row)
                except:
                    # print(player[i])
                    continue
    
    logger.info(f"完成年度統計計算，共產生 {len(rows)} 筆記錄")
    return pd.DataFrame(rows)

def calculate_batter_career_stats(stock):
    """
    計算打者生涯擊球統計數據
    """
    player = list(set(stock['Batter']))
    rows = []

    for i in tqdm(range(0, len(player))):
        df = stock[(stock['Batter'] == player[i])].reset_index(drop=True)
        try:
            # print(len(df))
            new_row = {
                '_id': ' ',
                'Batter': player[i],
                'Batterid': str(df.at[0, 'Batterid']),
                'TaggedPitchType':'全部',
                'Year': '生涯',
                'GB/FB':GB_FB(df),
                'GB%':GB_P(df),
                'LD%':LD_P(df),
                'FB%':FB_P(df),
                'IFFB%':IFFB_P(df),
                'HR/FB':HR_FB_P(df),
                'IFH%':Infield_Hit_P(df),
                'BUH%':Bunt_Hit_P(df),
                'Soft%':Soft_P(df),
                'Med%':Med_P(df),
                'Hard%':Hard_P(df),
                'Pull%':Pull_P(df),
                'Cent%':Cent_P(df),
                'Oppo%':Oppo_P(df),
                'In-Play':len(df[(df['PitchCode'] == 'In-Play')]),
                'GB':len(df[df['HitType'] == 'GROUND']),
                'FB':len(df[df['HitType'].isin(['FLYB', 'POPB'])]),
                'LD':len(df[df['HitType'] == 'LINE']),
                'IFFB':len(df[df['HitType'] == 'POPB']),
                'IFH':len(df[df['HitTag'] == 'INFH']),
                'BU':len(df[(df['PitchCode'] == 'In-Play') & (df['BuntTag'].isin(['SBUNT', 'BUNT']))]),
                'BUH':len(df[(df['BuntTag'].isin(['SBUNT', 'BUNT'])) & (df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR']))]),
                'EV': '',
                'Max EV': '',
                'LA':''           
            }
            rows.append(new_row)
        except:
            continue
        
    logger.info(f"完成生涯統計計算，共產生 {len(rows)} 筆記錄")
    return pd.DataFrame(rows)

def update_specific_table():
    """
    更新 bb_BallsStat_BattedBall_B 資料表的主要函數
    """
    logger.info("開始更新 test_BallsStat_BattedBall_B 測試資料表...")
    
    try:
        # 建立資料庫連線 (使用原本 notebook 的連線資訊)
        engine = create_engine("mysql+pymysql://lshyu0520:O1ueufpkd5ivf@database-test.c4zrhmao4pj4.ap-northeast-1.rds.amazonaws.com:38064/test_ERP_Modules")
        
        logger.info("成功建立資料庫連線")
        
        # 讀取 bb_BallsStat_CPBL 資料
        logger.info("讀取 bb_BallsStat_CPBL 資料...")
        with engine.connect() as con:
            stock = pd.read_sql("SELECT * FROM bb_BallsStat_CPBL WHERE Year IN (2022, 2023, 2024,2025) AND GameNo BETWEEN 1 AND 360 ", con)
        stock.Theta = pd.to_numeric(stock.Theta)
        
        logger.info(f"讀取到 {len(stock)} 筆 bb_BallsStat_CPBL 記錄")
        
        # 計算打者年度擊球統計
        df_yearly = calculate_batter_yearly_stats(stock)
        
        # 計算打者生涯擊球統計
        df_career = calculate_batter_career_stats(stock)
        
        # 合併年度和生涯統計
        bb_BallsStat_BattedBall_B = pd.concat([df_yearly, df_career], ignore_index=True)
        
        if len(bb_BallsStat_BattedBall_B) == 0:
            logger.warning("沒有產生任何統計數據")
            return
        
        logger.info(f"產生 {len(bb_BallsStat_BattedBall_B)} 筆打者擊球統計記錄")
        
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
            logger.info("檢查 test_BallsStat_BattedBall_B 測試表...")
            cursor.execute("SHOW TABLES LIKE 'test_BallsStat_BattedBall_B'")
            table_exists = cursor.fetchone()
            
            if table_exists:
                logger.info("測試表已存在，清空原有資料...")
                cursor.execute("DELETE FROM test_BallsStat_BattedBall_B")
                db.commit()
                logger.info("測試表資料清空完成")
            else:
                logger.info("測試表不存在，將創建新表...")
            
            # 插入新資料到測試表
            logger.info("開始插入新的統計數據到測試表...")
            with engine.connect() as con:
                bb_BallsStat_BattedBall_B.to_sql(
                    "test_BallsStat_BattedBall_B", 
                    con, 
                    index=False, 
                    if_exists="append", 
                    chunksize=10000
                )
            
            logger.info("測試表資料插入完成")
            logger.info(f"成功更新 test_BallsStat_BattedBall_B 測試資料表，共 {len(bb_BallsStat_BattedBall_B)} 筆記錄")
            
            # 輸出 CSV 檔案
            csv_filename = f"test_BallsStat_BattedBall_B_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            csv_path = os.path.join(os.path.dirname(__file__), csv_filename)
            bb_BallsStat_BattedBall_B.to_csv(csv_path, index=False, encoding='utf-8-sig')
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
        logger.error(f"更新 test_BallsStat_BattedBall_B 測試資料表時發生錯誤: {e}")
        logger.error(f"錯誤詳情: {traceback.format_exc()}")
        raise

def main():
    """
    主函數，用於直接執行腳本時調用
    """
    update_specific_table()

if __name__ == "__main__":
    main()
