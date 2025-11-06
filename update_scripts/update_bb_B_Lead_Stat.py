"""
update_bb_B_Lead_Stat.py
基於 bb_B_Lead.ipynb 重構的打者數據更新腳本
更新 bb_B_Stat (打者年度統計) 和 bb_B_Lead (打者逐場數據)
"""

import sys
import os
import logging
import traceback
import pandas as pd
import requests
import json
import time
import warnings
from datetime import datetime
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.chrome.service import Service
from dotenv import load_dotenv

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

warnings.filterwarnings('ignore')

# 設定日誌
log_file_path = os.path.join(os.path.dirname(__file__), 'update_bb_B_Lead_Stat.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 全域變數 (來自 notebook)
current_token = None
current_game_token = None

# ========= 來自 Notebook 的輔助函數 =========

def _write_log(log_path, content):
    """寫入日誌到檔案"""
    log_dir = os.path.dirname(log_path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(content)

def update_player_data_by_id(player_id, player_name, df_filtered, engine, table_name,
                             key_column='batter_id', log_path=None, year=None):
    """
    根據 player_id / pitcher_id，更新對應 MySQL 資料表內容，並可選擇寫入 log。
    (來自 Notebook)
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if df_filtered.empty:
        msg = f"[{timestamp}] [!] 無資料可更新：{table_name}.{key_column} = {player_id}\n"
        if log_path:
            _write_log(log_path, msg)
        else:
            print(msg.strip())
        return

    # 組合 DELETE 條件
    where_clause = f"{key_column} = :val"
    params = {"val": player_id}
    if year is not None:
        where_clause += " AND Year = :year"
        params["year"] = year

    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {table_name} WHERE {where_clause}"),
            params
        )
        msg_del = f"[{timestamp}] [✓] {player_name} 已刪除 {table_name} {key_column} = {player_id}" + (f" AND Year = {year}" if year else "")

    # 插入資料
    df_filtered.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    msg_add = f"[{timestamp}] [✓] {player_name} 已插入 {table_name} {key_column} = {player_id} 的新資料，共 {len(df_filtered)} 筆"

    # 印出 or 寫入 log
    if log_path:
        _write_log(log_path, msg_del + '\n' + msg_add + '\n')
    else:
        print(msg_del)
        print(msg_add)

def rename_batter_stat_columns(df):
    """將打者統計資料欄位重新命名為標準格式 (來自 Notebook)"""
    rename_map = {
        'Name': 'batter_name',
        'player_id': 'batter_id',
        'Year': 'year',
        'TotalGames': 'G',
        'PlateAppearances': 'PA',
        'HitCnt': 'AB',
        'RunBattedINCnt': 'RBI',
        'ScoreCnt': 'R',
        'HittingCnt': 'H',
        'OneBaseHitCnt': '1B',
        'TwoBaseHitCnt': '2B',
        'ThreeBaseHitCnt': '3B',
        'HomeRunCnt': 'HR',
        'TotalBases': 'TB',
        'StrikeOutCnt': 'K',
        'StealBaseOKCnt': 'SB',
        'Obp': 'OBP',
        'Slg': 'SLG',
        'Avg': 'AVG',
        'DoublePlayBatCnt': 'DP',
        'SacrificeHitCnt': 'SH',
        'SacrificeFlyCnt': 'SF',
        'BasesONBallsCnt': 'BB',
        'IntentionalBasesONBallsCnt': 'IBB',
        'HitBYPitchCnt': 'HBP',
        'StealBaseFailCnt': 'CS',
        'GroundOut': 'GO',
        'FlyOut': 'AO',
        'Goao': 'GOAO',
        'SB': 'SB_P',
        'Ops': 'OPS'
    }
    return df.rename(columns=rename_map)

def rename_batter_game_log_columns(df):
    """將打者逐場紀錄資料欄位重新命名為標準格式 (來自 Notebook)"""
    rename_map = {
        'Year': 'year',
        'HitterName': 'batter_name',
        'player_id': 'batter_id',
        'team_name': 'batter_team',
        'GameDate': 'game_date',
        'GameSno': 'game_No',
        'FightTeamAbbrName': 'oppo_team',
        'PlateAppearances': 'PA',
        'HitCnt': 'AB',
        'RunBattedINCnt': 'RBI',
        'ScoreCnt': 'R',
        'HittingCnt': 'H',
        'TwoBaseHitCnt': '2B',
        'ThreeBaseHitCnt': '3B',
        'HomeRunCnt': 'HR',
        'TotalBases': 'TB',
        'StrikeOutCnt': 'K',
        'StealBaseOKCnt': 'SB',
        'StealBaseFailCnt': 'CS',
        'SacrificeHitCnt': 'SH',
        'SacrificeFlyCnt': 'SF',
        'BasesONBallsCnt': 'BB',
        'IntentionalBasesONBallsCnt': 'IBB',
        'HitBYPitchCnt': 'HBP',
        'DoublePlayBatCnt': 'DP',
        'TripplePlayBatCnt': 'TP',
        'Lobs': 'LOB',
        'PutoutCnt': 'PO',
        'AssistCnt': 'A',
        'JoinDoublePlayCnt': 'D_DP',
        'JoinTripplePlayCnt': 'D_TP',
        'ErrorCnt': 'E',
        'CaughtStealingCnt': 'D_CS',
        'PassedBallCnt': 'PB'
    }
    return df.rename(columns=rename_map)

# ========= API 相關函數 (來自 Notebook) =========

def get_api_token():
    """獲取 CPBL API Token (來自 Notebook)"""
    global current_token, current_game_token
    
    chrome_driver_path = os.getenv('CHROME_DRIVER_PATH')
    brave_browser_path = os.getenv('BRAVE_BROWSER_PATH')
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # 無頭模式
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    if brave_browser_path and os.path.exists(brave_browser_path):
        chrome_options.binary_location = brave_browser_path
    
    try:
        driver = webdriver.Chrome(
            # service=Service(chrome_driver_path),
            options=chrome_options
        )
        driver.get("https://www.cpbl.com.tw/schedule")
        current_game_token = driver.get_cookie('__RequestVerificationToken')
        src = driver.page_source
        text2find1 = "/schedule/getgamedatas"
        url_idx = src.find(text2find1)
        text2find2 = "RequestVerificationToken: '"
        token_idx = src.find(text2find2, url_idx)
        current_token = src[token_idx+len(text2find2):token_idx+len(text2find2)+185]
        driver.close()
        
        logger.info(f"成功獲取 API Token: {current_token[:20]}...")
        return current_token, current_game_token
        
    except Exception as e:
        logger.error(f"獲取 API Token 失敗: {e}")
        return None, None

def fetch_batting_score(acnt):
    """獲取打者年度統計 (來自 Notebook)"""
    global current_token

    url = "https://www.cpbl.com.tw/team/getbattingscore"

    headers = {
        'accept': '*/*',
        'accept-language': 'zh-TW,zh;q=0.5',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://www.cpbl.com.tw',
        'priority': 'u=1, i',
        'referer': f'https://www.cpbl.com.tw/team/person?acnt={acnt}',
        'requestverificationtoken': current_token,
        'sec-ch-ua': '"Brave";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'sec-gpc': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }

    payload = f"acnt={acnt}&kindCode=A"
    res = requests.post(url, headers=headers, data=payload)

    if not res.ok or res.text.strip() == "null":
        logger.warning(f"[失敗] 嘗試更新 token 再查詢 acnt={acnt}")
        get_api_token()
        headers['requestverificationtoken'] = current_token
        res = requests.post(url, headers=headers, data=payload)

        if not res.ok or res.text.strip() == "null":
            logger.error(f"[失敗] 重新嘗試後仍無法取得資料 acnt={acnt}")
            return None

    try:
        return res.json()
    except:
        return res.text

def fetch_lead(acnt, batter_or_pitcher, year):
    """獲取球員逐場數據 (來自 Notebook)"""
    global current_token

    url = "https://www.cpbl.com.tw/team/getfollowscore"

    headers = {
        'accept': '*/*',
        'accept-language': 'zh-TW,zh;q=0.5',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://www.cpbl.com.tw',
        'priority': 'u=1, i',
        'referer': f'https://www.cpbl.com.tw/team/person?acnt={acnt}',
        'requestverificationtoken': current_token,
        'sec-ch-ua': '"Brave";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'sec-gpc': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }

    if batter_or_pitcher == 'B':
        payload = f"acnt={acnt}&kindCode=A&year={year}"
    else:
        payload = f"acnt={acnt}&kindCode=A&year={year}&defendStation=%E6%8A%95%E6%89%8B"

    res = requests.post(url, headers=headers, data=payload)

    if not res.ok or res.text.strip() == "null":
        logger.warning(f"[失敗] 嘗試更新 token 再查詢 acnt={acnt}")
        get_api_token()
        headers['requestverificationtoken'] = current_token
        res = requests.post(url, headers=headers, data=payload)

        if not res.ok or res.text.strip() == "null":
            logger.error(f"[失敗] 重新嘗試後仍無法取得資料 acnt={acnt}")
            return None

    try:
        return res.json()
    except:
        return res.text

# ========= 主要更新函數 =========

def update_specific_table():
    """主要的更新函數 (基於 Notebook 邏輯)"""
    logger.info("========== 開始執行 update_bb_B_Lead_Stat.py ==========")
    print("========== 開始執行 update_bb_B_Lead_Stat.py ==========")
    
    # 獲取年份
    try:
        claim_year = int(os.getenv('UPDATE_LEAD_YEAR', '2025'))
        logger.info(f"更新年份: {claim_year}")
    except (TypeError, ValueError):
        logger.error("環境變數 UPDATE_LEAD_YEAR 設定錯誤")
        return

    # 建立資料庫連線
    try:
        engine = get_db_engine()
        logger.info("資料庫連線建立成功")
    except Exception as e:
        logger.error(f"資料庫連線失敗: {e}")
        return

    # 讀取球員資料 (仿照 Notebook)
    logger.info("讀取球員資料...")
    try:
        with engine.connect() as cnx:
            dfplayer = pd.read_sql("SELECT * FROM `vw_PlayerList_info`", con=cnx)
            
        # 篩選中華職棒球員 (仿照 Notebook)
        dfplayer = dfplayer[dfplayer['league_name']=='中華職棒'].reset_index(drop=True)
        logger.info(f"找到 {len(dfplayer)} 位中華職棒球員")
        
        if dfplayer.empty:
            logger.warning("沒有找到中華職棒球員")
            return
            
    except Exception as e:
        logger.error(f"讀取球員資料失敗: {e}")
        return

    # 獲取 API Token
    logger.info("獲取 API Token...")
    token_result = get_api_token()
    if not token_result[0]:
        logger.error("獲取 API Token 失敗")
        return

    # 開始處理球員 (仿照 Notebook 的主迴圈)
    log_path = os.path.join(os.path.dirname(__file__), "log", "update_batter_stats.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    
    processed_count = 0
    total_batters = len(dfplayer[dfplayer['player_batter_pitcher'] == 'B'])
    logger.info(f"開始處理 {total_batters} 位打者...")

    for i in range(len(dfplayer)):
        time.sleep(1)
        if dfplayer['player_batter_pitcher'].iloc[i] == 'B':
            processed_count += 1
            acnt = dfplayer['cpbl_player_id'].iloc[i]
            player_id = dfplayer['player_id'].iloc[i]
            player_name = dfplayer['player_name'].iloc[i]
            team_name = dfplayer['team_name'].iloc[i]
            
            logger.info(f"[{processed_count}/{total_batters}] 處理打者: {player_name} (acnt: {acnt})")
            print(f"{player_name} start")
            
            # A. 處理年度統計 (bb_B_Stat)
            try:
                res_bstat = fetch_batting_score(acnt)
                if res_bstat and isinstance(res_bstat, dict) and "BattingScore" in res_bstat:
                    bstat_data = json.loads(res_bstat["BattingScore"])
                    if bstat_data:
                        df_bstat = pd.DataFrame(bstat_data)
                        df_bstat['player_id'] = player_id
                        
                        # 欄位選擇 (來自 Notebook)
                        df_bstat = df_bstat[['Name','player_id','Year','TotalGames','PlateAppearances','HitCnt',
                            'RunBattedINCnt', 'ScoreCnt','HittingCnt', 
                            'OneBaseHitCnt', 'TwoBaseHitCnt', 'ThreeBaseHitCnt','HomeRunCnt', 
                            'TotalBases', 'StrikeOutCnt', 'StealBaseOKCnt',
                            'Obp','Slg', 'Avg', 
                            'DoublePlayBatCnt', 'SacrificeHitCnt', 'SacrificeFlyCnt',
                            'BasesONBallsCnt', 'IntentionalBasesONBallsCnt', 'HitBYPitchCnt',
                            'StealBaseFailCnt', 'GroundOut', 'FlyOut', 'Goao', 'SB', 'Ops']]
                        
                        df_bstat = rename_batter_stat_columns(df_bstat)
                        update_player_data_by_id(player_id, player_name, df_bstat, engine, 'bb_B_Stat', 'batter_id', log_path)
                        
            except Exception as e:
                logger.error(f"處理 {player_name} 年度統計失敗: {e}")

            # B. 處理逐場數據 (bb_B_Lead)
            try:
                res_lead = fetch_lead(acnt, 'B', claim_year)
                if res_lead and isinstance(res_lead, dict) and "FollowScore" in res_lead:
                    lead_data = json.loads(res_lead["FollowScore"])
                    if lead_data:
                        df_blead = pd.DataFrame(lead_data)
                        df_blead['player_id'] = player_id
                        df_blead['team_name'] = team_name
                        df_blead['GameDate'] = df_blead['GameDate'].str[:10]
                        
                        # 欄位選擇 (來自 Notebook)
                        df_blead = df_blead[['Year','HitterName','player_id','team_name','GameDate','GameSno',
                              'FightTeamAbbrName','PlateAppearances', 'HitCnt', 'RunBattedINCnt',
                              'ScoreCnt', 'HittingCnt', 'TwoBaseHitCnt',
                              'ThreeBaseHitCnt', 'HomeRunCnt', 'TotalBases', 'StrikeOutCnt',
                              'StealBaseOKCnt', 'StealBaseFailCnt', 'SacrificeHitCnt',
                              'SacrificeFlyCnt', 'BasesONBallsCnt', 'IntentionalBasesONBallsCnt',
                              'HitBYPitchCnt', 'DoublePlayBatCnt', 'TripplePlayBatCnt', 'Lobs',
                              'PutoutCnt', 'AssistCnt', 'JoinDoublePlayCnt', 'JoinTripplePlayCnt',
                              'ErrorCnt', 'CaughtStealingCnt', 'PassedBallCnt']]
                        
                        df_blead = rename_batter_game_log_columns(df_blead)
                        update_player_data_by_id(player_id, player_name, df_blead, engine, 'bb_B_Lead', 'batter_id', log_path, claim_year)
                        
            except Exception as e:
                logger.error(f"處理 {player_name} 逐場數據失敗: {e}")

    logger.info("========== update_bb_B_Lead_Stat.py 執行完畢 ==========")
    print("========== update_bb_B_Lead_Stat.py 執行完畢 ==========")

if __name__ == "__main__":
    # 環境變數檢查
    required_vars = ['CHROME_DRIVER_PATH', 'UPDATE_LEAD_YEAR', 'DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        error_msg = f"請設定以下環境變數: {', '.join(missing_vars)}"
        logger.error(error_msg)
        print(error_msg)
        sys.exit(1)
    
    logger.info("腳本以獨立模式執行")
    update_specific_table() 