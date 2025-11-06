"""
update_bb_P_Lead_Stat.py
基於 bb_P_Lead.ipynb 重構的投手數據更新腳本
更新 bb_P_Stat (投手年度統計) 和 bb_P_Lead (投手逐場數據)
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
log_file_path = os.path.join(os.path.dirname(__file__), 'update_bb_P_Lead_Stat.log')
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
                             key_column='pitcher_id', log_path=None, year=None):
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

def rename_pitcher_stat_columns(df):
    """
    將原始投手逐年或逐場統計欄位，重新命名為標準格式。
    (來自 Notebook)
    """
    rename_map = {
        'Name': 'pitcher_name',
        'player_id': 'pitcher_id',
        'Year': 'year',
        'TotalGames': 'G',
        'PitchStarting': 'GS',
        'PitchCloser': 'SVO',
        'CompleteGames': 'CG',
        'ShoutOut': 'SHO',
        'NoBaseBalled': 'NO_BB',
        'Wins': 'W',
        'Loses': 'L',
        'SaveOK': 'SV',
        'SaveFail': 'BS',
        'ReliefPointCnt': 'H',
        'InningPitched': 'INN',
        'Whip': 'WHIP',
        'Era': 'ERA',
        'PlateAppearances': 'BF',
        'PitchCnt': 'BC',
        'HittingCnt': 'Hit',
        'HomeRunCnt': 'HR',
        'BasesONBallsCnt': 'BB',
        'IntentionalBasesONBallsCnt': 'IBB',
        'HitBYPitchCnt': 'HBP',
        'StrikeOutCnt': 'SO',
        'WildPitchCnt': 'WP',
        'BalkCnt': 'BK',
        'RunCnt': 'R',
        'EarnedRunCnt': 'ER',
        'GroundOut': 'GO',
        'FlyOut': 'AO',
        'Goao': 'GOAO'
    }
    return df.rename(columns=rename_map)

def rename_pitcher_game_log_columns(df):
    """
    將投手逐場紀錄資料欄位重新命名為標準格式。
    (來自 Notebook)
    """
    rename_map = {
        'Year': 'year',
        'PitcherName': 'pitcher_name',
        'player_id': 'pitcher_id',
        'team_name': 'pitcher_team',
        'GameDate': 'game_date',
        'GameSno': 'game_No',
        'FightTeamAbbrName': 'oppo_team',
        'RoleType': 'position',
        'GS': 'GS',
        'W': 'W',
        'L': 'L',
        'H': 'H',
        'SV': 'SV',
        'SaveFail': 'BS',
        'CompleteGames': 'CG',
        'ShoutOut': 'SHO',
        'NoBaseBalled': 'NO_BB',
        'InningPitchedCnt': 'INN',
        'Outs': 'Outs',
        'PlateAppearances': 'BF',
        'PitchCnt': 'BC',
        'HittingCnt': 'Hit',
        'HomeRunCnt': 'HR',
        'StrikeOutCnt': 'SO',
        'RunCnt': 'R',
        'EarnedRunCnt': 'ER',
        'BasesONBallsCnt': 'BB',
        'IntentionalBasesONBallsCnt': 'IBB',
        'HitBYPitchCnt': 'HBP',
        'GroundOut': 'GO',
        'FlyOut': 'AO',
        'StrikeCnt': 'SC',
        'StealCnt': 'SB',
        'WildPitchCnt': 'WP',
        'BalkCnt': 'BK',
        'PutoutCnt': 'PO',
        'AssistCnt': 'A',
        'JoinDoublePlayCnt': 'DP',
        'JoinTripplePlayCnt': 'TP',
        'ErrorCnt': 'E',
        'PitchOutCnt': 'pickO'
    }
    return df.rename(columns=rename_map)

def set_pitcher_game_result_flags(df, result_col='GameResult'):
    """
    根據 GameResult 欄位自動設定 W、L、SV 三欄位為 0 或 1
    (來自 Notebook)

    - '勝' -> W = 1
    - '敗' -> L = 1
    - '救援成功' -> SV = 1
    - '中繼成功' -> H = 1
    """
    df['W'] = (df[result_col] == '勝').astype(int)
    df['L'] = (df[result_col] == '敗').astype(int)
    df['SV'] = (df[result_col] == '救援成功').astype(int)
    df['H'] = (df[result_col] == '中繼成功').astype(int)
    return df

def convert_inn_to_outs(df, inn_col='InningPitchedCnt', outs_col='Outs'):
    """
    將 INN 欄位轉換為 Outs 欄位（每局 3 出局，支援 x.1 → 1 出局，x.2 → 2 出局）
    (來自 Notebook)
    
    INN 例：1.2 → 1 局又 2 人出局 → 3+2 = 5
    """
    df[outs_col] = df[inn_col].apply(lambda x: int(x) * 3 + int(round((x % 1) * 10)))
    return df

def set_gs_by_role_type(df, role_col='RoleType', target_col='GS'):
    """
    根據 RoleType 是否為「先發」來設定 GS 欄位為 1 或 0
    (來自 Notebook)
    
    - role_col: 角色欄位（預設為 'RoleType'）
    - target_col: 欲設定的目標欄位（預設為 'GS'）
    """
    df[target_col] = 0
    df.loc[df[role_col] == '先發', target_col] = 1
    return df

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

def fetch_pitching_score(acnt):
    """獲取投手年度統計 (來自 Notebook)"""
    global current_token

    url = "https://www.cpbl.com.tw/team/getpitchscore"

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
    logger.info("========== 開始執行 update_bb_P_Lead_Stat.py ==========")
    print("========== 開始執行 update_bb_P_Lead_Stat.py ==========")
    
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
    log_path = os.path.join(os.path.dirname(__file__), "log", "update_pitcher_stats.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    
    processed_count = 0
    total_pitchers = len(dfplayer[dfplayer['player_batter_pitcher'] == 'P'])
    logger.info(f"開始處理 {total_pitchers} 位投手...")

    for i in range(len(dfplayer)):
        time.sleep(1)
        if dfplayer['player_batter_pitcher'].iloc[i] == 'P':
            processed_count += 1
            acnt = dfplayer['cpbl_player_id'].iloc[i]
            player_id = dfplayer['player_id'].iloc[i]
            player_name = dfplayer['player_name'].iloc[i]
            team_name = dfplayer['team_name'].iloc[i]
            
            logger.info(f"[{processed_count}/{total_pitchers}] 處理投手: {player_name} (acnt: {acnt})")
            print(f"{player_name} start")
            
            # A. 處理年度統計 (bb_P_Stat)
            try:
                res_pstat = fetch_pitching_score(acnt)
                if res_pstat and isinstance(res_pstat, dict) and "PitchScore" in res_pstat:
                    pstat_data = json.loads(res_pstat["PitchScore"])
                    if pstat_data:
                        df_pstat = pd.DataFrame(pstat_data)
                        df_pstat['player_id'] = player_id
                        
                        # 欄位選擇 (來自 Notebook)
                        df_pstat = df_pstat[['Name','player_id','Year','TotalGames','PitchStarting','PitchCloser','ShoutOut','CompleteGames',
                               'NoBaseBalled', 'Wins','Loses', 'SaveOK', 'SaveFail', 'ReliefPointCnt','InningPitched',
                               'Whip', 'Era', 'PlateAppearances', 'PitchCnt', 'HittingCnt','HomeRunCnt', 
                               'BasesONBallsCnt', 'IntentionalBasesONBallsCnt','HitBYPitchCnt', 'StrikeOutCnt', 
                               'WildPitchCnt', 'BalkCnt', 'RunCnt','EarnedRunCnt', 'GroundOut', 'FlyOut', 'Goao']]
                        
                        df_pstat = rename_pitcher_stat_columns(df_pstat)
                        update_player_data_by_id(player_id, player_name, df_pstat, engine, 'bb_P_Stat', 'pitcher_id', log_path)
                        
            except Exception as e:
                logger.error(f"處理 {player_name} 年度統計失敗: {e}")

            # B. 處理逐場數據 (bb_P_Lead)
            try:
                res_lead = fetch_lead(acnt, 'P', claim_year)
                if res_lead and isinstance(res_lead, dict) and "FollowScore" in res_lead:
                    lead_data = json.loads(res_lead["FollowScore"])
                    if lead_data:
                        df_plead = pd.DataFrame(lead_data)
                        df_plead['player_id'] = player_id
                        df_plead['team_name'] = team_name    
                        df_plead['GameDate'] = df_plead['GameDate'].str[:10]
                        
                        # 投手特殊數據處理 (來自 Notebook)
                        df_plead = set_gs_by_role_type(df_plead)
                        df_plead = set_pitcher_game_result_flags(df_plead)
                        df_plead = convert_inn_to_outs(df_plead)
                        
                        # 欄位選擇 (來自 Notebook)
                        df_plead = df_plead[['Year','PitcherName','player_id','team_name','GameDate','GameSno','FightTeamAbbrName','RoleType','GS','W','L','H','SV','SaveFail',
                                'CompleteGames', 'ShoutOut', 'NoBaseBalled','InningPitchedCnt','Outs','PlateAppearances', 'PitchCnt',
                                'HittingCnt', 'HomeRunCnt','StrikeOutCnt', 'RunCnt', 'EarnedRunCnt', 'BasesONBallsCnt','IntentionalBasesONBallsCnt', 
                                'HitBYPitchCnt', 'GroundOut', 'FlyOut','StrikeCnt', 'StealCnt', 'WildPitchCnt', 'BalkCnt', 'PutoutCnt','AssistCnt', 
                                'JoinDoublePlayCnt', 'JoinTripplePlayCnt', 'ErrorCnt','PitchOutCnt']]
                        
                        df_plead = rename_pitcher_game_log_columns(df_plead)
                        update_player_data_by_id(player_id, player_name, df_plead, engine, 'bb_P_Lead', 'pitcher_id', log_path, claim_year)
                        
            except Exception as e:
                logger.error(f"處理 {player_name} 逐場數據失敗: {e}")

    logger.info("========== update_bb_P_Lead_Stat.py 執行完畢 ==========")
    print("========== update_bb_P_Lead_Stat.py 執行完畢 ==========")

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