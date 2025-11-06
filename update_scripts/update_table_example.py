import sys
import os
import logging
import traceback
from sqlalchemy import text, exc as sqlalchemy_exc # 引入 text 和 sqlalchemy_exc

# 將專案根目錄添加到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_connection import get_db_engine # 修改為 get_db_engine

# 設定日誌紀錄
log_file_name = 'update_table_example.log'
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), log_file_name)

# 為每個腳本實例化一個新的 logger，避免與其他腳本或主 scheduler 的 logger 混淆
logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.INFO)
# 創建 file handler
fh = logging.FileHandler(log_file_path)
fh.setLevel(logging.INFO)
# 創建 formatter 並將其添加到 handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# 將 handler 添加到 logger
if not logger.hasHandlers(): # 避免重複添加 handler
    logger.addHandler(fh)

# (可選) 如果也想在控制台看到此特定腳本的日誌，可以添加 StreamHandler
# sh = logging.StreamHandler()
# sh.setFormatter(formatter)
# logger.addHandler(sh)

def update_specific_table():
    """更新特定資料表的邏輯 (使用 SQLAlchemy)"""
    engine = None
    script_name = os.path.basename(__file__)
    logger.info(f"腳本 {script_name} 開始執行更新 (使用 SQLAlchemy)...")
    print(f"腳本 {script_name}: 開始執行更新 (使用 SQLAlchemy)...")

    try:
        logger.info(f"腳本 {script_name}: 嘗試獲取 SQLAlchemy Engine...")
        print(f"腳本 {script_name}: 嘗試獲取 SQLAlchemy Engine...")
        engine = get_db_engine()
        
        if engine is None:
            logger.error(f"腳本 {script_name}: 無法獲取 SQLAlchemy Engine，中止更新操作。")
            print(f"腳本 {script_name}: 錯誤 - 無法獲取 SQLAlchemy Engine。請檢查 .env 設定和資料庫狀態。")
            return
        
        logger.info(f"腳本 {script_name}: 成功獲取 SQLAlchemy Engine。")
        print(f"腳本 {script_name}: 成功獲取 SQLAlchemy Engine。")

        # 使用 with engine.connect() 來獲取連線，確保連線在使用後被關閉
        with engine.connect() as connection:
            logger.info(f"腳本 {script_name}: 成功建立資料庫連線。")
            print(f"腳本 {script_name}: 成功建立資料庫連線。")
            
            # --- 在這裡編寫您的 SQL 更新語句 ---
            logger.info(f"腳本 {script_name}: 準備執行 SQL 更新 (此處為範例，實際語句未執行)。")
            print(f"腳本 {script_name}: 準備執行 SQL 更新 (此處為範例)。")
            
            # 範例：(請取消註解並修改為您的實際邏輯)
            # table_to_update = 'your_table_name'
            # new_value = "updated_by_sqlalchemy"
            # condition_val = "condition_example"
            # logger.info(f"腳本 {script_name}: 準備更新資料表 '{table_to_update}'")
            # print(f"腳本 {script_name}: 準備更新資料表 '{table_to_update}'")
            
            # 使用 text() 構建 SQL，並使用 :param 綁定參數以防止 SQL 注入
            # sql_query = text(f"UPDATE {table_to_update} SET column1 = :new_val WHERE condition_column = :cond_val")
            # params = {"new_val": new_value, "cond_val": condition_val}
            
            # logger.debug(f"腳本 {script_name}: 執行 SQL: {sql_query} 參數: {params}")
            # print(f"腳本 {script_name}: 執行 SQL 更新...") 
            # result = connection.execute(sql_query, params)
            # logger.info(f"腳本 {script_name}: 更新影響的行數: {result.rowcount}")
            # print(f"腳本 {script_name}: 更新影響的行數: {result.rowcount}")
            # -----------------------------------
            
            # 如果有執行寫入操作，需要 commit
            # connection.commit()
            # logger.info(f"腳本 {script_name}: 資料庫 commit 成功。特定資料表更新完成。")
            # print(f"腳本 {script_name}: 資料庫 commit 成功。特定資料表更新完成。")
            
            # 由於這是範例，我們只記錄/打印一條成功消息
            logger.info(f"腳本 {script_name}: (範例) 特定資料表更新邏輯執行完畢。")
            print(f"腳本 {script_name}: (範例) 特定資料表更新邏輯執行完畢。")

    except sqlalchemy_exc.SQLAlchemyError as db_err: # 捕獲 SQLAlchemy 的錯誤
        # SQLAlchemy 的 Engine 和 Connection 會自動處理 rollback (如果事務失敗)
        # 但明確記錄總是好的
        logger.error(f"腳本 {script_name}: 更新時發生 SQLAlchemy 資料庫錯誤: {db_err}\n{traceback.format_exc()}")
        print(f"腳本 {script_name}: 錯誤 - 更新時發生 SQLAlchemy 資料庫問題: {db_err}\nCLI 錯誤追蹤:\n{traceback.format_exc()}")

    except Exception as e:
        logger.error(f"腳本 {script_name}: 更新時發生未預期錯誤: {e}\n{traceback.format_exc()}")
        print(f"腳本 {script_name}: 錯誤 - 更新時發生未預期問題: {e}\nCLI 錯誤追蹤:\n{traceback.format_exc()}")

    finally:
        # Engine 通常不需要顯式關閉，SQLAlchemy 會管理連線池。
        # `with engine.connect() as connection:` 確保了單個連線的關閉。
        logger.info(f"腳本 {script_name} 執行結束。")
        print(f"腳本 {script_name}: 執行結束。")

if __name__ == "__main__":
    # from db_connection import get_db_engine # 移至頂部
    # import mysql.connector # 不再直接使用 mysql.connector
    update_specific_table() 