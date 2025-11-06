from sqlalchemy import create_engine, exc as sqlalchemy_exc
import os
from dotenv import load_dotenv
import logging # 為了在連線失敗時記錄日誌

# 如果此模組被直接執行，配置一個基本的 logger
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_engine():
    """建立並返回一個 SQLAlchemy Engine"""
    load_dotenv()  # 從 .env 檔案載入環境變數

    try:
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")

        if not all([db_user, db_password, db_host, db_name]):
            logging.error("資料庫連線資訊不完整。請檢查 .env 檔案中的 DB_USER, DB_PASSWORD, DB_HOST, DB_NAME 設定。")
            print("錯誤：資料庫連線資訊不完整。請檢查 .env 檔案。")
            return None

        # SQLAlchemy 的 MySQL 連線字串格式 (使用 mysqlconnector 驅動):
        # mysql+mysqlconnector://user:password@host:port/database
        db_url = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}"
        
        if db_port and db_port.isdigit():
            db_url += f":{db_port}"
            logging.info(f"準備使用連接埠 {db_port} 建立資料庫引擎...")
            print(f"準備使用連接埠 {db_port} 建立資料庫引擎...")
        else:
            logging.info("DB_PORT 未設定或無效，將使用預設連接埠 (通常是 3306)。")
            print("DB_PORT 未設定或無效，將使用預設連接埠 (通常是 3306)。")

        db_url += f"/{db_name}"

        logging.info(f"使用資料庫 URL: mysql+mysqlconnector://{db_user}:****@{db_host}:{db_port or '預設'}/{db_name}")
        print(f"使用資料庫 URL: mysql+mysqlconnector://{db_user}:****@{db_host}:{db_port or '預設'}/{db_name}")

        # echo=True 會將 SQLAlchemy 執行的 SQL 語句打印到控制台，用於調試
        # 您可以在生產環境中將其設為 False 或移除
        engine = create_engine(db_url, echo=False, pool_recycle=3600) 
        
        # 測試連線 (可選，但建議)
        # create_engine 不會立即連線，第一次執行操作 (如 .connect()) 時才會
        try:
            with engine.connect() as connection:
                logging.info("成功建立 SQLAlchemy Engine 並測試連線成功！")
                print("成功建立 SQLAlchemy Engine 並測試連線成功！")
            return engine
        except sqlalchemy_exc.SQLAlchemyError as e_conn:
            logging.error(f"建立 SQLAlchemy Engine 後測試連線失敗: {e_conn}", exc_info=True)
            print(f"錯誤: 建立 SQLAlchemy Engine 後測試連線失敗: {e_conn}")
            return None

    except Exception as e:
        logging.error(f"建立 SQLAlchemy Engine 時發生未預期錯誤: {e}", exc_info=True)
        print(f"錯誤: 建立 SQLAlchemy Engine 時發生未預期錯誤: {e}")
        return None

if __name__ == '__main__':
    print("開始測試 SQLAlchemy 資料庫引擎建立功能 (請確保 .env 已設定)...")
    engine = get_db_engine()
    if engine:
        print("SQLAlchemy Engine 建立測試成功。")
        # 您可以在這裡進一步測試執行一個簡單的查詢
        # from sqlalchemy import text
        # try:
        #     with engine.connect() as connection:
        #         result = connection.execute(text("SELECT 1"))
        #         print(f"執行 'SELECT 1' 結果: {result.scalar_one()}")
        #         connection.commit() # 如果是 SELECT，通常不需要 commit
        # except Exception as e_query:
        #     print(f"使用引擎執行查詢時出錯: {e_query}")
    else:
        print("SQLAlchemy Engine 建立測試失敗。請檢查 .env 設定及錯誤訊息。") 