import os
import importlib
import logging
import sys
import traceback # 新增 traceback 模組
import argparse # 新增 argparse 模組
# import schedule # 移除 schedule
# import time   # 移除 time

# 設定主排程器的日誌
logging.basicConfig(filename='scheduler.log', 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

SCRIPTS_DIR = "update_scripts"

def run_script(script_name):
    """執行指定的更新腳本"""
    current_time = ""
    try:
        import time
        current_time = time.strftime('%Y-%m-%d %H:%M:%S') + " - "
    except ImportError:
        pass

    logging.info(f"{current_time}開始執行腳本: {script_name}")
    print(f"{current_time}開始執行腳本: {script_name}")
    try:
        # 確保 update_scripts 目錄在 sys.path 中，這樣 importlib 才能找到模組
        # 這裡假設 scheduler.py 在專案根目錄，update_scripts 是其子目錄
        module_path = os.path.abspath(SCRIPTS_DIR) # 取得 update_scripts 的絕對路徑
        if module_path not in sys.path:
            # 將 update_scripts 的父目錄(即專案根目錄)加入 sys.path
            # 這樣 importlib.import_module("update_scripts.script_name") 才能正確運作
            parent_dir = os.path.dirname(module_path)
            if parent_dir not in sys.path:
                 sys.path.insert(0, parent_dir)
            # 如果 update_scripts 本身不是一個有效的 package (例如缺少 __init__.py)，
            # 直接將其路徑加入 sys.path 可能不是最佳做法，
            # 更好的方式是確保 import 的是 package.module
            # 但考慮到目前架構，如果 update_scripts 被視為包含多個獨立腳本的目錄，
            # 且這些腳本內部處理了它們對根目錄 db_connection.py 的依賴，
            # 那麼這裡的 module_name = f"{SCRIPTS_DIR}.{script_name[:-3]}" 是合理的。

        module_name = f"{SCRIPTS_DIR}.{script_name[:-3]}" # 模組名稱格式為 'update_scripts.your_script_name'
        
        # 如果模組已經被載入，重新載入它以獲取最新更改
        if module_name in sys.modules:
            script_module = importlib.reload(sys.modules[module_name])
        else:
            script_module = importlib.import_module(module_name)
        
        if hasattr(script_module, 'update_specific_table'):
            script_module.update_specific_table()
            logging.info(f"{current_time}腳本 {script_name} 的 update_specific_table 函數執行成功")
            print(f"{current_time}腳本 {script_name} 執行成功")
        elif hasattr(script_module, 'main'): # 向下相容，如果腳本有 main()
            script_module.main()
            logging.info(f"{current_time}腳本 {script_name} 的 main 函數執行成功")
            print(f"{current_time}腳本 {script_name} 執行成功")
        else:
            logging.warning(f"{current_time}腳本 {script_name} 中找不到可執行的 update_specific_table 或 main 函數")
            print(f"{current_time}警告: 腳本 {script_name} 中找不到可執行的 update_specific_table 或 main 函數")
            
    except Exception as e:
        error_details = traceback.format_exc()
        logging.error(f"{current_time}執行腳本 {script_name} 時發生嚴重錯誤:\n{error_details}")
        print(f"{current_time}錯誤: 執行腳本 {script_name} 時發生問題。詳細資訊請查看 scheduler.log。\nCLI 錯誤追蹤:\n{error_details}")

def job(specific_scripts=None):
    """定義要執行的工作。可以指定只執行特定的腳本。"""
    current_time = ""
    try:
        import time
        current_time = time.strftime('%Y-%m-%d %H:%M:%S') + " - "
    except ImportError:
        pass 

    if specific_scripts:
        logging.info(f"{current_time}scheduler job 開始：僅執行指定的腳本: {specific_scripts}")
        print(f"{current_time}scheduler job 開始：僅執行指定的腳本: {specific_scripts}")
        scripts_to_run = []
        for script_name in specific_scripts:
            # 驗證指定的腳本是否存在於 SCRIPTS_DIR 中且為 .py 檔案
            script_path = os.path.join(SCRIPTS_DIR, script_name)
            if not script_name.endswith(".py"): # 將 filename 改為 script_name
                 # 自動加上 .py 副檔名 (如果使用者忘記輸入)
                 #  這裡檢查 script_path + ".py" 比較好，因為 script_name 可能還沒加上 .py
                 #  但原始邏輯是檢查 script_path + ".py"，我們先保持一致性
                 #  更 robust 的做法是先檢查 script_name 是否包含 .py，若無，則嘗試附加並檢查
                 potential_py_script_path = os.path.join(SCRIPTS_DIR, script_name + ".py")
                 actual_script_name_to_check = script_name # 保存原始的 script_name 以供日誌記錄

                 if os.path.isfile(potential_py_script_path):
                      script_name += ".py"
                      script_path = potential_py_script_path # 更新 script_path
                 else: 
                    logging.warning(f"{current_time}指定的腳本 '{actual_script_name_to_check}' 不是 .py 檔案，或附加 .py 後在 '{SCRIPTS_DIR}' 中仍找不到，將跳過。")
                    print(f"{current_time}警告: 指定的腳本 '{actual_script_name_to_check}' 不是 .py 檔案，或附加 .py 後在 '{SCRIPTS_DIR}' 中仍找不到，將跳過。")
                    continue       
            
            # 在這裡，script_name 應該已經是 .py 結尾 (如果原本不是且能找到 .py 版本)
            # script_path 也應該是更新後的正確路徑
            if os.path.isfile(script_path):
                scripts_to_run.append(script_name)
            else:
                logging.warning(f"{current_time}指定的腳本 '{script_name}' 在 '{SCRIPTS_DIR}' 中找不到，將跳過。")
                print(f"{current_time}警告: 指定的腳本 '{script_name}' 在 '{SCRIPTS_DIR}' 中找不到，將跳過。")
    else:
        logging.info(f"{current_time}scheduler job 開始：掃描並執行所有更新腳本...")
        print(f"{current_time}scheduler job 開始：掃描並執行所有更新腳本...")
        scripts_to_run = []
        if not os.path.isdir(SCRIPTS_DIR):
            logging.error(f"{current_time}錯誤: 找不到腳本目錄 '{SCRIPTS_DIR}'")
            print(f"{current_time}錯誤: 找不到腳本目錄 '{SCRIPTS_DIR}'")
            return
        
        print(f"{current_time}正在從 '{SCRIPTS_DIR}' 目錄掃描腳本...")
        for filename in os.listdir(SCRIPTS_DIR):
            if filename.endswith(".py") and filename != "__init__.py":
                scripts_to_run.append(filename)
    
    if not scripts_to_run:
        if specific_scripts: # 如果是指定了腳本但都無效
            logging.warning(f"{current_time}所有指定的腳本都無效或找不到。沒有腳本被執行。")
            print(f"{current_time}警告: 所有指定的腳本都無效或找不到。沒有腳本被執行。")
        else: # 如果是掃描目錄但沒有找到腳本
            logging.info(f"{current_time}在 '{SCRIPTS_DIR}' 中沒有找到可執行的 .py 腳本。")
            print(f"{current_time}在 '{SCRIPTS_DIR}' 中沒有找到可執行的 .py 腳本。")
    else:
        if not specific_scripts:
             print(f"{current_time}找到 {len(scripts_to_run)} 個腳本準備執行: {scripts_to_run}")
        
        for script_file in scripts_to_run:
            run_script(script_file)
        
    logging.info(f"{current_time}所有已選定的更新腳本已處理完畢。scheduler job 結束。")
    print(f"{current_time}所有已選定的更新腳本已處理完畢。scheduler job 結束。")

def main():
    parser = argparse.ArgumentParser(description="執行 MySQL 資料表更新腳本。")
    parser.add_argument(
        "--only",
        nargs='+', # 接受一個或多個參數
        metavar="SCRIPT_NAME",
        help="指定只執行一個或多個特定的更新腳本檔案名稱 (例如: script1.py script2.py)。如果未提供，則執行 update_scripts 目錄中的所有腳本。"
    )
    args = parser.parse_args()

    logging.info("scheduler.py 被觸發執行。")
    if args.only:
        logging.info(f"參數 --only 被使用，準備執行指定的腳本: {args.only}")
        print(f"參數 --only 被使用，準備執行指定的腳本: {args.only}")
    job(specific_scripts=args.only) 
    logging.info("scheduler.py 執行完畢。")

if __name__ == "__main__":
    # 確保 update_scripts 的父目錄在 sys.path 中，使得 from update_scripts.script_name import ... 能夠運作
    # 假設 scheduler.py 在專案根目錄，update_scripts 是其子目錄
    project_root = os.path.dirname(os.path.abspath(__file__))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    
    # 為了讓 importlib.import_module(f"{SCRIPTS_DIR}.{script_name[:-3]}") 運作，
    # Python 需要能夠找到名為 "update_scripts" 的套件。
    # 這通常意味著 "update_scripts" 的父目錄需要在 sys.path 中，
    # 並且 "update_scripts" 本身可以被視為一個套件（例如，包含一個 __init__.py 檔案，即使是空的）。
    # 我們已將 project_root 加入 sys.path，如果 SCRIPTS_DIR 是 project_root 下的目錄，
    # 則 f"{SCRIPTS_DIR}.module" 形式的導入應該可以運作。
    main() 