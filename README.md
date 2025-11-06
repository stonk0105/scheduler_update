# MySQL 資料表自動更新專案

此專案旨在提供一個框架，用於排程自動更新 MySQL 資料庫中的多個資料表。每個資料表的更新邏輯都封裝在獨立的 Python 腳本中。

## 專案結構

```
.您的專案根目錄/
├── update_scripts/              # 存放所有資料表更新腳本的資料夾
│   ├── __init__.py              # 使 update_scripts 成為一個 Python 套件 (可選，但建議)
│   ├── update_table_example.py  # 一個更新資料表的範例腳本
│   ├── update_bb_B_Lead_Stat.py      # 更新中華職棒打者逐場成績 (bb_B_Lead) 和年度統計 (bb_B_Stat) 的腳本
│   ├── update_bb_P_Lead_Stat.py # 更新中華職棒投手逐場成績 (bb_P_Lead) 和年度統計 (bb_P_Stat) 的腳本
│   └── ...                      # 其他您的資料表更新腳本 .py 檔案
├── .env                         # (手動創建) 儲存資料庫連線、API路徑等敏感資訊
├── db_connection.py             # 處理資料庫連線的模組
├── scheduler.py                 # 主排程腳本，用於定時執行更新
├── requirements.txt             # 專案所需的 Python 套件
├── scheduler.log                # 主排程器的日誌檔案
└── README.md                    # 本說明檔案
```

## 設定步驟

1.  **安裝 Python**：
    確保您的系統已安裝 Python (建議 3.7 或更高版本)。

2.  **複製專案** (如果您是從版本控制系統獲取)：
    將專案複製到您的本地電腦。

3.  **安裝依賴套件**：
    打開終端機或命令提示字元，進入專案根目錄，然後執行：
    ```bash
    pip install -r requirements.txt
    ```

4.  **設定環境變數 (`.env` 檔案)**：
    在專案根目錄中，手動創建一個名為 `.env` 的檔案。複製以下內容到 `.env` 檔案中，並根據您的 MySQL 資料庫設定修改其值：
    ```ini
    DB_HOST=localhost
    DB_USER=your_mysql_username
    DB_PASSWORD=your_mysql_password
    DB_NAME=your_database_name
    DB_PORT=3306 # 如果您的 MySQL 使用非標準端口，請修改此行

    # === CPBL 數據更新腳本所需環境變數 ===
    # (適用於 update_bb_B_Lead_Stat.py 和 update_bb_P_Lead_Stat.py)
    # 這些腳本使用 Selenium 從 CPBL 網站獲取 API Token (儲存在 localStorage 的 CPBLToken_NFL)

    # ChromeDriver 的絕對路徑
    CHROME_DRIVER_PATH="C:\path\to\your\chromedriver.exe" # Windows 範例
    # CHROME_DRIVER_PATH="/usr/local/bin/chromedriver" # macOS/Linux 範例

    # Brave 瀏覽器執行檔的絕對路徑 (可選, 如果未提供或路徑無效，將嘗試使用預設 Chrome)
    # BRAVE_BROWSER_PATH="C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe" # Windows 範例
    # BRAVE_BROWSER_PATH="/Applications/Brave Browser.app/Contents/MacOS/Brave Browser" # macOS 範例

    # 要處理的數據年份 (例如 2023, 2024)
    UPDATE_LEAD_YEAR=2024
    ```
    **注意**：
    *   請務必將上述路徑 (`CHROME_DRIVER_PATH`, `BRAVE_BROWSER_PATH`) 和資料庫連線資訊替換為您的實際設定。
    *   請勿將 `.env` 檔案提交到版本控制系統 (例如 Git)，因為它包含敏感資訊。如果使用 Git，請將 `.env` 加入到 `.gitignore` 檔案中。

5.  **編寫您的資料表更新腳本**：
    -   在 `update_scripts` 資料夾中，為您需要更新的每一張資料表創建一個 `.py` 檔案。
    -   您可以參考 `update_scripts/update_table_example.py`、`update_scripts/update_bb_B_Lead_Stat.py` 或 `update_scripts/update_bb_P_Lead_Stat.py` 的結構。
    -   確保每個腳本中都有一個名為 `update_specific_table()` 的函數，其中包含該資料表的具體更新邏輯。
    -   每個腳本都會有自己的日誌檔案，記錄在 `update_scripts` 資料夾下 (例如 `update_table_example.log`, `update_bb_B_Lead_Stat.log` 等)。

## 如何執行

1.  **手動執行單個更新腳本** (用於測試)：
    您可以直接執行 `update_scripts` 資料夾中的某個腳本來測試其功能。執行前，請確保已在 `.env` 檔案中設定了該腳本所需的所有環境變數 (例如資料庫連線、`CHROME_DRIVER_PATH`、`UPDATE_LEAD_YEAR` 等)。
    ```bash
    # 範例：執行打者數據更新腳本
    python update_scripts/update_bb_B_Lead_Stat.py

    # 範例：執行投手數據更新腳本
    python update_scripts/update_bb_P_Lead_Stat.py
    ```

2.  **執行主更新管理器 (`scheduler.py`)**：
    要手動觸發一次所有資料表的更新，請在專案根目錄執行 `scheduler.py`：
    ```bash
    python scheduler.py
    ```
    這個腳本會立即執行 `update_scripts` 資料夾中的所有 `.py` 腳本一次，然後結束。它不會在背景持續運行。

    **執行特定的更新腳本**：
    如果您只想執行 `update_scripts` 資料夾中的一個或多個特定腳本，可以使用 `--only` 命令列選項：
    ```bash
    # 執行單個腳本 (例如 update_specific_table.py)
    python scheduler.py --only update_specific_table.py

    # 執行多個腳本 (例如 script1.py 和 script2.py)
    python scheduler.py --only script1.py script2.py
    ```
    您可以提供腳本的完整檔案名稱，或者腳本會嘗試自動附加 `.py` 副檔名（如果省略）。

## 在 Windows 中設定排程任務

要在 Windows 系統中設定此專案的自動排程執行 (例如，每天定時執行一次所有更新)，您可以使用「工作排程器」：

1.  **創建一個批次檔 (.bat)**：
    在您的專案根目錄中，創建一個名為 `run_all_updates.bat` (或任何您喜歡的名稱) 的檔案，內容如下：
    ```batch
    @echo off
    REM 將 "C:\path\to\your\python.exe" 替換為您 Python 直譯器的實際路徑
    REM 將 "C:\path\to\your\project\scheduler.py" 替換為您 scheduler.py 檔案的實際完整路徑
    
    set PYTHON_EXE="C:\Python39\python.exe"  REM <--- 修改這裡為您的 Python 路徑
    set SCHEDULER_SCRIPT="D:\path\to\your\project\folder\scheduler.py" REM <--- 修改這裡為您的 scheduler.py 路徑
    set PROJECT_DIR="D:\path\to\your\project\folder" REM <--- 修改這裡為您的專案根目錄路徑
    REM 確保 .env 檔案與 scheduler.py 在同一目錄，或者 scheduler.py 能夠找到 .env 檔案
    REM 如果 scheduler.py 和 .env 不在 PROJECT_DIR，您可能需要在 scheduler.py 中調整 dotenv 的載入路徑
    
    echo Starting all updates...
    cd /D %PROJECT_DIR%
    REM 直接執行 scheduler.py，它會完成所有更新然後結束
    %PYTHON_EXE% %SCHEDULER_SCRIPT%
    echo All updates finished.
    ```
    **重要**：
    *   請務必將批次檔中的路徑替換為您系統上 Python 直譯器 (`python.exe`) 和 `scheduler.py` 檔案的 **絕對路徑**。
    *   `cd /D %PROJECT_DIR%` 是為了確保腳本在正確的工作目錄下執行。

2.  **打開 Windows 工作排程器**：
    -   按 `Win + R`，輸入 `taskschd.msc`，然後按 Enter。

3.  **創建基本工作**：
    -   在右側「動作」窗格中，點擊「建立基本工作...」。
    -   輸入任務的「名稱」(例如 `MySQL自動更新`) 和「描述」(可選)，然後點擊「下一步」。

4.  **設定觸發程序**：
    -   選擇您希望任務執行的頻率 (例如「每天」、「每週」等)，然後點擊「下一步」。
    -   根據您的選擇設定具體的時間和日期，然後點擊「下一步」。

5.  **設定動作**：
    -   選擇「啟動程式」，然後點擊「下一步」。
    -   在「程式或指令碼」欄位中，瀏覽並選擇您在步驟1中創建的 `run_all_updates.bat` 檔案。
    -   「新增引數 (可選)」和「開始位置 (可選)」通常可以留空，因為我們已經在批次檔中處理了路徑問題。
    -   點擊「下一步」。

6.  **完成**：
    -   檢查設定摘要，如果一切正確，點擊「完成」。

7.  **(可選) 進階設定**：
    -   創建後，您可以在工作排程器程式庫中找到您的任務，右鍵點擊它並選擇「內容」來進行更進階的設定，例如：
        -   「一般」索引標籤：設定執行時使用的使用者帳戶 (例如，即使使用者未登入也執行)。
        -   「條件」索引標籤：設定只有在特定條件下 (例如電腦閒置時) 才執行。
        -   「設定」索引標籤：設定如果任務失敗的重試行為等。

## 日誌

-   主排程器的活動會記錄在專案根目錄的 `scheduler.log` 中。
-   每個單獨的更新腳本 (`update_scripts` 中的 `.py` 檔案) 會在 `update_scripts` 資料夾內生成自己的日誌檔案 (例如 `update_table_example.log`, `update_bb_B_Lead_Stat.log` 等)。

## 注意事項

-   確保您的 MySQL 伺服器正在運行，並且防火牆允許 Python 腳本進行連線。
-   定期檢查日誌檔案以監控更新過程和任何潛在錯誤。 