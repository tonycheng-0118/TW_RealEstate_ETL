---
name: tw-realestate-etl
description: >
  台灣實價登錄 ETL 工具。支援初始化、資料下載匯入、查詢、備份、排程管理。
  當使用者詢問不動產成交價格、房價行情、租金查詢、實價登錄相關操作時使用。
argument-hint: <指令> <參數> — 指令: help / init / run / query / status / backup / schedule
---

# 台灣實價登錄 ETL 工具

使用者輸入：**$ARGUMENTS**

## 操作路由

解析 `$ARGUMENTS` 的第一個詞，分派到對應操作：

- 無參數 或 `help` → **操作 0: help**
- `init` → **操作 1: init**
- `run` → **操作 2: run**
- `query` → **操作 3: query**
- `status` → **操作 4: status**
- `backup` → **操作 5: backup**
- `schedule` → **操作 6: schedule**

如果第一個詞不是上述指令，判斷是否為自然語言查詢（包含地名、房價、租金等關鍵字），
若是則視為 **操作 3: query**。否則顯示 help。

---

## 共用設定

```
SKILL_ROOT=~/.claude/skills/tw-realestate-etl
CONFIG_FILE=$SKILL_ROOT/config.json
```

### 前置檢查（操作 1 init 除外，其餘操作開始前都要執行）

1. 讀取 config.json：
   ```bash
   cat ~/.claude/skills/tw-realestate-etl/config.json
   ```
   如果不存在 → 告知使用者需先執行 `/tw-realestate-etl init`，**不要繼續**。

2. 從 config.json 讀取所有欄位值，用於後續操作。

3. 組合環境變數 prefix（呼叫 Python 腳本時使用）：
   ```bash
   REALPRICE_DB_HOST=<db_host> \
   REALPRICE_DB_PORT=<db_port> \
   REALPRICE_DB_NAME=<db_name> \
   REALPRICE_DB_USER=<db_user> \
   REALPRICE_DB_PASSWORD=<db_password> \
   REALPRICE_BACKUP_DIR=<backup_dir> \
   REALPRICE_BACKUP_KEEP_COUNT=<backup_keep_count> \
   REALPRICE_DATA_DIR=<data_dir> \
   REALPRICE_TARGET_CITIES=<target_cities joined by comma> \
   ```

---

## 操作 0: help

**觸發**: 無參數 或 `help`

直接輸出以下文字，不需要執行任何工具：

```
🏠 TW_RealEstate_ETL — 台灣實價登錄 ETL 工具

使用方式: /tw-realestate-etl <指令> [參數]

指令:
  help                          顯示此說明
  init                          初始化設定（DB、縣市、備份路徑）
  run <season> [city]           執行 ETL（下載 → 清洗 → 匯入 → 備份）
  query <描述>                  查詢實價登錄（自然語言）
  status                        查看 DB 狀態與匯入紀錄
  backup                        手動備份資料庫
  schedule <install|uninstall|status>  管理 macOS LaunchAgent 排程

範例:
  /tw-realestate-etl init
  /tw-realestate-etl run 114S1
  /tw-realestate-etl run 114S1 D
  /tw-realestate-etl run 112S1-114S1 D,A
  /tw-realestate-etl run current
  /tw-realestate-etl query 臺北市大安區忠孝東路近兩年大樓成交行情
  /tw-realestate-etl query 臺北市中正區透天厝均價
  /tw-realestate-etl status
  /tw-realestate-etl backup
  /tw-realestate-etl schedule install
  /tw-realestate-etl schedule status

縣市代碼:
  A=臺北市  B=臺中市  C=基隆市  D=臺南市  E=高雄市
  F=新北市  G=宜蘭縣  H=桃園市  I=嘉義市  J=新竹縣
  K=苗栗縣  L=臺東縣  M=花蓮縣  N=南投縣  O=新竹市
  P=雲林縣  Q=嘉義縣  R=屏東縣  S=彰化縣  V=澎湖縣
  W=金門縣  X=連江縣  all=全台
```

---

## 操作 1: init

**觸發**: `init` 或 config.json 不存在時引導

### Step 1: 檢查前置需求

```bash
which python3 && which psql && which pg_dump
```

- `python3` 不存在 → 提示安裝（`brew install python` 或從 python.org）
- `psql` / `pg_dump` 不存在 → 提示 `brew install postgresql@17`
- 全部存在 → 繼續

### Step 2: 安裝 Python 依賴

```bash
pip3 install pandas psycopg2-binary requests python-dotenv
```

### Step 3: 互動問答

用 Claude Code 對話方式收集以下資訊（每項都有預設值，使用者可直接 Enter 跳過）：

1. **預設縣市**（顯示代碼表，預設 `D`）
2. **DB 主機**（預設 `localhost`）
3. **DB 連接埠**（預設 `5432`）
4. **DB 名稱**（預設 `tw_realestate`）
5. **DB 使用者**（預設當前系統使用者）
6. **DB 密碼**（預設空字串）
7. **備份目錄**（預設 `~/.claude/skills/tw-realestate-etl/backups`）
8. **備份保留數量**（預設 `4`，0 = 全部保留）

### Step 4: 寫入 config.json

用 Write 工具將收集的設定寫入 `~/.claude/skills/tw-realestate-etl/config.json`：

```json
{
  "db_host": "localhost",
  "db_port": 5432,
  "db_name": "tw_realestate",
  "db_user": "<系統使用者>",
  "db_password": "",
  "target_cities": ["D"],
  "backup_dir": "~/.claude/skills/tw-realestate-etl/backups",
  "backup_keep_count": 4,
  "data_dir": "~/.claude/skills/tw-realestate-etl/data",
  "python_path": "<which python3 的結果>",
  "initialized_at": "<當前 ISO 時間>"
}
```

### Step 5: 建立資料庫

```bash
createdb <db_name> 2>/dev/null || echo "Database already exists"
psql -d <db_name> -f ~/.claude/skills/tw-realestate-etl/sql/schema.sql
```

### Step 6: 完成訊息

顯示初始化完成摘要，並提示下一步：
- `使用 /tw-realestate-etl run 114S1 開始匯入資料`
- `使用 /tw-realestate-etl help 查看所有指令`

---

## 操作 2: run

**觸發**: `run <season> [city]`

### 參數解析

從 `$ARGUMENTS` 中解析（去掉開頭的 `run`）：

- `run 114S1` → season=114S1, city=config 預設
- `run 114S1 D` → season=114S1, city=D
- `run 112S1-114S1 D,A` → from=112S1, to=114S1, city=D,A
- `run current` → current mode

### 執行 ETL

```bash
REALPRICE_DB_HOST=<db_host> \
REALPRICE_DB_PORT=<db_port> \
REALPRICE_DB_NAME=<db_name> \
REALPRICE_DB_USER=<db_user> \
REALPRICE_DB_PASSWORD=<db_password> \
REALPRICE_BACKUP_DIR=<backup_dir> \
REALPRICE_BACKUP_KEEP_COUNT=<backup_keep_count> \
REALPRICE_DATA_DIR=<data_dir> \
REALPRICE_TARGET_CITIES=<cities> \
python3 ~/.claude/skills/tw-realestate-etl/scripts/run_etl.py <flags>
```

其中 `<flags>` 根據參數組合：
- 單季：`--season 114S1`
- 範圍：`--from 112S1 --to 114S1`
- 當期：`--current`
- 指定縣市：加上 `--city D` 或 `--city D,A`

### 回報結果

顯示每個檔案的匯入狀態（成功/跳過/失敗）和筆數摘要。

---

## 操作 3: query

**觸發**: `query <自然語言查詢>`

### Step 1: 讀取 config.json 取得 DB 連線

從 config.json 取得 `db_name`、`db_user`、`db_host`、`db_port`。

### Step 2: 確認目標縣市資料是否已匯入

從使用者查詢中解析出目標**縣市**和**時間範圍**，查詢 etl_log：

```bash
psql -h <db_host> -p <db_port> -U <db_user> -d <db_name> -c "
  SELECT season, file_name, row_count, finished_at
  FROM etl_log
  WHERE status = 'success'
    AND file_name LIKE '<city_code>_%'
  ORDER BY finished_at DESC LIMIT 5;
"
```

**縣市代碼對照：**

| 代碼 | 縣市 | 代碼 | 縣市 | 代碼 | 縣市 |
|------|------|------|------|------|------|
| A | 臺北市 | J | 新竹縣 | R | 屏東縣 |
| B | 臺中市 | K | 苗栗縣 | S | 彰化縣 |
| C | 基隆市 | L | 臺東縣 | V | 澎湖縣 |
| D | 臺南市 | M | 花蓮縣 | W | 金門縣 |
| E | 高雄市 | N | 南投縣 | X | 連江縣 |
| F | 新北市 | O | 新竹市 | | |
| G | 宜蘭縣 | P | 雲林縣 | | |
| H | 桃園市 | Q | 嘉義縣 | | |
| I | 嘉義市 | | | | |

如果沒有資料 → 詢問使用者是否要先執行 `run` 下載。

### Step 3: 解析查詢意圖，組合 SQL

從使用者描述中解析：縣市、行政區、路段、建物型態、時間範圍。

**單位換算（回覆時一律提供坪數和萬元）：**
- m² → 坪：`m² / 3.30579`
- 元/m² → 元/坪：`元/m² × 3.30579`
- 總價元 → 萬元：`元 / 10000`

**排除特殊交易（非市場行情）：**
```sql
AND (note IS NULL OR (
    note NOT LIKE '%親友%'
    AND note NOT LIKE '%特殊關係%'
    AND note NOT LIKE '%受債權債務影響%'
    AND note NOT LIKE '%二親等%'
))
```

**淨房屋單價（扣除車位）：**
```sql
(total_price - COALESCE(parking_price, 0))
/ NULLIF(building_area - COALESCE(parking_area, 0), 0)
```

**建物型態關鍵字：**
- 透天厝：`building_type LIKE '%透天%'`
- 大樓（11F+）：`building_type LIKE '%住宅大樓%'`
- 華廈（7-10F）：`building_type LIKE '%華廈%'`
- 公寓（5F 以下無電梯）：`building_type LIKE '%公寓%'`

**門牌地址搜尋（GIN trigram 索引）：**
```sql
WHERE address LIKE '%忠孝東路%'
```

### Step 4: 執行 SQL 查詢

#### SQL 範例 — 買賣成交

```sql
SELECT
    transaction_date_ad AS 交易日期,
    address AS 門牌,
    building_type AS 建物型態,
    ROUND(building_area / 3.30579, 1) AS 坪數,
    rooms || '房' || halls || '廳' || bathrooms || '衛' AS 格局,
    total_price / 10000 AS 總價萬,
    ROUND(unit_price * 3.30579) AS 單價_坪,
    CASE WHEN parking_price > 0
        THEN ROUND((total_price - parking_price) / NULLIF(building_area - COALESCE(parking_area, 0), 0) * 3.30579)
        ELSE ROUND(unit_price * 3.30579)
    END AS 淨單價_坪,
    note AS 備註
FROM transactions
WHERE city_code = '<city_code>'
  AND district = '<district>'
  AND transaction_date_ad >= CURRENT_DATE - INTERVAL '2 years'
  AND (note IS NULL OR note NOT LIKE '%親友%')
ORDER BY transaction_date_ad DESC;
```

#### SQL 範例 — 租金行情

```sql
SELECT
    transaction_date_ad AS 交易日期,
    address AS 門牌,
    building_type AS 建物型態,
    ROUND(building_area / 3.30579, 1) AS 坪數,
    total_rent AS 月租金,
    ROUND(unit_rent * 3.30579) AS 單價_坪
FROM rentals
WHERE city_code = '<city_code>'
  AND district = '<district>'
  AND transaction_date_ad >= CURRENT_DATE - INTERVAL '1 year'
ORDER BY transaction_date_ad DESC;
```

執行查詢：
```bash
psql -h <db_host> -p <db_port> -U <db_user> -d <db_name> -c "<SQL>"
```

### Step 5: 整理回覆

查詢結果用**繁體中文**回覆，包含：
1. 列表或表格呈現成交紀錄
2. 行情摘要（均價、價格區間、筆數）
3. 注意事項（車位混合計價、特殊交易等）

---

## 操作 4: status

**觸發**: `status`

### 執行查詢

```bash
psql -h <db_host> -p <db_port> -U <db_user> -d <db_name> -c "
  SELECT 'transactions' AS table_name, COUNT(*) FROM transactions
  UNION ALL
  SELECT 'rentals', COUNT(*) FROM rentals;
"

psql -h <db_host> -p <db_port> -U <db_user> -d <db_name> -c "
  SELECT season, file_name, row_count, status, finished_at
  FROM etl_log ORDER BY finished_at DESC LIMIT 10;
"

ls -lh <backup_dir>/tw_realestate_*.sql.gz 2>/dev/null | head -5
```

### 回覆內容

- 各表筆數
- 最近 10 筆匯入紀錄
- 最新備份檔案（檔名、大小、時間）

---

## 操作 5: backup

**觸發**: `backup`

### 執行備份

```bash
REALPRICE_DB_HOST=<db_host> \
REALPRICE_DB_PORT=<db_port> \
REALPRICE_DB_NAME=<db_name> \
REALPRICE_DB_USER=<db_user> \
REALPRICE_DB_PASSWORD=<db_password> \
REALPRICE_BACKUP_DIR=<backup_dir> \
REALPRICE_BACKUP_KEEP_COUNT=<backup_keep_count> \
python3 ~/.claude/skills/tw-realestate-etl/scripts/backup.py
```

回報備份結果（檔案路徑、大小）。

---

## 操作 6: schedule

**觸發**: `schedule <install|uninstall|status>`

### schedule install

1. 讀取 config.json 取得 `python_path`（沒有則用 `which python3` 的結果）

2. 讀取 plist 模板：
   ```bash
   cat ~/.claude/skills/tw-realestate-etl/launchd/com.tw-realestate.etl.plist
   ```

3. 用 Write 工具產生新的 plist，替換模板中的路徑：
   - `/path/to/TW_RealEstate_ETL/.venv/bin/python` → `<python_path>`
   - `/path/to/TW_RealEstate_ETL/scripts/run_etl.py` → `~/.claude/skills/tw-realestate-etl/scripts/run_etl.py`（展開為絕對路徑）
   - `/path/to/TW_RealEstate_ETL` (WorkingDirectory) → `~/.claude/skills/tw-realestate-etl`（展開為絕對路徑）
   - `/path/to/TW_RealEstate_ETL/logs/` → `~/.claude/skills/tw-realestate-etl/logs/`（展開為絕對路徑）
   - 加入 `<key>EnvironmentVariables</key>` 區段，嵌入所有 `REALPRICE_*` 環境變數

4. 寫入：
   ```bash
   # Write 工具寫到 ~/Library/LaunchAgents/com.tw-realestate.etl.plist
   ```

5. 載入：
   ```bash
   launchctl load ~/Library/LaunchAgents/com.tw-realestate.etl.plist
   ```

6. 回報安裝成功，顯示排程時間（每月 2、12、22 日 03:00）。

### schedule uninstall

```bash
launchctl unload ~/Library/LaunchAgents/com.tw-realestate.etl.plist 2>/dev/null
rm -f ~/Library/LaunchAgents/com.tw-realestate.etl.plist
```

回報已移除。

### schedule status

```bash
launchctl list | grep tw-realestate
ls -la ~/Library/LaunchAgents/com.tw-realestate.etl.plist 2>/dev/null
```

回報排程狀態（已安裝/未安裝、上次執行狀態）。

---

## DB Schema 參考

### transactions 表（不動產買賣 + 預售屋）

| 欄位 | 型別 | 說明 |
|------|------|------|
| district | TEXT | 鄉鎮市區 |
| address | TEXT | 門牌（GIN trigram 索引） |
| target_type | TEXT | 交易標的 |
| transaction_date | TEXT | 民國年日期 e.g. 1130715 |
| transaction_date_ad | DATE | 西元日期 e.g. 2024-07-15 |
| building_type | TEXT | 建物型態 |
| building_area | NUMERIC | 建物移轉總面積 (m²) |
| main_area | NUMERIC | 主建物面積 (m²) |
| rooms / halls / bathrooms | INTEGER | 格局 |
| total_price | BIGINT | 總價 (元) |
| unit_price | NUMERIC | 單價 (元/m²) |
| parking_type | TEXT | 車位類別 |
| parking_area | NUMERIC | 車位面積 (m²) |
| parking_price | BIGINT | 車位總價 (元) |
| serial_no | TEXT | 編號 (唯一鍵) |
| note | TEXT | 備註 |
| source_season | TEXT | 資料季度 |
| city_code | CHAR(1) | 縣市代碼 |

### rentals 表（租賃）

結構同 transactions，價格欄位改為：
- `total_rent` BIGINT — 月租金 (元)
- `unit_rent` NUMERIC — 單價 (元/m²)

### etl_log 表

| 欄位 | 說明 |
|------|------|
| season | 季度 e.g. 113S4 |
| file_name | 來源檔名 e.g. j_lvr_land_a.csv |
| row_count | 匯入筆數 |
| status | success / failed |
| finished_at | 完成時間 |
