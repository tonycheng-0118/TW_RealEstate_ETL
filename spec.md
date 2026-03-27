# TW_RealEstate_ETL — 開發規格書

## 1. 專案概述

### 目標
建立自動化 pipeline，定期下載內政部實價登錄開放資料（Open Data），清洗後匯入本地 PostgreSQL，並透過 Claude Code Skill 以自然語言查詢不動產成交行情。

**Skill 發布目標**：將 TW_RealEstate_ETL 打包為自包含的 Claude Code Skill，使用者只需複製一個資料夾到 `~/.claude/skills/`，即可在 Claude Code 中完成所有操作（初始化、ETL、查詢、備份、排程管理）。不需要 git clone、不需要 pip install、不需要 npm。

### 使用情境
- 查詢特定地址/地號附近的歷史成交價格
- 分析某區域的房價趨勢與均價
- 比對租金行情
- 搭配地籍圖資做土地估價參考

### 使用者體驗（Skill 模式）

```bash
# 安裝（唯一步驟）
cp -r tw-realestate-etl/ ~/.claude/skills/

# 之後全部在 Claude Code 裡操作
claude
> /tw-realestate-etl                ← 顯示 help
> /tw-realestate-etl init
> /tw-realestate-etl run 114S1 A
> /tw-realestate-etl query 臺北市大安區忠孝東路近兩年大樓成交行情
> /tw-realestate-etl status
> /tw-realestate-etl backup
> /tw-realestate-etl schedule install
```

### 技術棧
- **語言**: Python 3.12+
- **資料庫**: PostgreSQL 17（透過 Homebrew 安裝）
- **排程**: macOS LaunchAgent
- **AI 查詢**: Claude Code Skill + MCP PostgreSQL Server（可選）
- **主要套件**: requests, pandas, psycopg2-binary

---

## 2. 系統架構

### Repo 模式（開發者）

```
[LaunchAgent 每月 2/12/22 日]
       │
       ▼
  run_etl.py  ← 唯一進入點，排程和 Claude Code 都呼叫它
       │
       ├─ Step 1: download    從 plvr.land.moi.gov.tw 下載 ZIP
       ├─ Step 2: transform   解壓、Big5→UTF8、型別轉換
       ├─ Step 3: load        Upsert 進 PostgreSQL
       └─ Step 4: backup      pg_dump + gzip → 備份目錄
                                 │
                                 ▼
                          [PostgreSQL]
                          localhost:5432
                          db: tw_realestate
                                 ▲
                                 │
  [Claude Code] ─── MCP ───────┘
```

### Skill 模式（一般使用者）

```
[使用者]
    │
    ▼
[Claude Code]
    │  /tw-realestate-etl <指令>
    ▼
[SKILL.md 路由]
    │
    ├─ init     → 互動問答 → 寫入 config.json → createdb + schema.sql
    ├─ run      → env vars + python run_etl.py
    ├─ query    → psql 執行 SQL
    ├─ status   → psql 查 etl_log + 筆數
    ├─ backup   → python backup.py
    └─ schedule → launchctl 管理
```

---

## 3. 資料來源

### 下載端點

| 類型 | URL | 說明 |
|------|-----|------|
| 按季歷史 | `https://plvr.land.moi.gov.tw/DownloadSeason?season={民國年}S{季}&type=zip&fileName=lvr_landcsv.zip` | 可直接用 requests 下載 |
| 當期 | `https://plvr.land.moi.gov.tw/Download?type=zip&fileName=lvr_landcsv.zip` | 可能需要 session，不一定能直接抓 |

### ZIP 內容結構

解壓後包含各縣市的 CSV，檔名格式：`{縣市代碼小寫}_lvr_land_{類型}.csv`

| 檔名模式 | 類型 | 目標 table |
|---------|------|-----------|
| `a_lvr_land_a.csv` | 不動產買賣 | transactions |
| `a_lvr_land_b.csv` | 預售屋買賣 | transactions（同表，用 source_file 區分） |
| `a_lvr_land_c.csv` | 租賃 | rentals |

其中 `a` = 臺北市。縣市代碼對照見下方 city_codes 表。

### CSV 編碼與格式
- **編碼**: Big5 (cp950)，部分新版可能是 UTF-8-sig
- **讀取順序**: 先嘗試 cp950 → big5 → utf-8-sig → utf-8
- **表頭**: 第 1 列為中文欄位名，部分版本第 2 列為英文欄位名（需跳過）
- **更新頻率**: 每月 1 日、11 日、21 日發布

---

## 4. 專案結構

### Repo 結構

```
TW_RealEstate_ETL/
├── README.md                 # 專案說明與快速開始指南
├── requirements.txt          # Python 依賴套件
├── config.py                 # 全域設定（雙模式：repo + skill）
├── build_skill.sh            # 打包腳本，產出 dist/tw-realestate-etl/
├── spec.md                   # 開發規格書（本文件）
├── .mcp.json                 # Claude Code MCP PostgreSQL 設定
├── scripts/
│   ├── __init__.py
│   ├── run_etl.py            # 主控腳本（唯一進入點）
│   ├── download.py           # Step 1: 下載 ZIP
│   ├── transform.py          # Step 2: 解壓 + 清洗 CSV
│   ├── load.py               # Step 3: Upsert 進 PostgreSQL
│   └── backup.py             # Step 4: pg_dump + gzip 備份
├── sql/
│   └── schema.sql            # PostgreSQL DDL（表、索引、觸發器）
├── launchd/
│   └── com.tw-realestate.etl.plist  # macOS LaunchAgent 排程模板
├── claude-skill/
│   ├── tw-realestate-etl/
│   │   └── SKILL.md          # 統一 Skill（7 操作：help/init/run/query/status/backup/schedule）
│   └── tw-realestate-query/
│       └── SKILL.md          # 舊版查詢 Skill（僅查詢功能）
├── tests/
│   └── test_transform.py     # 單元測試
├── logs/                     # 執行日誌（gitignore）
└── data/                     # 下載的 ZIP/CSV（gitignore）
```

### Skill 發布結構（打包後）

```
~/.claude/skills/tw-realestate-etl/
├── SKILL.md                    # 技能定義（唯一進入點）
├── config.json                 # init 時產生，存放使用者設定
├── scripts/
│   ├── __init__.py
│   ├── requirements.txt        # Python 依賴
│   ├── config.py               # Python 設定（讀取 config.json）
│   ├── download.py             # Step 1: 下載 ZIP
│   ├── transform.py            # Step 2: 解壓 + 清洗
│   ├── load.py                 # Step 3: Upsert PostgreSQL
│   ├── backup.py               # Step 4: pg_dump + gzip
│   └── run_etl.py              # 主控腳本（串接 1→2→3→4）
├── sql/
│   └── schema.sql              # PostgreSQL DDL
└── launchd/
    └── com.tw-realestate.etl.plist  # macOS LaunchAgent 模板
```

總大小約 50-60 KB，極輕量。

---

## 5. PostgreSQL Schema

### 5.1 transactions（不動產買賣 + 預售屋）

```sql
CREATE TABLE transactions (
    id                  UUID DEFAULT uuid_generate_v4() PRIMARY KEY,

    -- 位置
    district            TEXT,           -- 鄉鎮市區
    address             TEXT,           -- 土地區段位置建物區段門牌
    land_section        TEXT,           -- 解析出的地段名稱（ETL 時處理）
    land_number         TEXT,           -- 解析出的地號（ETL 時處理）

    -- 交易
    target_type         TEXT,           -- 交易標的（房地/土地/建物/車位）
    transaction_date    TEXT,           -- 民國年日期 e.g. 1130715
    transaction_date_ad DATE,           -- 西元日期 e.g. 2024-07-15
    transaction_note    TEXT,           -- 交易筆棟數

    -- 土地
    land_area           NUMERIC(12,2),  -- 土地移轉總面積（m²）
    zoning_urban        TEXT,           -- 都市土地使用分區
    zoning_non_urban    TEXT,           -- 非都市土地使用分區
    zoning_non_urban_cd TEXT,           -- 非都市土地使用編定

    -- 建物
    floor_transferred   TEXT,           -- 移轉層次
    total_floors        TEXT,           -- 總樓層數
    building_type       TEXT,           -- 建物型態
    main_purpose        TEXT,           -- 主要用途
    main_material       TEXT,           -- 主要建材
    build_complete_date TEXT,           -- 建築完成年月
    building_area       NUMERIC(12,2),  -- 建物移轉總面積（m²）
    main_area           NUMERIC(12,2),  -- 主建物面積（m²）
    sub_area            NUMERIC(12,2),  -- 附屬建物面積（m²）
    balcony_area        NUMERIC(12,2),  -- 陽台面積（m²）

    -- 格局
    rooms               INTEGER,        -- 房
    halls               INTEGER,        -- 廳
    bathrooms           INTEGER,        -- 衛
    has_partition       TEXT,            -- 隔間

    -- 管理
    has_management      TEXT,           -- 有無管理組織
    has_elevator        TEXT,           -- 電梯

    -- 價格
    total_price         BIGINT,         -- 總價（元）
    unit_price          NUMERIC(12,2),  -- 單價（元/m²）

    -- 車位
    parking_type        TEXT,           -- 車位類別
    parking_area        NUMERIC(12,2),  -- 車位面積（m²）
    parking_price       BIGINT,         -- 車位總價（元）

    -- 其他
    serial_no           TEXT,           -- 編號（唯一鍵，用於去重）
    transfer_no         TEXT,           -- 移轉編號
    note                TEXT,           -- 備註

    -- ETL metadata
    source_file         TEXT,           -- 來源檔名 e.g. a_lvr_land_a.csv
    source_season       TEXT,           -- 季度 e.g. 113S4
    city_code           CHAR(1),        -- 縣市代碼 e.g. A
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_serial_no UNIQUE (serial_no)
);
```

### 5.2 rentals（租賃）

結構類似 transactions，價格欄位改為：
- `total_rent` BIGINT — 月租金（元）
- `unit_rent` NUMERIC(12,2) — 單價（元/m²）

唯一鍵同樣用 `serial_no`。

### 5.3 etl_log（ETL 執行紀錄）

```sql
CREATE TABLE etl_log (
    id          SERIAL PRIMARY KEY,
    season      TEXT NOT NULL,          -- e.g. 113S4
    file_name   TEXT NOT NULL,          -- e.g. a_lvr_land_a.csv
    row_count   INTEGER,
    status      TEXT DEFAULT 'success', -- success / failed
    started_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_etl_season_file UNIQUE (season, file_name)
);
```

用途：避免重複匯入；可查詢資料最新更新時間。

### 5.4 city_codes（縣市代碼對照）

| code | name |
|------|------|
| A | 臺北市 |
| B | 臺中市 |
| C | 基隆市 |
| D | 臺南市 |
| E | 高雄市 |
| F | 新北市 |
| G | 宜蘭縣 |
| H | 桃園市 |
| ... | ... |

### 5.5 索引策略

| 索引 | 欄位 | 用途 |
|------|------|------|
| idx_txn_district | district | 按行政區查詢 |
| idx_txn_address | address (GIN + pg_trgm) | 門牌模糊查詢 |
| idx_txn_date_ad | transaction_date_ad | 日期範圍查詢 |
| idx_txn_building_type | building_type | 按建物型態查詢 |
| idx_txn_total_price | total_price | 價格範圍查詢 |
| idx_txn_land_section | land_section, land_number | 按地段地號查詢 |
| idx_txn_district_date | district, transaction_date_ad | 複合查詢（最常見） |

需要啟用 PostgreSQL 擴充：`uuid-ossp`、`pg_trgm`

---

## 6. CSV 欄位對照表

### lvr_land_a.csv（不動產買賣）中文 → DB column

```python
COLUMN_MAP_A = {
    "鄉鎮市區": "district",
    "交易標的": "target_type",
    "土地區段位置建物區段門牌": "address",
    "土地移轉總面積平方公尺": "land_area",
    "都市土地使用分區": "zoning_urban",
    "非都市土地使用分區": "zoning_non_urban",
    "非都市土地使用編定": "zoning_non_urban_cd",
    "交易年月日": "transaction_date",
    "交易筆棟數": "transaction_note",
    "移轉層次": "floor_transferred",
    "總樓層數": "total_floors",
    "建物型態": "building_type",
    "主要用途": "main_purpose",
    "主要建材": "main_material",
    "建築完成年月": "build_complete_date",
    "建物移轉總面積平方公尺": "building_area",
    "建物現況格局-房": "rooms",
    "建物現況格局-廳": "halls",
    "建物現況格局-衛": "bathrooms",
    "建物現況格局-隔間": "has_partition",
    "有無管理組織": "has_management",
    "總價元": "total_price",
    "單價元平方公尺": "unit_price",
    "車位類別": "parking_type",
    "車位移轉總面積平方公尺": "parking_area",
    "車位總價元": "parking_price",
    "備註": "note",
    "編號": "serial_no",
    "主建物面積": "main_area",
    "附屬建物面積": "sub_area",
    "陽台面積": "balcony_area",
    "電梯": "has_elevator",
    "移轉編號": "transfer_no",
}
```

---

## 7. ETL 流程細節

### 7.1 download.py

**功能**: 從 plvr.land.moi.gov.tw 下載 ZIP 壓縮檔

**CLI 介面**:
```bash
python scripts/download.py --season 113S4        # 單季
python scripts/download.py --from 112S1 --to 114S1  # 範圍
python scripts/download.py --current              # 當期
```

**邏輯**:
1. 組合下載 URL
2. 用 requests.get 下載
3. 驗證回傳內容是否為 ZIP（檢查 magic bytes `PK\x03\x04`）
4. 儲存到 `data/{season}.zip`
5. 如檔案已存在則跳過
6. 每次下載間隔 10 秒（`DOWNLOAD_DELAY_SEC`），避免被封鎖

**季度範圍產生**: `parse_season_range("112S1", "113S2")` → `["112S1", "112S2", "112S3", "112S4", "113S1", "113S2"]`

### 7.2 transform.py

**功能**: 解壓 ZIP、讀取 CSV、清洗與型別轉換

**邏輯**:
1. 解壓 ZIP 到 `data/{season}/` 資料夾
2. 列出所有 CSV，根據檔名判斷類型（a=買賣、b=預售、c=租賃）
3. 根據 `config.TARGET_CITY_CODES` 篩選目標縣市（預設 `["A"]` 只抓臺北）
4. 讀取 CSV（自動偵測 Big5/UTF-8 編碼）
5. 用 `COLUMN_MAP` 將中文欄位名 rename 為英文
6. 型別轉換:
   - 民國年日期 → 西元 DATE（`roc_date_to_ad`: "1130715" → 2024-07-15）
   - 面積/單價 → NUMERIC（`safe_numeric`）
   - 總價/車位價 → BIGINT（`safe_int`）
   - 房/廳/衛 → INTEGER（`safe_int`）
7. 過濾掉 serial_no 為空的列
8. 加入 metadata 欄位：source_file、source_season、city_code

### 7.3 load.py

**功能**: 將 DataFrame upsert 進 PostgreSQL

**邏輯**:
1. `check_already_loaded()`: 查 etl_log 判斷該季度/檔案是否已成功匯入，已匯入則跳過
2. `upsert_transactions()`: INSERT ... ON CONFLICT (serial_no) DO UPDATE
3. `upsert_rentals()`: 同上，寫入 rentals 表
4. 每 500 筆為一批次（executemany）
5. NaN 轉為 None（PostgreSQL NULL）
6. `log_etl()`: 寫入 etl_log 紀錄

### 7.4 run_etl.py

**功能**: 主控腳本，串接 download → transform → load → backup。排程和 Claude Code 都透過這支腳本操作。

**CLI 介面**:
```bash
python scripts/run_etl.py --season 113S4            # 單季 ETL + 備份
python scripts/run_etl.py --from 112S1 --to 114S1   # 範圍 ETL + 備份
python scripts/run_etl.py --current                  # 當期 ETL + 備份
python scripts/run_etl.py --backup-only              # 只執行備份，不跑 ETL
python scripts/run_etl.py --season 114S1 --city A    # 指定縣市
python scripts/run_etl.py --from 111S1 --to 112S4 --city A,F  # 多縣市
```

**流程**:
```
run_etl.py
  │
  ├─ 對每個 season:
  │   1. download_season(season) → ZIP
  │   2. process_season(season)  → {key: DataFrame}
  │   3. 對每個 DataFrame:
  │      - check_already_loaded → 已匯入則 skip
  │      - 判斷類型 → upsert_transactions 或 upsert_rentals
  │      - log_etl 記錄結果
  │
  └─ 全部 season 處理完成後:
      4. backup_database() → pg_dump + gzip → BACKUP_DIR
```

### 7.5 backup.py

**功能**: ETL 完成後自動產生 PostgreSQL 備份壓縮檔

**邏輯**:
1. 呼叫 `pg_dump -d tw_realestate` 透過 subprocess 執行
2. 輸出透過 gzip 壓縮為 `.sql.gz`
3. 檔名格式: `tw_realestate_{YYYYMMDD}.sql.gz`
4. 存放路徑: `config.BACKUP_DIR`（可設定為雲端同步資料夾）
5. 清理舊備份：本地只保留最近 `config.BACKUP_KEEP_COUNT` 份（預設 4），舊的自動刪除
6. 備份目錄可由使用者設定為雲端同步資料夾，`backup.py` 本身不處理上傳，只負責把檔案放到正確位置

**重要設計**:
- 備份是 `run_etl.py` 的最後一步，ETL 失敗時不會產生備份，確保備份永遠對應一致的資料庫狀態
- `--backup-only` 模式可以跳過 ETL 直接備份，供 Claude Code skill 手動觸發
- 還原方式: `gunzip -c tw_realestate_20260327.sql.gz | psql -d tw_realestate`

---

## 8. config.py 設定項

### 雙模式設計

config.py 支援兩種目錄佈局：

| 模式 | config.py 位置 | 偵測方式 |
|------|---------------|---------|
| Repo | 專案根目錄 | `_this_dir.name != "scripts"` |
| Skill | `scripts/` 子資料夾內 | `_this_dir.name == "scripts"` |

### 設定來源優先順序

```
config.json（skill 模式主要設定源）
    ↓ 不存在時
環境變數（REALPRICE_* 前綴）
    ↓ 不存在時
預設值
```

```python
def _get(key: str, env_key: str, default):
    """config.json → env var → default"""
    if key in _cfg:
        return _cfg[key]
    return os.environ.get(env_key, default)
```

### config.json 設計

由 `init` 操作產生，存放在 skill 資料夾內，所有操作都讀這個檔案。

```json
{
  "db_host": "localhost",
  "db_port": 5432,
  "db_name": "tw_realestate",
  "db_user": "yourname",
  "db_password": "",
  "target_cities": ["A"],
  "backup_dir": "~/.claude/skills/tw-realestate-etl/backups",
  "backup_keep_count": 4,
  "data_dir": "~/.claude/skills/tw-realestate-etl/data",
  "python_path": "/usr/bin/python3",
  "initialized_at": "2026-03-28T15:30:00"
}
```

**特性**:
- 路徑欄位支援 `~`（scripts 內展開為絕對路徑）
- `python_path` 在 init 時自動偵測（`which python3`）
- 如果 config.json 不存在，SKILL.md 指示 Claude Code 引導使用者執行 init

### 設定項一覽

| 設定 | config.json key | 環境變數 | 預設值 | 說明 |
|------|----------------|---------|--------|------|
| DB host | `db_host` | `REALPRICE_DB_HOST` | `localhost` | PostgreSQL 主機 |
| DB port | `db_port` | `REALPRICE_DB_PORT` | `5432` | PostgreSQL 埠 |
| DB name | `db_name` | `REALPRICE_DB_NAME` | `tw_realestate` | 資料庫名稱 |
| DB user | `db_user` | `REALPRICE_DB_USER` | `$USER` | 系統使用者名稱 |
| DB password | `db_password` | `REALPRICE_DB_PASSWORD` | `""` | 本地端通常免密碼 |
| 資料目錄 | `data_dir` | `REALPRICE_DATA_DIR` | `./data` | ZIP/CSV 暫存目錄 |
| 備份目錄 | `backup_dir` | `REALPRICE_BACKUP_DIR` | `./backups` | 備份檔存放路徑 |
| 備份保留數 | `backup_keep_count` | `REALPRICE_BACKUP_KEEP_COUNT` | `4` | 保留最近 N 份，0=全部保留 |
| 目標縣市 | `target_cities` | `REALPRICE_TARGET_CITIES` | `["A"]` | 篩選縣市，`None`/`"all"`=全台 |
| 下載間隔 | `download_delay` | `REALPRICE_DOWNLOAD_DELAY` | `10` | 下載間隔秒數 |
| 批次大小 | `batch_size` | `REALPRICE_BATCH_SIZE` | `500` | Upsert 批次筆數 |

---

## 9. Claude Code Skill 整合

### 9.1 Skill 架構

SKILL.md 是唯一進入點，包含 7 個操作，Claude Code 根據使用者輸入判斷要執行哪一個。

#### 操作 0: help — 顯示使用說明

**觸發**: `/tw-realestate-etl help` 或無參數

直接輸出使用說明文字，不需要執行任何指令。

#### 操作 1: init — 初始化設定

**觸發**: `/tw-realestate-etl init` 或首次使用時 config.json 不存在

**流程**:
```
Step 1: 檢查前置需求
  ├─ which python3     → 沒有則提示安裝
  ├─ which psql        → 沒有則提示 brew install postgresql@17
  └─ which pg_dump     → 同上

Step 2: 安裝 Python 依賴
  pip install pandas psycopg2-binary requests python-dotenv

Step 3: 互動問答（Claude Code 直接對話）
  ├─ 選擇預設縣市（顯示代碼表）
  ├─ DB 設定（host/port/name/user，都有預設值）
  └─ 備份目錄

Step 4: 寫入 config.json

Step 5: 建立資料庫
  createdb <db_name>
  psql -d <db_name> -f <SKILL_ROOT>/sql/schema.sql

Step 6: 顯示完成訊息與下一步指引
```

**重要**: init 的互動問答不是由 Python script 處理，而是由 Claude Code 自己跟使用者對話，收集答案後組成 config.json 寫入。

#### 操作 2: run — 執行 ETL

**觸發**: `/tw-realestate-etl run <season> [city]`

**範例**:
```
/tw-realestate-etl run 114S1          ← 用 config.json 裡的預設縣市
/tw-realestate-etl run 114S1 A        ← 指定臺北
/tw-realestate-etl run 112S1-114S1 A,F ← 範圍 + 多縣市
/tw-realestate-etl run current        ← 當期
```

**流程**:
1. 讀取 config.json → 不存在則引導 init
2. 組合環境變數 prefix + 呼叫 `python run_etl.py --season <season> --city <city>`
3. 回報結果

#### 操作 3: query — 查詢實價登錄

**觸發**: `/tw-realestate-etl query <自然語言查詢>`

**範例**:
```
/tw-realestate-etl query 臺北市大安區忠孝東路近兩年大樓成交行情
/tw-realestate-etl query 臺北市中正區透天厝均價
```

**流程**:
1. 讀取 config.json 取得 DB 連線
2. 確認目標縣市資料已匯入（查 etl_log）
3. 解析查詢意圖 → 組合 SQL（含單位換算、排除特殊交易、扣除車位計算淨單價）
4. 執行 psql 查詢
5. 用繁體中文回覆（表格 + 行情摘要 + 注意事項）

#### 操作 4: status — 查看狀態

**觸發**: `/tw-realestate-etl status`

查詢並回報：各表筆數、最近匯入紀錄、最新備份檔案。

#### 操作 5: backup — 手動備份

**觸發**: `/tw-realestate-etl backup`

呼叫 `python backup.py`，回報備份結果。

#### 操作 6: schedule — 排程管理

**觸發**: `/tw-realestate-etl schedule <install|uninstall|status>`

- **install**: 讀 plist 模板 → 替換路徑 + 嵌入環境變數 → 寫入 `~/Library/LaunchAgents/` → `launchctl load`
- **uninstall**: `launchctl unload` → 刪除 plist
- **status**: `launchctl list | grep tw-realestate`

### 9.2 環境變數傳遞機制

SKILL.md 指示 Claude Code 在呼叫 Python 腳本前，從 config.json 讀取值並組成環境變數前綴：

```bash
REALPRICE_DB_HOST=localhost \
REALPRICE_DB_PORT=5432 \
REALPRICE_DB_NAME=tw_realestate \
REALPRICE_DB_USER=yourname \
REALPRICE_BACKUP_DIR=~/.claude/skills/tw-realestate-etl/backups \
REALPRICE_TARGET_CITIES=A \
python ~/.claude/skills/tw-realestate-etl/scripts/run_etl.py --season 114S1
```

Python 腳本內的 config.py 同時支援 config.json 和環境變數，兩種方式都能正常運作。

### 9.3 MCP 設定（可選）

在專案 `.mcp.json` 或全域設定中加入：

```json
{
  "mcpServers": {
    "postgres": {
      "command": "npx",
      "args": [
        "-y",
        "@modelcontextprotocol/server-postgres",
        "postgresql://localhost:5432/tw_realestate"
      ]
    }
  }
}
```

在純 Skill 方案中 MCP 非必要，query 操作用 `psql` CLI 即可。但如果使用者的環境有 MCP 可用，query 可走 MCP 查詢。

### 9.4 使用方式

```bash
# 互動模式（Skill）
claude
> /tw-realestate-etl query 臺北市大安區忠孝東路近兩年大樓成交行情

# 非互動模式（可串接 LINE bot）
claude -p "臺北市大安區建國段100地號附近的實價登錄"
```

---

## 10. 排程（macOS LaunchAgent）

使用 macOS 原生的 `launchd` 排程（LaunchAgent）。

### plist 模板

存放於 `launchd/com.tw-realestate.etl.plist`，包含 `/path/to/` 佔位符，由 `schedule install` 操作自動替換。

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.tw-realestate.etl</string>

    <key>ProgramArguments</key>
    <array>
        <string>/path/to/python</string>
        <string>/path/to/TW_RealEstate_ETL/scripts/run_etl.py</string>
        <string>--current</string>
    </array>

    <key>WorkingDirectory</key>
    <string>/path/to/TW_RealEstate_ETL</string>

    <!-- 每月 2/12/22 日凌晨 3 點執行 -->
    <key>StartCalendarInterval</key>
    <array>
        <dict>
            <key>Day</key><integer>2</integer>
            <key>Hour</key><integer>3</integer>
            <key>Minute</key><integer>0</integer>
        </dict>
        <dict>
            <key>Day</key><integer>12</integer>
            <key>Hour</key><integer>3</integer>
            <key>Minute</key><integer>0</integer>
        </dict>
        <dict>
            <key>Day</key><integer>22</integer>
            <key>Hour</key><integer>3</integer>
            <key>Minute</key><integer>0</integer>
        </dict>
    </array>

    <key>StandardOutPath</key>
    <string>/path/to/TW_RealEstate_ETL/logs/etl.stdout.log</string>

    <key>StandardErrorPath</key>
    <string>/path/to/TW_RealEstate_ETL/logs/etl.stderr.log</string>

    <key>TimeOut</key>
    <integer>600</integer>
</dict>
</plist>
```

### 安裝與管理

```bash
# 載入排程
launchctl load ~/Library/LaunchAgents/com.tw-realestate.etl.plist

# 卸載排程
launchctl unload ~/Library/LaunchAgents/com.tw-realestate.etl.plist

# 手動觸發一次（測試用）
launchctl start com.tw-realestate.etl

# 確認狀態
launchctl list | grep tw-realestate
```

### 設計原則
- LaunchAgent 只做一件事：呼叫 `run_etl.py --current`
- 不寫任何業務邏輯，所有邏輯封裝在 `run_etl.py` 內部
- 手動執行或透過 Skill 觸發，行為完全一致
- stdout/stderr 分開記錄到 `logs/` 目錄

---

## 11. 單位換算速查

| 項目 | 公式 |
|------|------|
| m² → 坪 | m² ÷ 3.30579 |
| 坪 → m² | 坪 × 3.30579 |
| 元/m² → 元/坪 | 元/m² × 3.30579 |
| 總價元 → 總價萬 | 元 ÷ 10000 |

---

## 12. 已知限制與注意事項

1. **免費版無座標**: Open Data 不含經緯度，地點比對只能用門牌/地段地號文字匹配。如需座標，可用 TGOS Geocoding API 補上（參考 github.com/zbryikt/realestate）。

2. **CSV 編碼**: 歷史資料大多為 Big5 (cp950)，需讀取時指定編碼。

3. **欄位可能變動**: 內政部偶爾會調整 CSV 欄位順序或新增欄位，transform.py 應只取 COLUMN_MAP 中存在的欄位，對缺少的欄位容錯處理。

4. **當期下載限制**: DownloadOpenData 頁面可能需要先在瀏覽器同意授權才能下載。按季歷史資料（DownloadSeason）則可直接用 requests 抓。

5. **去重邏輯**: 不同期發布的資料可能包含重複案件，用 `serial_no` 做 upsert 去重。

6. **特殊交易**: 備註欄含「親友交易」「特殊關係」「受債權債務影響」等非一般市場交易，查詢時建議排除。

7. **車位混合計價**: 部分交易的總價含車位，計算淨房屋單價時應扣除：`(total_price - parking_price) / (building_area - parking_area)`。

8. **歷史資料量**: 全台自 101 年起約數百萬筆。只抓單一縣市的話，預估幾十萬筆，一般桌機完全沒問題。

---

## 13. Skill 打包與發布

### 從 repo 打包 skill

```bash
./build_skill.sh
```

腳本將需要的檔案組裝到 `dist/tw-realestate-etl/`：
- `SKILL.md` ← 從 `claude-skill/tw-realestate-etl/`
- `scripts/*.py` + `config.py`（root → scripts/）
- `sql/schema.sql`
- `launchd/*.plist`

### GitHub Release 發布

```bash
# 打包
./build_skill.sh
cd dist && zip -r tw-realestate-etl.zip tw-realestate-etl/

# 在 GitHub Release 頁面上傳 tw-realestate-etl.zip
# 使用者下載後：
unzip tw-realestate-etl.zip -d ~/.claude/skills/
```

### 向後相容設計

config.py 的雙模式偵測確保兩種佈局都能正常運作：

| 場景 | config.py 位置 | `_this_dir.name` | 行為 |
|------|---------------|-------------------|------|
| Repo 開發 | 專案根目錄 | `TW_RealEstate_ETL` | 讀 `.env` → env vars → defaults |
| Skill 安裝 | `scripts/` 內 | `scripts` | 讀 `config.json` → env vars → defaults |

run_etl.py 使用 try/except import 同時支援 package-qualified（repo）和 direct（skill）兩種 import 路徑。

---

## 14. 開發步驟

### Phase 1: 基礎建設
1. 安裝 PostgreSQL 17
2. 建立 tw_realestate database 並執行 schema.sql
3. 實作 config.py

### Phase 2: ETL 核心
4. 實作 download.py，先用 `--season 114S1` 測試單季下載
5. 實作 transform.py，確認 Big5 讀取和型別轉換正確
6. 實作 load.py，確認 upsert 和 etl_log 正常運作
7. 實作 run_etl.py 串接全流程
8. 用 `--from 112S1 --to 114S1` 批次匯入近 2-3 年資料

### Phase 3: Claude Code 整合
9. 設定 MCP PostgreSQL Server
10. 安裝查詢 SKILL.md
11. 測試自然語言查詢

### Phase 4: 自動化與備份
12. 實作 backup.py（pg_dump + gzip + 清理舊檔）
13. 將 backup 整合進 run_etl.py 作為最後一步
14. 設定 `BACKUP_DIR` 指向備份目錄
15. 建立 LaunchAgent plist，`launchctl load` 啟用排程

### Phase 5: Skill 打包發布
16. 擴充 SKILL.md 為 7 操作（init/run/query/status/backup/schedule）
17. 修改 config.py 支援 config.json + 雙模式偵測
18. 修改 run_etl.py 支援雙模式 import
19. 寫 build_skill.sh
20. 測試完整流程：乾淨環境 → cp skill → init → run → query
21. 在 GitHub Release 發布 zip
22. 更新 README 加入 skill 安裝說明
