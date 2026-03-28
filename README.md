![CI](https://github.com/tonycheng-0118/TW_RealEstate_ETL/actions/workflows/ci.yml/badge.svg)

# TW_RealEstate_ETL

台灣內政部**實價登錄**開放資料自動化 ETL pipeline。

定期從[內政部不動產成交案件實際資訊資料供應系統](https://plvr.land.moi.gov.tw/)下載歷史及當期 CSV（Big5 / UTF-8），清洗後匯入 PostgreSQL，並透過 [Claude Code](https://claude.ai/claude-code) MCP 以自然語言查詢不動產成交行情。

## 功能特色

- **全自動 ETL**：下載 → 解壓 → 編碼轉換 → 型別清洗 → Upsert → 備份，一行指令搞定
- **增量匯入**：以 `serial_no` 做 upsert 去重，重複執行不會產生重複資料
- **Big5 / UTF-8 自動偵測**：相容歷史檔案（cp950）與新版 UTF-8-sig
- **民國年 → 西元自動轉換**：`1130715` → `2024-07-15`
- **排程支援**：macOS LaunchAgent，每月 2 / 12 / 22 日自動執行
- **Claude Code 整合**：透過 MCP PostgreSQL Server，直接用自然語言查詢資料庫

## 資料來源

| 類型 | 端點 | 說明 |
|------|------|------|
| 歷史按季 | `plvr.land.moi.gov.tw/DownloadSeason?season={season}` | 101 年起，可直接 `requests.get` |
| 當期 | `plvr.land.moi.gov.tw/Download?type=zip&fileName=lvr_landcsv.zip` | 每月 1、11、21 日更新 |

ZIP 內含各縣市 CSV，檔名格式：`{縣市代碼小寫}_lvr_land_{類型}.csv`

| 後綴 | 類型 | 目標表 |
|------|------|--------|
| `a` | 不動產買賣 | `transactions` |
| `b` | 預售屋買賣 | `transactions`（同表，以 `source_file` 區分） |
| `c` | 租賃 | `rentals` |

> 編碼：歷史資料多為 Big5 (cp950)，部分新版為 UTF-8-sig。程式會依序嘗試 `cp950 → big5 → utf-8-sig → utf-8`。

## 系統架構

```
[LaunchAgent 每月 2/12/22 日]
       │
       ▼
  run_etl.py  ← 唯一進入點
       │
       ├─ Step 1: download    從 plvr.land.moi.gov.tw 下載 ZIP
       ├─ Step 2: transform   解壓、Big5→UTF8、型別轉換
       ├─ Step 3: load        Upsert 進 PostgreSQL
       └─ Step 4: backup      pg_dump + gzip → 備份目錄
                                 │
                                 ▼
                           [PostgreSQL]
                           tw_realestate
                                 ▲
                                 │
  [Claude Code] ─── MCP ───────┘
```

## 快速開始

### 前置需求

- macOS（已於 Mac mini M4 驗證）
- Python 3.12+
- PostgreSQL 17（`brew install postgresql@17`）
- Node.js（MCP server 需要 `npx`）

### 安裝

```bash
# 1. Clone & 建立虛擬環境
git clone https://github.com/<your-username>/TW_RealEstate_ETL.git
cd TW_RealEstate_ETL
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. 設定環境變數
cp .env.template .env
# 編輯 .env，填入你的本機設定（DB 帳號、備份路徑等）

# 3. 啟動 PostgreSQL 並建立資料庫
brew services start postgresql@17
createdb tw_realestate

# 4. 建立 Schema
psql -d tw_realestate -f sql/schema.sql
```

### 執行 ETL

```bash
# 單季
python scripts/run_etl.py --season 114S1

# 批次匯入（範圍）
python scripts/run_etl.py --from 112S1 --to 114S1

# 指定縣市（覆蓋 config.py 預設值）
python scripts/run_etl.py --season 114S1 --city J

# 多縣市
python scripts/run_etl.py --from 111S1 --to 112S4 --city J,H

# 全台
python scripts/run_etl.py --season 114S1 --city all

# 當期資料
python scripts/run_etl.py --current

# 只執行備份
python scripts/run_etl.py --backup-only

# ETL 完不備份
python scripts/run_etl.py --season 114S1 --skip-backup
```

### 啟用排程（macOS LaunchAgent）

```bash
# 先編輯 plist，將 /path/to/TW_RealEstate_ETL 替換為你的實際路徑
cp launchd/com.tw-realestate.etl.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.tw-realestate.etl.plist

# 手動觸發一次（測試用）
launchctl start com.tw-realestate.etl

# 確認狀態
launchctl list | grep tw-realestate
```

## 專案結構

```
TW_RealEstate_ETL/
├── config.py                 # 全域設定（DB、URL、欄位對照、路徑）
├── requirements.txt          # Python 依賴
├── .env.template             # 環境變數範本（複製為 .env 後填入本機設定）
├── spec.md                   # 開發規格書（完整設計文件）
├── .mcp.json                 # Claude Code MCP PostgreSQL 設定
├── scripts/
│   ├── run_etl.py            # 主控腳本（唯一進入點）
│   ├── download.py           # Step 1: 下載 ZIP
│   ├── transform.py          # Step 2: 解壓 + 清洗 CSV
│   ├── load.py               # Step 3: Upsert PostgreSQL
│   └── backup.py             # Step 4: pg_dump + gzip 備份
├── sql/
│   └── schema.sql            # PostgreSQL DDL（表、索引、觸發器）
├── launchd/
│   └── com.tw-realestate.etl.plist  # macOS LaunchAgent 排程
├── build_skill.sh            # Skill 打包腳本（產出 dist/tw-realestate-etl/）
├── claude-skill/
│   └── tw-realestate-etl/
│       └── SKILL.md          # Claude Code 統一技能（7 操作：help/init/run/query/status/backup/schedule）
├── tests/
│   └── test_transform.py     # 單元測試
├── logs/                     # 執行日誌（gitignore）
└── data/                     # 下載的 ZIP/CSV（gitignore）
```

## 資料庫 Schema

> 完整 DDL 見 `sql/schema.sql`，需啟用 `uuid-ossp` 及 `pg_trgm` 擴充。

### transactions（不動產買賣 + 預售屋）

| 分類 | 欄位 | 型別 | 說明 |
|------|------|------|------|
| 位置 | `district` | TEXT | 鄉鎮市區 |
| | `address` | TEXT | 門牌（GIN trigram 索引，支援模糊搜尋） |
| | `land_section` | TEXT | 地段名稱 |
| | `land_number` | TEXT | 地號 |
| 交易 | `transaction_date` | TEXT | 民國年日期，如 `1130715` |
| | `transaction_date_ad` | DATE | 西元日期，如 `2024-07-15` |
| | `target_type` | TEXT | 交易標的（房地/土地/建物/車位） |
| 建物 | `building_type` | TEXT | 透天厝、住宅大樓、公寓、華廈… |
| | `building_area` | NUMERIC | 建物移轉總面積（m²） |
| | `rooms` / `halls` / `bathrooms` | INTEGER | 格局 |
| 價格 | `total_price` | BIGINT | 總價（元） |
| | `unit_price` | NUMERIC | 單價（元/m²） |
| 車位 | `parking_price` | BIGINT | 車位總價（元） |
| | `parking_area` | NUMERIC | 車位面積（m²） |
| 唯一鍵 | `serial_no` | TEXT | 編號，用於 upsert 去重 |

### rentals（租賃）

結構同 transactions，價格欄位改為：
- `total_rent` BIGINT — 月租金（元）
- `unit_rent` NUMERIC — 單價（元/m²）

### etl_log（ETL 紀錄）

追蹤每次匯入的季度、檔案、筆數、狀態。`(season, file_name)` 為唯一鍵，防止重複匯入。

## 設定

所有設定集中在 `config.py`，支援環境變數覆寫（前綴 `REALPRICE_`）。

複製 `.env.template` 為 `.env` 填入本機設定，`config.py` 啟動時會透過 `python-dotenv` 自動載入：

```bash
cp .env.template .env
```

| 設定 | 預設值 | 環境變數 |
|------|--------|----------|
| DB host | `localhost` | `REALPRICE_DB_HOST` |
| DB port | `5432` | `REALPRICE_DB_PORT` |
| DB name | `tw_realestate` | `REALPRICE_DB_NAME` |
| DB user | `$USER` | `REALPRICE_DB_USER` |
| 目標縣市 | `["A"]`（臺北） | — |
| 下載間隔 | `10` 秒 | `REALPRICE_DOWNLOAD_DELAY` |
| 備份路徑 | `./backups` | `REALPRICE_BACKUP_DIR` |
| 備份保留數 | `0`（全部保留） | `REALPRICE_BACKUP_KEEP_COUNT` |
| 批次大小 | `500` | `REALPRICE_BATCH_SIZE` |

### 縣市代碼表

在 `config.py` 的 `TARGET_CITY_CODES` 中填入需要的代碼，設為 `None` 則下載全台：

| 代碼 | 縣市 | 代碼 | 縣市 | 代碼 | 縣市 |
|------|------|------|------|------|------|
| A | 臺北市 | J | 新竹縣 | R | 屏東縣 |
| B | 臺中市 | K | 苗栗縣 | S | 彰化縣 |
| C | 基隆市 | L | 臺東縣 | T | 臺東縣 |
| D | 臺南市 | M | 花蓮縣 | U | 花蓮縣 |
| E | 高雄市 | N | 南投縣 | V | 澎湖縣 |
| F | 新北市 | O | 新竹市 | W | 金門縣 |
| G | 宜蘭縣 | P | 雲林縣 | X | 連江縣 |
| H | 桃園市 | Q | 嘉義縣 | | |
| I | 嘉義市 | | | | |

## Claude Code Skill 安裝

本專案提供自包含的 Claude Code Skill，安裝後即可在 Claude Code 中完成所有操作（初始化、ETL、查詢、備份、排程），**不需要 git clone、pip install、npm**。

### 方法一：從 Release 安裝（推薦）

到 [Releases](https://github.com/tonycheng-0118/TW_RealEstate_ETL/releases) 頁面下載 **`tw-realestate-etl.zip`**（Assets 區的附件，不是 Source code）：

```bash
# 解壓到 skills 目錄（zip 內已含 tw-realestate-etl/ 資料夾）
unzip tw-realestate-etl.zip -d ~/.claude/skills/

# 在 Claude Code 中初始化
/tw-realestate-etl init
```

> **注意**：請下載 Assets 中的 `tw-realestate-etl.zip`，而非 GitHub 自動產生的 Source code 壓縮檔。
> Source code 包含整個 repo（tests、CI 設定等），不適合直接當 skill 使用。

### 方法二：從 repo 打包安裝

```bash
git clone https://github.com/tonycheng-0118/TW_RealEstate_ETL.git
cd TW_RealEstate_ETL

# 打包（產出 dist/tw-realestate-etl/，只含 skill 需要的檔案）
./build_skill.sh

# 複製到 skills 目錄
cp -r dist/tw-realestate-etl ~/.claude/skills/

# 在 Claude Code 中初始化
/tw-realestate-etl init
```

### Skill 使用方式

```
/tw-realestate-etl help                           顯示所有指令
/tw-realestate-etl init                           初始化設定
/tw-realestate-etl run 114S1                      匯入單季
/tw-realestate-etl run 112S1-114S1 D,A            批次匯入
/tw-realestate-etl run current                    當期資料
/tw-realestate-etl query 臺北市大安區忠孝東路近兩年大樓成交行情
/tw-realestate-etl status                         查看 DB 狀態
/tw-realestate-etl backup                         手動備份
/tw-realestate-etl schedule install               安裝排程
```


## 單位換算

| 轉換 | 公式 |
|------|------|
| m² → 坪 | `m² ÷ 3.30579` |
| 坪 → m² | `坪 × 3.30579` |
| 元/m² → 元/坪 | `元/m² × 3.30579` |
| 總價元 → 萬元 | `元 ÷ 10000` |

## 備份與還原

ETL 完成後自動執行 `pg_dump + gzip`，備份至 `REALPRICE_BACKUP_DIR`（可指向 Google Drive 同步資料夾）。

檔名格式：`tw_realestate_{YYYYMMDD}.sql.gz`。同日重複執行會覆寫並記錄於 log。

還原方式：

```bash
gunzip -c tw_realestate_20260327.sql.gz | psql -d tw_realestate
```

## 測試

```bash
source .venv/bin/activate
python -m pytest tests/ -v
```

## 已知限制

1. **無座標資料**：Open Data 不含經緯度，地點比對僅能透過門牌 / 地段地號文字匹配。如需座標可用 [TGOS Geocoding API](https://www.tgos.tw/) 補上。
2. **特殊交易**：備註欄含「親友交易」「特殊關係」等非市場交易，查詢時建議排除（詳見 `SKILL.md`）。
3. **車位混合計價**：部分交易總價含車位，計算淨房屋單價應扣除：`(total_price - parking_price) / (building_area - parking_area)`。
4. **CSV 欄位變動**：內政部偶爾調整欄位，`transform.py` 僅取 `COLUMN_MAP` 中定義的欄位，未知欄位靜默忽略。

## 相關專案

- [ronnywang/realprice](https://github.com/ronnywang/realprice) — 實價登錄爬蟲 + JSON/CSV 轉換（PHP）
- [zbryikt/realestate](https://github.com/zbryikt/realestate) — 實價登錄解譯器 + Geocoding（LiveScript）
- [grimmerk/Taiwan-house-price-data](https://github.com/grimmerk/Taiwan-house-price-data) — 實價登錄資料整理
- [ShenTengTu/realprice-analysis](https://gist.github.com/ShenTengTu/5c51f5b765108312181fb46338129679) — 實價登錄資料分析（Python + Pandas）

## 授權

MIT License
