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
| 歷史按季 | `plvr.land.moi.gov.tw/DownloadSeason?season={season}` | 101 年起；用系統 `curl` 下載（Python 3.13+ 的 `requests` 不接受此站的 TWCA 憑證鏈） |
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
- Python 3.12–3.14（3.15 起 `requirements.txt` 的 pandas 沒有預先編好的 wheel，`pip install` 會失敗）
- PostgreSQL 17（`brew install postgresql@17`）
- `curl`（macOS 內建；下載用）
- Node.js（MCP server 需要 `npx`）

### 安裝

```bash
# 1. Clone & 建立虛擬環境
git clone https://github.com/<your-username>/TW_RealEstate_ETL.git
cd TW_RealEstate_ETL
python3.14 -m venv .venv   # 3.12–3.14 皆可
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
# 當前季度（不帶參數 = 當前季度）
python scripts/run_etl.py

# 單季
python scripts/run_etl.py --start 114S1 --end 114S1

# 批次匯入（範圍）
python scripts/run_etl.py --start 112S1 --end 114S1

# 從特定季度到當前（省略 --end = 到當前季度）
python scripts/run_etl.py --start 113S1

# 指定縣市（覆蓋 config.py 預設值）
python scripts/run_etl.py --start 114S1 --end 114S1 --city J

# 多縣市
python scripts/run_etl.py --start 111S1 --end 112S4 --city J,H

# 全台
python scripts/run_etl.py --start 114S1 --end 114S1 --city all

# 只執行備份
python scripts/run_etl.py --backup-only

# ETL 完不備份
python scripts/run_etl.py --start 114S1 --end 114S1 --skip-backup
```

**參數行為：**

| `--start` | `--end` | 實際行為 |
|-----------|---------|----------|
| 留空 | 留空 | 當前季度（單季） |
| `113S1` | `113S1` | 113S1 單季 |
| `113S1` | `114S4` | 113S1 ~ 114S4 區間 |
| `113S1` | 留空 | 113S1 ~ 當前季度 |

**季度格式**：`{民國年}S{季度}`，例如 `113S1` = 2024 Q1、`114S4` = 2025 Q4

> `source_season` 永遠使用實際季度號（如 `115S2`），不再使用 `current` 字串。
> 當期資料在 `etl_log` 記成 `current/{檔名}`，每次都重新 upsert（每月 1、11、21 日各一批），也不會擋到該季日後的整季匯入。

**Exit code：**

| exit | 意義 |
|------|------|
| 0 | 全部成功（有跑備份就含備份成功） |
| 1 | 部分季（或部分檔）失敗、備份失敗；備份仍有跑。DB 連不上等未預期錯誤也是 1，但沒跑備份 |
| 3 | 每一季都失敗（含目標縣市沒有資料）；不跑備份，避免把好的備份輪掉 |
| 2 | 參數錯誤 |

### 刪除資料

```bash
# 刪除當前季度
python scripts/delete.py

# 刪除單季
python scripts/delete.py --start 112S1 --end 112S1

# 刪除區間
python scripts/delete.py --start 112S1 --end 113S4

# 刪除特定城市的特定季度
python scripts/delete.py --start 113S2 --end 113S2 --city A
```

### 回補預售屋建案名稱

既有 DB 升級後（`run_etl.py` 開頭會檢查，缺欄才自動 `ALTER TABLE … ADD COLUMN`），舊的預售屋列沒有 `project_name`／`building_unit`／`cancel_status`。
用本機已下載的 b 檔補上（不下載、只 UPDATE 這三欄、不新增列；只讀季別資料夾如 `114S1/` 與 `current/`，其他資料夾如 `114S1.old/` 略過；同一筆在多個檔出現時以最新季、最新 current 為準；同一季在多個 `--data-dir` 都有時，以 `<季>.zip` 較新的為準）：

```bash
# 預設讀 config.DATA_DIR；--data-dir 可重複，--dry-run 只讀檔不寫 DB；一個 b 檔都沒找到 → exit 4
python scripts/backfill_presale.py --city D,J,O
python scripts/backfill_presale.py --city D,J,O --data-dir data --data-dir ~/.claude/skills/tw-realestate-etl/data

# 重新下載某一季後只套那一季，並證明換到新檔：--since 填改名舊 zip 前的 `date +%s`
# exit 4：找不到目標季的檔、<季>.zip 比 --since 舊（還是舊下載）、沒有預售欄位（107S1 以前）、或有資料列卻對到 0 列（只有表頭的檔不算）
python scripts/backfill_presale.py --city D --data-dir data --season 115S3 --since 1763800000
```

`--season` 看的是 `<季>.zip` 的 mtime，不是 CSV：`run_etl.py` 每次都會重新解壓，CSV 的 mtime 永遠是新的。

### 啟用排程（macOS LaunchAgent）

```bash
# 先編輯 plist，將 /path/to/TW_RealEstate_ETL 替換為你的實際路徑
# 預設只跑當期；要指定縣市就在 ProgramArguments 加 --city 與 D,J,O
cp launchd/com.tw-realestate.etl.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.tw-realestate.etl.plist

# 手動觸發一次（測試用）
launchctl start com.tw-realestate.etl

# 確認狀態（第二欄是上次的 exit code）
launchctl list | grep tw-realestate
```

## 專案結構

```
TW_RealEstate_ETL/
├── config.py                 # 全域設定（DB、URL、欄位對照、路徑）
├── requirements.txt          # Python 依賴
├── requirements-dev.txt      # 測試與 lint 工具（pytest、pytest-xdist、ruff）
├── ruff.toml                 # ruff 規則（pre-push hook 用）
├── githooks/
│   └── pre-push              # 本機 regression：ruff＋全套 pytest（取代 CI）
├── .env.template             # 環境變數範本（複製為 .env 後填入本機設定）
├── spec.md                   # 開發規格書（完整設計文件）
├── .mcp.json                 # Claude Code MCP PostgreSQL 設定
├── scripts/
│   ├── run_etl.py            # 主控腳本（唯一進入點）
│   ├── download.py           # Step 1: 下載 ZIP
│   ├── transform.py          # Step 2: 解壓 + 清洗 CSV
│   ├── load.py               # Step 3: Upsert PostgreSQL
│   ├── backup.py             # Step 4: pg_dump + gzip 備份
│   ├── delete.py             # 刪除指定季度/區間資料
│   ├── backfill_presale.py   # 用本機 b 檔回補預售屋建案名稱／棟及號／解約情形
│   └── season_utils.py       # 季度計算、驗證、參數解析
├── sql/
│   └── schema.sql            # PostgreSQL DDL（表、索引、觸發器）
├── launchd/
│   └── com.tw-realestate.etl.plist  # macOS LaunchAgent 排程
├── build_skill.sh            # Skill 打包腳本（產出 dist/tw-realestate-etl/）
├── claude-skill/
│   └── tw-realestate-etl/
│       └── SKILL.md          # Claude Code 統一技能（7 操作：help/init/run/query/status/backup/schedule）
├── tests/
│   ├── test_transform.py     # 單元測試
│   ├── test_run_etl.py       # exit code、備份、下載、etl_log 過濾
│   ├── test_presale.py       # 預售屋三欄：transform、schema 遷移、回補（DB 測試用 TEMP 表，連不上就 skip）
│   ├── test_backup.py        # backup.py 命令列：--help／未知參數不備份
│   └── fixtures/             # 測試用的真實回應樣本
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
| 預售屋 | `project_name` | TEXT | 建案名稱（b 檔 107S1 起才有，其餘 NULL） |
| | `building_unit` | TEXT | 棟及號（同上） |
| | `cancel_status` | TEXT | 解約情形，如 `1130116全部解約`（b 檔 112S3 起；NULL＝匯入時未解約，匯入後才登記的解約不會回寫） |
| 唯一鍵 | `serial_no` | TEXT | 編號，用於 upsert 去重 |

### rentals（租賃）

結構同 transactions，價格欄位改為：
- `total_rent` BIGINT — 月租金（元）
- `unit_rent` NUMERIC — 單價（元/m²）

### etl_log（ETL 紀錄）

追蹤每次匯入的季度、檔案、筆數、狀態。`(season, file_name)` 為唯一鍵，防止重複匯入；當期資料記成 `current/{檔名}`、每次都重新匯入。

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

本專案提供自包含的 Claude Code Skill，安裝後即可在 Claude Code 中完成所有操作（初始化、ETL、查詢、備份、排程），**不需要 git clone、npm**；`init` 會在 skill 目錄建 `.venv` 裝 Python 套件，之後所有指令都用 config.json 的 `python_path`（那個 `.venv` 的 python）。

### 方法一：從 Release 安裝（推薦）

到 [Releases](https://github.com/tonycheng-0118/TW_RealEstate_ETL/releases) 頁面下載 **`tw-realestate-etl.zip`**（Assets 區的附件，不是 Source code）：

```bash
# 解壓到 skills 目錄（zip 內已含 tw-realestate-etl/ 資料夾）
unzip tw-realestate-etl.zip -d ~/.claude/skills/

# 在 Claude Code 中初始化
/tw-realestate-etl init
```

> **注意**：請下載 Assets 中的 `tw-realestate-etl.zip`，而非 GitHub 自動產生的 Source code 壓縮檔。
> Source code 包含整個 repo（tests、hook 設定等），不適合直接當 skill 使用。

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
.venv/bin/pip install -r requirements-dev.txt   # pytest、pytest-xdist、ruff
.venv/bin/python -m ruff check .
.venv/bin/python -m pytest -q -n auto -rs
```

### pre-push hook（取代 GitHub Actions）

本 repo 不用 CI，push 前由本機 hook 跑 ruff＋全套 pytest。每個 clone 設一次（同一 clone 的 worktree 共用 repo config）：

```bash
git config core.hooksPath githooks
```

- 本 hook 之前切出、沒有 `githooks/` 的分支 push 時找不到 hook，會直接放行：先 rebase 到 main。
- hook 用該 worktree 的 `.venv/bin/python`；沒有或缺套件時會印出建置指令、擋下 push。
- 工作區要乾淨（含 untracked），測的就是要推的 HEAD。不 push 只想驗：`sh githooks/pre-push --manual`，成功最後一行是 `pre-push: ✓ passed`。
- 只測 `.venv` 那一版 Python（Mac mini 是 3.14，和 launchd 排程用的 main checkout `.venv` 同版）；以前 CI 跑的 3.12／3.13 不再測，要支援舊版時手動建該版 `.venv` 跑一次。
- **不要求 PostgreSQL**：`tests/test_presale.py` 的 DB 測試在連不上時 skip（`-rs` 會列出 skip 原因）。Mac mini 有本機 DB 所以會跑到；在沒有 DB 的機器 push 前，hook 在成功字串前直接探一次 DB 連線，連不上就印 `pre-push: WARNING: PostgreSQL unreachable; …`，代表這幾支沒測到（test_gate 快取命中時也會探）。

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
