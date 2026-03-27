---
name: tw-realestate-query
description: 查詢台灣實價登錄資料。當使用者詢問不動產成交價格、房價行情、租金查詢、特定地址或地段的實價登錄時使用。
argument-hint: <查詢描述，如「臺北市大安區忠孝東路近兩年大樓成交行情」>
allowed-tools: Bash(psql *), Bash(python *), Bash(gunzip *), Bash(ls *), Read, Grep, Glob
---

# 台灣實價登錄查詢

使用者的查詢需求：**$ARGUMENTS**

## 專案路徑設定

<!-- 使用者請將下方路徑改為自己的 TW_RealEstate_ETL 專案絕對路徑 -->
ETL 專案根目錄：`ETL_ROOT=/path/to/TW_RealEstate_ETL`

所有需要執行 Python 腳本的步驟，都必須先：
```bash
cd $ETL_ROOT
source .venv/bin/activate
```

## 執行流程

你是台灣實價登錄資料查詢助手。收到查詢後依序執行以下步驟：

### Step 1: 確認資料庫可用

檢查 PostgreSQL 是否運行中，以及 `tw_realestate` 資料庫是否存在：

```bash
psql -d tw_realestate -c "SELECT 1;" 2>&1
```

如果資料庫不存在或無法連線：
1. 嘗試從備份還原（見 Step 2）
2. 如果沒有備份，告知使用者需先建立資料庫（`createdb tw_realestate && psql -d tw_realestate -f $ETL_ROOT/sql/schema.sql`）

### Step 2: 確認目標縣市資料是否已匯入

從使用者查詢中解析出目標**縣市**和**時間範圍**，然後查詢 `etl_log` 確認資料是否存在：

```bash
psql -d tw_realestate -c "
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
| C | 基隆市 | L | 臺東縣 | T | 臺東縣 |
| D | 臺南市 | M | 花蓮縣 | U | 花蓮縣 |
| E | 高雄市 | N | 南投縣 | V | 澎湖縣 |
| F | 新北市 | O | 新竹市 | W | 金門縣 |
| G | 宜蘭縣 | P | 雲林縣 | X | 連江縣 |
| H | 桃園市 | Q | 嘉義縣 | | |
| I | 嘉義市 | | | | |

### Step 3: 資料不存在時的補救措施

如果 etl_log 中**找不到目標縣市/季度的資料**，依序嘗試：

#### 3a. 從備份還原

先檢查備份目錄是否有檔案：

```bash
ls -lt ${REALPRICE_BACKUP_DIR:-./backups}/tw_realestate_*.sql.gz 2>/dev/null | head -5
```

如果有備份檔，還原最新的那份：

```bash
gunzip -c <最新備份檔路徑> | psql -d tw_realestate
```

還原後重新檢查 etl_log，如果目標資料已存在就跳到 Step 4。

#### 3b. 執行 ETL 下載

如果備份中也沒有目標資料，執行 ETL 下載：

```bash
# 根據使用者查詢的時間範圍決定季度
# 西元年 → 民國年：西元 - 1911 = 民國年
# 每年分四季：S1(1-3月) S2(4-6月) S3(7-9月) S4(10-12月)
cd $ETL_ROOT && source .venv/bin/activate
python scripts/run_etl.py --from <from_season> --to <to_season> --city <city_code>
```

**注意**：下載需要時間（每季約 3-5 秒 + 10 秒間隔），告知使用者正在下載。

### Step 4: 執行查詢

資料確認存在後，用 `psql` 執行 SQL 查詢。

#### 查詢規則

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

#### SQL 範例

```sql
-- 查詢特定行政區近兩年大樓成交行情
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

```sql
-- 查詢租金行情
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

### Step 5: 整理回覆

查詢結果用**繁體中文**回覆，包含：
1. 列表或表格呈現成交紀錄
2. 行情摘要（均價、價格區間、趨勢）
3. 注意事項（車位混合計價、特殊交易等）

## DB Schema 參考

### transactions 表（不動產買賣 + 預售屋）

| 欄位 | 型別 | 說明 |
|------|------|------|
| district | TEXT | 鄉鎮市區 |
| address | TEXT | 門牌（GIN trigram 索引） |
| land_section | TEXT | 地段名稱 |
| land_number | TEXT | 地號 |
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
