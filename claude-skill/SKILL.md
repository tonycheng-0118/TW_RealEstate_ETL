# 台灣實價登錄查詢技能

## 觸發條件

當使用者詢問以下相關問題時啟用此技能：
- 台灣不動產/房地產/房屋/土地的成交價格、歷史行情
- 特定地址/地段/地號附近的實價登錄
- 某區域的房價趨勢、均價分析
- 租金行情查詢
- 坪數/面積換算與房價計算

## 資料庫連線

- **資料庫**: `tw_realestate` (PostgreSQL，透過 MCP postgres server 連線)
- **資料來源**: 內政部實價登錄開放資料
- **資料範圍**: 預設僅含臺北市（city_code = 'A'），可擴充

## Schema 參考

### transactions 表（不動產買賣 + 預售屋）

| 欄位 | 型別 | 說明 |
|------|------|------|
| district | TEXT | 鄉鎮市區，如「大安區」「信義區」 |
| address | TEXT | 土地區段位置建物區段門牌 |
| land_section | TEXT | 地段名稱（如「建國段」） |
| land_number | TEXT | 地號 |
| target_type | TEXT | 交易標的：房地(含車位)、土地、建物、車位 |
| transaction_date | TEXT | 民國年日期，如 1130715 |
| transaction_date_ad | DATE | 西元日期，如 2024-07-15 |
| land_area | NUMERIC | 土地移轉總面積（m²） |
| building_area | NUMERIC | 建物移轉總面積（m²） |
| main_area | NUMERIC | 主建物面積（m²） |
| sub_area | NUMERIC | 附屬建物面積（m²） |
| balcony_area | NUMERIC | 陽台面積（m²） |
| building_type | TEXT | 建物型態：透天厝、住宅大樓、公寓、華廈等 |
| main_purpose | TEXT | 主要用途 |
| total_floors | TEXT | 總樓層數 |
| floor_transferred | TEXT | 移轉層次 |
| rooms | INTEGER | 房 |
| halls | INTEGER | 廳 |
| bathrooms | INTEGER | 衛 |
| total_price | BIGINT | 總價（元） |
| unit_price | NUMERIC | 單價（元/m²） |
| parking_type | TEXT | 車位類別 |
| parking_area | NUMERIC | 車位面積（m²） |
| parking_price | BIGINT | 車位總價（元） |
| serial_no | TEXT | 編號（唯一鍵） |
| note | TEXT | 備註 |
| source_season | TEXT | 資料季度，如 113S4 |
| city_code | CHAR(1) | 縣市代碼 |

### rentals 表（租賃）

結構同 transactions，但價格欄位不同：
- `total_rent` BIGINT — 月租金（元）
- `unit_rent` NUMERIC — 單價（元/m²）

### etl_log 表（ETL 紀錄）

可查詢資料最新更新時間：
```sql
SELECT season, file_name, row_count, finished_at
FROM etl_log WHERE status = 'success'
ORDER BY finished_at DESC LIMIT 10;
```

## 單位換算

| 轉換 | 公式 |
|------|------|
| m² → 坪 | m² ÷ 3.30579 |
| 坪 → m² | 坪 × 3.30579 |
| 元/m² → 元/坪 | 元/m² × 3.30579 |
| 總價元 → 萬元 | 元 ÷ 10000 |

**回覆時一律提供坪數和萬元，方便使用者理解。**

## 查詢注意事項

### 1. 排除特殊交易
備註欄含特殊關係的交易不具市場參考價值，查詢時應排除：
```sql
WHERE (note IS NULL OR (
    note NOT LIKE '%親友%'
    AND note NOT LIKE '%特殊關係%'
    AND note NOT LIKE '%受債權債務影響%'
    AND note NOT LIKE '%二親等%'
))
```

### 2. 淨房屋單價（扣除車位）
部分交易的總價含車位，計算真實房屋單價時應扣除：
```sql
(total_price - COALESCE(parking_price, 0))
/ NULLIF(building_area - COALESCE(parking_area, 0), 0)
```

### 3. 建物型態關鍵字
- 透天厝：`building_type LIKE '%透天%'`
- 大樓（11F+）：`building_type LIKE '%住宅大樓%'`
- 華廈（7-10F）：`building_type LIKE '%華廈%'`
- 公寓（5F以下無電梯）：`building_type LIKE '%公寓%'`

### 4. 門牌地址搜尋
address 欄位已建立 GIN trigram 索引，支援模糊搜尋：
```sql
WHERE address LIKE '%忠孝東路%'
```

### 5. 地段地號搜尋
```sql
WHERE land_section = '建國段' AND land_number = '100'
```

## 常用 SQL 範例

### 查詢特定行政區近兩年透天成交行情
```sql
SELECT
    district,
    address,
    transaction_date_ad,
    building_type,
    ROUND(building_area / 3.30579, 1) AS "坪數",
    ROUND(total_price / 10000.0, 0) AS "總價萬",
    ROUND(
        (total_price - COALESCE(parking_price, 0))
        / NULLIF(building_area - COALESCE(parking_area, 0), 0)
        * 3.30579
        / 10000.0, 1
    ) AS "每坪萬元"
FROM transactions
WHERE district = '大安區'
  AND building_type LIKE '%透天%'
  AND transaction_date_ad >= CURRENT_DATE - INTERVAL '2 years'
  AND (note IS NULL OR note NOT LIKE '%親友%')
ORDER BY transaction_date_ad DESC;
```

### 查詢特定路段均價
```sql
SELECT
    COUNT(*) AS "筆數",
    ROUND(AVG(total_price) / 10000.0, 0) AS "平均總價萬",
    ROUND(
        AVG(
            (total_price - COALESCE(parking_price, 0))
            / NULLIF(building_area - COALESCE(parking_area, 0), 0)
            * 3.30579
        ) / 10000.0, 1
    ) AS "平均每坪萬元"
FROM transactions
WHERE address LIKE '%忠孝東路%'
  AND transaction_date_ad >= CURRENT_DATE - INTERVAL '2 years'
  AND (note IS NULL OR note NOT LIKE '%親友%');
```

### 查詢租金行情
```sql
SELECT
    district,
    address,
    building_type,
    ROUND(building_area / 3.30579, 1) AS "坪數",
    total_rent AS "月租金",
    transaction_date_ad
FROM rentals
WHERE district = '信義區'
  AND transaction_date_ad >= CURRENT_DATE - INTERVAL '1 year'
ORDER BY transaction_date_ad DESC;
```

## ETL 操作

如需更新資料，可在專案根目錄執行：
```bash
python scripts/run_etl.py --current
```
