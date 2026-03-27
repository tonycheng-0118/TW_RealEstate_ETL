-- TW_RealEstate_ETL — PostgreSQL Schema
-- Idempotent: safe to run multiple times (uses IF NOT EXISTS throughout).

-- Required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ==========================================================
-- Trigger function: auto-update updated_at on row modification
-- ==========================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ==========================================================
-- transactions: 不動產買賣 + 預售屋 (real estate sales + pre-sales)
-- ==========================================================
CREATE TABLE IF NOT EXISTS transactions (
    id                  UUID DEFAULT uuid_generate_v4() PRIMARY KEY,

    -- 位置 (Location)
    district            TEXT,           -- 鄉鎮市區
    address             TEXT,           -- 土地區段位置建物區段門牌
    land_section        TEXT,           -- 解析出的地段名稱 (parsed by ETL)
    land_number         TEXT,           -- 解析出的地號 (parsed by ETL)

    -- 交易 (Transaction)
    target_type         TEXT,           -- 交易標的 (房地/土地/建物/車位)
    transaction_date    TEXT,           -- 民國年日期 e.g. 1130715
    transaction_date_ad DATE,           -- 西元日期 e.g. 2024-07-15
    transaction_note    TEXT,           -- 交易筆棟數

    -- 土地 (Land)
    land_area           NUMERIC(12,2),  -- 土地移轉總面積 (m²)
    zoning_urban        TEXT,           -- 都市土地使用分區
    zoning_non_urban    TEXT,           -- 非都市土地使用分區
    zoning_non_urban_cd TEXT,           -- 非都市土地使用編定

    -- 建物 (Building)
    floor_transferred   TEXT,           -- 移轉層次
    total_floors        TEXT,           -- 總樓層數
    building_type       TEXT,           -- 建物型態
    main_purpose        TEXT,           -- 主要用途
    main_material       TEXT,           -- 主要建材
    build_complete_date TEXT,           -- 建築完成年月
    building_area       NUMERIC(12,2),  -- 建物移轉總面積 (m²)
    main_area           NUMERIC(12,2),  -- 主建物面積 (m²)
    sub_area            NUMERIC(12,2),  -- 附屬建物面積 (m²)
    balcony_area        NUMERIC(12,2),  -- 陽台面積 (m²)

    -- 格局 (Layout)
    rooms               INTEGER,        -- 房
    halls               INTEGER,        -- 廳
    bathrooms           INTEGER,        -- 衛
    has_partition       TEXT,            -- 隔間

    -- 管理 (Management)
    has_management      TEXT,           -- 有無管理組織
    has_elevator        TEXT,           -- 電梯

    -- 價格 (Price)
    total_price         BIGINT,         -- 總價 (元)
    unit_price          NUMERIC(12,2),  -- 單價 (元/m²)

    -- 車位 (Parking)
    parking_type        TEXT,           -- 車位類別
    parking_area        NUMERIC(12,2),  -- 車位面積 (m²)
    parking_price       BIGINT,         -- 車位總價 (元)

    -- 其他 (Others)
    serial_no           TEXT,           -- 編號 (unique key for dedup)
    transfer_no         TEXT,           -- 移轉編號
    note                TEXT,           -- 備註

    -- ETL metadata
    source_file         TEXT,           -- 來源檔名 e.g. d_lvr_land_a.csv
    source_season       TEXT,           -- 季度 e.g. 113S4
    city_code           CHAR(1),        -- 縣市代碼 e.g. D
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_txn_serial_no UNIQUE (serial_no)
);

-- Trigger for auto-updating updated_at
DROP TRIGGER IF EXISTS trg_transactions_updated_at ON transactions;
CREATE TRIGGER trg_transactions_updated_at
    BEFORE UPDATE ON transactions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ==========================================================
-- rentals: 租賃 (rental contracts)
-- ==========================================================
CREATE TABLE IF NOT EXISTS rentals (
    id                  UUID DEFAULT uuid_generate_v4() PRIMARY KEY,

    -- 位置 (Location)
    district            TEXT,
    address             TEXT,
    land_section        TEXT,
    land_number         TEXT,

    -- 交易 (Transaction)
    target_type         TEXT,
    transaction_date    TEXT,
    transaction_date_ad DATE,
    transaction_note    TEXT,

    -- 土地 (Land)
    land_area           NUMERIC(12,2),
    zoning_urban        TEXT,
    zoning_non_urban    TEXT,
    zoning_non_urban_cd TEXT,

    -- 建物 (Building)
    floor_transferred   TEXT,
    total_floors        TEXT,
    building_type       TEXT,
    main_purpose        TEXT,
    main_material       TEXT,
    build_complete_date TEXT,
    building_area       NUMERIC(12,2),
    main_area           NUMERIC(12,2),
    sub_area            NUMERIC(12,2),
    balcony_area        NUMERIC(12,2),

    -- 格局 (Layout)
    rooms               INTEGER,
    halls               INTEGER,
    bathrooms           INTEGER,
    has_partition       TEXT,

    -- 管理 (Management)
    has_management      TEXT,
    has_elevator        TEXT,

    -- 價格 (Rent)
    total_rent          BIGINT,         -- 月租金 (元)
    unit_rent           NUMERIC(12,2),  -- 單價 (元/m²)

    -- 其他 (Others)
    serial_no           TEXT,
    transfer_no         TEXT,
    note                TEXT,

    -- ETL metadata
    source_file         TEXT,
    source_season       TEXT,
    city_code           CHAR(1),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_rental_serial_no UNIQUE (serial_no)
);

-- Trigger for auto-updating updated_at
DROP TRIGGER IF EXISTS trg_rentals_updated_at ON rentals;
CREATE TRIGGER trg_rentals_updated_at
    BEFORE UPDATE ON rentals
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ==========================================================
-- etl_log: ETL execution log (prevents duplicate imports)
-- ==========================================================
CREATE TABLE IF NOT EXISTS etl_log (
    id          SERIAL PRIMARY KEY,
    season      TEXT NOT NULL,            -- e.g. 113S4
    file_name   TEXT NOT NULL,            -- e.g. d_lvr_land_a.csv
    row_count   INTEGER,
    status      TEXT DEFAULT 'success',   -- success / failed
    started_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_etl_season_file UNIQUE (season, file_name)
);

-- ==========================================================
-- Indexes for transactions
-- ==========================================================
CREATE INDEX IF NOT EXISTS idx_txn_district
    ON transactions (district);

CREATE INDEX IF NOT EXISTS idx_txn_address
    ON transactions USING GIN (address gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_txn_date_ad
    ON transactions (transaction_date_ad);

CREATE INDEX IF NOT EXISTS idx_txn_building_type
    ON transactions (building_type);

CREATE INDEX IF NOT EXISTS idx_txn_total_price
    ON transactions (total_price);

CREATE INDEX IF NOT EXISTS idx_txn_land_section
    ON transactions (land_section, land_number);

-- Most common composite query: district + date range
CREATE INDEX IF NOT EXISTS idx_txn_district_date
    ON transactions (district, transaction_date_ad);

-- ==========================================================
-- Indexes for rentals
-- ==========================================================
CREATE INDEX IF NOT EXISTS idx_rental_district
    ON rentals (district);

CREATE INDEX IF NOT EXISTS idx_rental_address
    ON rentals USING GIN (address gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_rental_date_ad
    ON rentals (transaction_date_ad);

CREATE INDEX IF NOT EXISTS idx_rental_building_type
    ON rentals (building_type);

CREATE INDEX IF NOT EXISTS idx_rental_district_date
    ON rentals (district, transaction_date_ad);
