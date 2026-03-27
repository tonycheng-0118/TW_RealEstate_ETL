"""
TW_RealEstate_ETL — Global Configuration

All settings support environment variable overrides with the prefix REALPRICE_.
Example: REALPRICE_DB_HOST=192.168.1.100 overrides DB_CONFIG["host"].
"""

import os
from pathlib import Path

# Load .env file if present (does not override existing env vars).
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass  # python-dotenv is optional; env vars can be set manually

# ---------- Project paths ----------
# PROJECT_ROOT is the directory containing this config file.
PROJECT_ROOT = Path(__file__).resolve().parent

# DATA_DIR stores downloaded ZIPs and extracted CSVs.
DATA_DIR = Path(os.environ.get("REALPRICE_DATA_DIR", PROJECT_ROOT / "data"))

# BACKUP_DIR stores pg_dump gzip backups.
# Default: PROJECT_ROOT/backups. Override via env var to point to e.g. a Google Drive sync folder.
BACKUP_DIR = Path(os.environ.get(
    "REALPRICE_BACKUP_DIR",
    PROJECT_ROOT / "backups",
))

# How many backup files to keep. 0 means keep all (default).
BACKUP_KEEP_COUNT = int(os.environ.get("REALPRICE_BACKUP_KEEP_COUNT", "0"))

# Log directory for ETL execution logs.
LOG_DIR = PROJECT_ROOT / "logs"

# ---------- Database ----------
DB_CONFIG = {
    "host": os.environ.get("REALPRICE_DB_HOST", "localhost"),
    "port": int(os.environ.get("REALPRICE_DB_PORT", "5432")),
    "dbname": os.environ.get("REALPRICE_DB_NAME", "tw_realestate"),
    "user": os.environ.get("REALPRICE_DB_USER", os.environ.get("USER", "postgres")),
    "password": os.environ.get("REALPRICE_DB_PASSWORD", ""),
}

# ---------- Download ----------
# URL template for historical seasonal data. {season} is e.g. "113S4".
SEASON_URL_TEMPLATE = (
    "https://plvr.land.moi.gov.tw/DownloadSeason"
    "?season={season}&type=zip&fileName=lvr_landcsv.zip"
)

# URL for the current (latest) period data.
CURRENT_URL = (
    "https://plvr.land.moi.gov.tw/Download"
    "?type=zip&fileName=lvr_landcsv.zip"
)

# Seconds to wait between consecutive season downloads to avoid rate limiting.
DOWNLOAD_DELAY_SEC = int(os.environ.get("REALPRICE_DOWNLOAD_DELAY", "10"))

# ---------- Transform ----------
# Encoding detection order for CSV files. Most historical files are Big5 (cp950).
ENCODING_ORDER = ["cp950", "big5", "utf-8-sig", "utf-8"]

# Only process CSVs for these city codes. Set to None for all cities.
# "A" = 臺北市. See CITY_CODES below for full mapping.
TARGET_CITY_CODES = ["A"]

# ---------- Load ----------
# Batch size for executemany upsert operations.
BATCH_SIZE = int(os.environ.get("REALPRICE_BATCH_SIZE", "500"))

# ---------- File type → target table mapping ----------
# CSV filename suffix determines the data type and target DB table.
# a = 不動產買賣 (real estate sales), b = 預售屋 (pre-sale), c = 租賃 (rentals).
FILE_TYPE_MAP = {
    "a": "transactions",
    "b": "transactions",
    "c": "rentals",
}

# ---------- Column mapping: Chinese CSV headers → English DB columns ----------
# Used by transform.py to rename columns. Only mapped columns are kept.

# 不動產買賣 (a) and 預售屋 (b) share the same column mapping.
COLUMN_MAP_A = {
    "鄉鎮市區": "district",
    "交易標的": "target_type",
    "土地區段位置建物區段門牌": "address",
    "土地位置建物門牌": "address",
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

# 預售屋 (b) uses the same mapping as 買賣 (a).
COLUMN_MAP_B = COLUMN_MAP_A.copy()

# 租賃 (c) — differs in price-related columns (rent instead of sale price).
COLUMN_MAP_C = {
    "鄉鎮市區": "district",
    "交易標的": "target_type",
    "土地區段位置建物區段門牌": "address",
    "土地位置建物門牌": "address",
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
    # Rental-specific price columns
    "租金元": "total_rent",
    "單價元平方公尺": "unit_rent",
    "備註": "note",
    "編號": "serial_no",
    "主建物面積": "main_area",
    "附屬建物面積": "sub_area",
    "陽台面積": "balcony_area",
    "電梯": "has_elevator",
    "移轉編號": "transfer_no",
}

# Lookup: file type suffix → column map.
COLUMN_MAP_BY_TYPE = {
    "a": COLUMN_MAP_A,
    "b": COLUMN_MAP_B,
    "c": COLUMN_MAP_C,
}

# ---------- City codes (縣市代碼對照) ----------
CITY_CODES = {
    "A": "臺北市",
    "B": "臺中市",
    "C": "基隆市",
    "D": "臺南市",
    "E": "高雄市",
    "F": "新北市",
    "G": "宜蘭縣",
    "H": "桃園市",
    "I": "嘉義市",
    "J": "新竹縣",
    "K": "苗栗縣",
    "L": "臺東縣",
    "M": "花蓮縣",
    "N": "南投縣",
    "O": "新竹市",
    "P": "雲林縣",
    "Q": "嘉義縣",
    "R": "屏東縣",
    "S": "彰化縣",
    "T": "臺東縣",
    "U": "花蓮縣",
    "V": "澎湖縣",
    "W": "金門縣",
    "X": "連江縣",
}
