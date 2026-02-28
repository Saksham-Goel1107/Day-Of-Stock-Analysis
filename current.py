import pandas as pd
import numpy as np
import re
import gspread
from google.colab import auth, userdata
from google.auth import default


# =========================================================
# GOOGLE OAUTH
# =========================================================
auth.authenticate_user()
creds, _ = default()
gc = gspread.authorize(creds)

SPREADSHEET_ID = userdata.get("SPREADSHEET_ID")
WORKSHEET_NAME = "Master Data"

BUFFER_DAYS = 5


# =========================================================
# FILE PATHS
# =========================================================
sales_file = "Sale Report - Item Wise-Nov 18, 2025-Feb 25, 2026-490.xlsx"
stock_file = "Stock Report-Feb 25, 2026-Feb 25, 2026-488.xlsx"
sourcing_file = "Sourcing time.xlsx"


# =========================================================
# LOAD DATA
# =========================================================
sales = pd.read_excel(sales_file)
stock = pd.read_excel(stock_file)
sourcing = pd.read_excel(sourcing_file)

sales.columns = sales.columns.str.strip().str.lower()
stock.columns = stock.columns.str.strip().str.lower()
sourcing.columns = sourcing.columns.str.strip().str.lower()


# =========================================================
# FILTER
# =========================================================
stock = stock[stock["category"].notna()]

sales = sales[
    sales["billed_by"].astype(str).str.lower().str.strip() == "sw-noida-cashier"
]

sales["code"] = sales["code"].astype(str).str.strip()
sales = sales[~sales["code"].isin(["", "nan", "none"])]

if "quantity" not in sales.columns:
    sales["quantity"] = 1


# =========================================================
# CLEAN PRODUCT NAMES
# =========================================================
def clean_name(x):
    x = str(x).lower().strip()
    x = re.sub(r"[^\w\s]", "", x, flags=re.UNICODE)
    x = re.sub(r"\s+", " ", x)
    return x


stock["name"] = stock["name"].apply(clean_name)
sourcing["brand name/name"] = sourcing["brand name/name"].apply(clean_name)

sales["code"] = sales["code"].str.lower()
stock["code"] = stock["code"].astype(str).str.lower().str.strip()


# =========================================================
# PARSE LEAD TIME
# =========================================================
def parse_days(text):
    if pd.isna(text):
        return 0

    txt = str(text).lower()

    if "instant" in txt:
        return 0

    nums = re.findall(r"\d+", txt)
    return max(map(int, nums)) if nums else 0


sourcing["lead_time_label"] = sourcing["time needed"]
sourcing["lead_time_days"] = sourcing["time needed"].apply(parse_days)


# =========================================================
# TOKEN MATCH LEAD TIME
# =========================================================
source_map = [
    (
        set(str(row["brand name/name"]).split()),
        row["lead_time_days"],
        row["lead_time_label"],
    )
    for _, row in sourcing.iterrows()
]

def match_lead(product_name):
    product_tokens = set(product_name.split())

    best_days = 0
    best_label = ""

    for tokens, days, label in source_map:
        if tokens and tokens.issubset(product_tokens):
            if days >= best_days:
                best_days = days
                best_label = label

    return pd.Series([best_days, best_label])


stock[["lead_time_days", "lead_time_label"]] = stock["name"].apply(match_lead)


# =========================================================
# DATE HANDLING
# =========================================================
sales["date"] = pd.to_datetime(sales["date"], errors="coerce")
stock["createdat"] = pd.to_datetime(stock["createdat"], errors="coerce")

sales = sales.dropna(subset=["date"])
today = sales["date"].max().normalize()


# =========================================================
# SALES SUMMARY
# =========================================================
sales_summary = (
    sales.groupby("code", as_index=False)["quantity"]
    .sum()
    .rename(columns={"quantity": "total_sales"})
)

df = pd.merge(stock, sales_summary, on="code", how="left")
df["total_sales"] = df["total_sales"].fillna(0)


# =========================================================
# STOCK COLUMN SAFE DETECTION
# =========================================================
def find_column(df, include_words):
    for col in df.columns:
        if all(word in col.lower() for word in include_words):
            return col
    raise ValueError(f"Column not found with keywords: {include_words}")


smartworks_col = find_column(df, ["smartworks", "noida", "stock"])
main_stock_col = find_column(df, ["main", "stock"])

df["current_stock"] = pd.to_numeric(df[smartworks_col], errors="coerce").fillna(0)
df["main_stock_display"] = df[main_stock_col]


# =========================================================
# WORKING DAYS
# =========================================================
def working_days(start, end):
    if pd.isna(start):
        return 0
    return max(np.busday_count(start.date(), end.date()), 1)


df["working_days"] = df["createdat"].apply(lambda x: working_days(x, today))
df["live_on"] = df["createdat"]


# =========================================================
# RUN RATE & REORDER LOGIC
# =========================================================
df["daily_run_rate"] = np.where(
    df["working_days"] > 0,
    df["total_sales"] / df["working_days"],
    0,
)

df["days_of_stock_left"] = np.where(
    df["daily_run_rate"] > 0,
    df["current_stock"] / df["daily_run_rate"],
    9999,
)

df["reorder_threshold_days"] = BUFFER_DAYS + df["lead_time_days"]

df["reorder"] = np.where(
    df["days_of_stock_left"] <= df["reorder_threshold_days"],
    "YES",
    "NO",
)


# =========================================================
# FINAL DATAFRAME
# =========================================================
final = (
    df[
        [
            "code",
            "name",
            "category",
            "live_on",
            "lead_time_label",
            "reorder_threshold_days",
            "main_stock_display",
            "current_stock",
            "total_sales",
            "working_days",
            "daily_run_rate",
            "days_of_stock_left",
            "reorder",
        ]
    ]
    .rename(columns={"code": "sku_code"})
    .sort_values("days_of_stock_left")
)

# Preserve SKU as text explicitly
final["sku_code"] = final["sku_code"].astype(str)
# =========================================================
# HARD JSON SANITIZATION
# =========================================================
import math
import datetime

def to_json_safe(val):
    # None stays None
    if val is None:
        return None

    # Pandas NA
    if pd.isna(val):
        return None

    # Timestamp → string
    if isinstance(val, (pd.Timestamp, datetime.datetime, datetime.date)):
        return str(val)

    # Numpy integers
    if isinstance(val, (np.integer,)):
        return int(val)

    # Numpy floats
    if isinstance(val, (np.floating,)):
        val = float(val)

    # Python float
    if isinstance(val, float):
        if not math.isfinite(val):
            return None
        return val

    return val


safe_values = []
safe_values.append(list(final.columns))

for row in final.itertuples(index=False):
    safe_row = [to_json_safe(x) for x in row]
    safe_values.append(safe_row)


# =========================================================
# UPDATE SHEET
# =========================================================
sheet = gc.open_by_key(SPREADSHEET_ID)
ws = sheet.worksheet(WORKSHEET_NAME)

existing_rows = len(ws.get_all_values())
if existing_rows > 1:
    col_count = len(final.columns)
    end_col = chr(64 + col_count)
    ws.batch_clear([f"A2:{end_col}{existing_rows}"])

ws.update(safe_values, "A1", value_input_option="USER_ENTERED")

print("Upload completed successfully.")