"""
Stock Analysis and Sync Script.
Designed for prod deployment via Docker & Cron.
Authenticates via Google Service Account (service.json).
"""

import os
import re
import math
import logging
import datetime
from typing import List, Any
import time

import pandas as pd
import numpy as np
import gspread
import requests
from dotenv import load_dotenv

# Load all variables from .env if present
load_dotenv()

# =========================================================
# 1. CONFIGURATION & LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        # logging.FileHandler("analysis.log") # Un-comment to log to a file
    ]
)
logger = logging.getLogger(__name__)

class Config:
    # Service Account Credentials (Replaces Colab OAuth)
    SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service.json")

    # SOURCE_MODE determines how input data is ingested.
    # Modes:
    #   'local'   -> Reads .xlsx files mounted in the Docker container.
    #   'gsheets' -> Reads directly from Google Sheets using Spreadsheet IDs.
    SOURCE_MODE = os.getenv("SOURCE_MODE", "gsheets").lower()

    # ---------------------------------------------------------
    # SOURCE CONFIG (Mode: 'local')
    # ---------------------------------------------------------
    SALES_FILE_PATH = os.getenv("SALES_FILE_PATH", "Sale Report - Item Wise-Nov 18, 2025-Feb 25, 2026-490.xlsx")
    STOCK_FILE_PATH = os.getenv("STOCK_FILE_PATH", "Stock Report-Feb 25, 2026-Feb 25, 2026-488.xlsx")
    SOURCING_FILE_PATH = os.getenv("SOURCING_FILE_PATH", "Sourcing time.xlsx")

    # ---------------------------------------------------------
    # SOURCE CONFIG (Mode: 'gsheets')
    # Use these if you have moved the source files into Google Sheets format.
    # ---------------------------------------------------------
    SALES_SPREADSHEET_ID = os.getenv("SALES_SPREADSHEET_ID", "YOUR_SALES_SPREADSHEET_ID")
    SALES_WORKSHEET_NAME = os.getenv("SALES_WORKSHEET_NAME", "Sheet1")

    STOCK_SPREADSHEET_ID = os.getenv("STOCK_SPREADSHEET_ID", "YOUR_STOCK_SPREADSHEET_ID")
    STOCK_WORKSHEET_NAME = os.getenv("STOCK_WORKSHEET_NAME", "Sheet1")

    SOURCING_SPREADSHEET_ID = os.getenv("SOURCING_SPREADSHEET_ID", "YOUR_SOURCING_SPREADSHEET_ID")
    SOURCING_WORKSHEET_NAME = os.getenv("SOURCING_WORKSHEET_NAME", "Sheet1")

    # ---------------------------------------------------------
    # TARGET CONFIG (Destination)
    # ---------------------------------------------------------
    TARGET_SPREADSHEET_ID = os.getenv("TARGET_SPREADSHEET_ID", "YOUR_TARGET_SPREADSHEET_ID")
    TARGET_WORKSHEET_NAME = os.getenv("TARGET_WORKSHEET_NAME", "Master Data")

    # Core Data Filtering
    BUFFER_DAYS = int(os.getenv("BUFFER_DAYS", "5"))

    # Heartbeat & Retry Configuration
    HEARTBEAT_URL = os.getenv("HEARTBEAT_URL", "https://uptime.betterstack.com/api/v1/heartbeat/n9Lt2EjGGL9s73163Hi3N7dZ")
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY = int(os.getenv("RETRY_DELAY", "60")) # seconds

# =========================================================
# 2. ANALYSIS CLASS
# =========================================================

class StockAnalyzer:
    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.gc = None

    def authenticate(self):
        """Authenticates using the Service Account JSON file."""
        self.logger.info(f"Authenticating to Google Services using '{self.config.SERVICE_ACCOUNT_FILE}'...")
        try:
            if not os.path.exists(self.config.SERVICE_ACCOUNT_FILE):
                raise FileNotFoundError(f"Service account file not found: {self.config.SERVICE_ACCOUNT_FILE}")

            self.gc = gspread.service_account(filename=self.config.SERVICE_ACCOUNT_FILE)
            self.logger.info("Service Account Authentication successful.")
        except Exception as e:
            self.logger.error(f"Failed to authenticate: {e}")
            raise

    def get_dataframe(self, local_path: str, sheet_id: str, sheet_name: str) -> pd.DataFrame:
        """Dynamically fetch DataFrame based on the SOURCE_MODE."""

        if self.config.SOURCE_MODE == "local":
            if not os.path.exists(local_path):
                raise FileNotFoundError(f"Source file missing: {local_path}. Is it mounted correctly?")
            self.logger.info(f"Loading local Excel file: {local_path}")
            return pd.read_excel(local_path)

        elif self.config.SOURCE_MODE == "gsheets":
            self.logger.info(f"Fetching Google Sheet via API: {sheet_id} [{sheet_name}]")
            try:
                worksheet = self.gc.open_by_key(sheet_id).worksheet(sheet_name)
                data = worksheet.get_all_values()
                if not data:
                    self.logger.warning(f"Warning: Extracted sheet {sheet_id} was empty.")
                    return pd.DataFrame()

                headers = data.pop(0)
                df = pd.DataFrame(data, columns=headers)
                # Google Sheets API returns empty cells as strings, which we want to act as NaNs
                df.replace("", np.nan, inplace=True)
                return df
            except Exception as e:
                self.logger.error(f"Failed interpreting Google Sheet {sheet_id}: {e}")
                raise
        else:
            raise ValueError(f"Invalid SOURCE_MODE '{self.config.SOURCE_MODE}' configured.")

    def load_data(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Load and normalize source table columns securely."""
        self.logger.info(f"Data Source Mode is set to: {self.config.SOURCE_MODE.upper()}")

        sales = self.get_dataframe(self.config.SALES_FILE_PATH, self.config.SALES_SPREADSHEET_ID, self.config.SALES_WORKSHEET_NAME)
        stock = self.get_dataframe(self.config.STOCK_FILE_PATH, self.config.STOCK_SPREADSHEET_ID, self.config.STOCK_WORKSHEET_NAME)
        sourcing = self.get_dataframe(self.config.SOURCING_FILE_PATH, self.config.SOURCING_SPREADSHEET_ID, self.config.SOURCING_WORKSHEET_NAME)

        sales.columns = sales.columns.str.strip().str.lower()
        stock.columns = stock.columns.str.strip().str.lower()
        sourcing.columns = sourcing.columns.str.strip().str.lower()

        return sales, stock, sourcing

    def process_data(self, sales: pd.DataFrame, stock: pd.DataFrame, sourcing: pd.DataFrame) -> pd.DataFrame:
        """Identical logic adapted safely for production."""
        self.logger.info("Initializing Data Processing and Business Logic Transformation...")

        # 1. Filters & Core Type Setup
        stock = stock[stock["category"].notna()].copy()

        sales["billed_by"] = sales["billed_by"].astype(str).str.lower().str.strip()
        sales = sales[sales["billed_by"] == "sw-noida-cashier"].copy()

        sales["code"] = sales["code"].astype(str).str.strip().str.lower()
        sales = sales[~sales["code"].isin(["", "nan", "none"])]

        if "quantity" not in sales.columns:
            sales["quantity"] = 1
        else:
            sales["quantity"] = pd.to_numeric(sales["quantity"], errors="coerce").fillna(1)

        stock["code"] = stock["code"].astype(str).str.lower().str.strip()

        # 2. Text Cleansing Logic
        def clean_name(x):
            if pd.isna(x): return ""
            x = str(x).lower().strip()
            x = re.sub(r"[^\w\s]", "", x, flags=re.UNICODE)
            x = re.sub(r"\s+", " ", x)
            return x

        stock["name_clean"] = stock["name"].apply(clean_name)
        sourcing["brand name clean"] = sourcing["brand name/name"].apply(clean_name)

        # 3. Time Constraints Logic
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

        # 4. Keyword Subsetting for Source Matching
        source_map = [
            (set(str(row["brand name clean"]).split()), row["lead_time_days"], row["lead_time_label"])
            for _, row in sourcing.iterrows()
        ]

        def match_lead(product_name):
            product_tokens = set(product_name.split())
            best_days, best_label = 0, ""
            for tokens, days, label in source_map:
                if tokens and tokens.issubset(product_tokens):
                    if days >= best_days:
                        best_days = days
                        best_label = label
            return pd.Series([best_days, best_label])

        stock[["lead_time_days", "lead_time_label"]] = stock["name_clean"].apply(match_lead)

        # 5. Pandas Time Resampling Setup
        sales["date"] = pd.to_datetime(sales["date"], errors="coerce")
        stock["createdat"] = pd.to_datetime(stock["createdat"], errors="coerce")
        sales = sales.dropna(subset=["date"])

        today = pd.Timestamp.now().normalize()
        if not sales.empty:
            today = sales["date"].max().normalize()

        # 6. Summation Frame Assembly
        sales_summary = sales.groupby("code", as_index=False)["quantity"].sum().rename(columns={"quantity": "total_sales"})
        df = pd.merge(stock, sales_summary, on="code", how="left")
        df["total_sales"] = df["total_sales"].fillna(0)

        # 7. Safe Keyword Detection (Resolves dynamic input headings)
        def find_column(df_search, include_words):
            for col in df_search.columns:
                if all(word in col.lower() for word in include_words):
                    return col
            raise ValueError(f"CRITICAL ERROR: Matrix column failed keywords requirement: {include_words}")

        smartworks_col = find_column(df, ["smartworks", "noida", "stock"])
        main_stock_col = find_column(df, ["main", "stock"])

        df["current_stock"] = pd.to_numeric(df[smartworks_col], errors="coerce").fillna(0)
        df["main_stock_display"] = df[main_stock_col]

        # 8. Business Execution Days Allocation
        def working_days(start, end):
            if pd.isna(start):
                return 0
            return max(np.busday_count(start.date(), end.date()), 1)

        df["working_days"] = df["createdat"].apply(lambda x: working_days(x, today))
        df["live_on"] = df["createdat"]

        # 9. Stock Limits Operations Phase
        df["daily_run_rate"] = np.where(df["working_days"] > 0, df["total_sales"] / df["working_days"], 0)
        df["days_of_stock_left"] = np.where(df["daily_run_rate"] > 0, df["current_stock"] / df["daily_run_rate"], 9999)
        df["reorder_threshold_days"] = self.config.BUFFER_DAYS + df["lead_time_days"]
        df["reorder"] = np.where(df["days_of_stock_left"] <= df["reorder_threshold_days"], "YES", "NO")

        # 10. Frame Export Format
        final = (
            df[
                ["code", "name", "category", "live_on", "lead_time_label",
                 "reorder_threshold_days", "main_stock_display", "current_stock",
                 "total_sales", "working_days", "daily_run_rate", "days_of_stock_left", "reorder"]
            ]
            .rename(columns={"code": "sku_code"})
            .sort_values("days_of_stock_left")
        )
        final["sku_code"] = final["sku_code"].astype(str)
        self.logger.info("Local Transformation Routine Executed.")
        return final

    @staticmethod
    def to_json_safe(val: Any) -> Any:
        # Secure format JSON compatibility handling specifically designed for robust Google Sheet APIs
        if pd.isna(val) or val is None:
            return "" # Clear Google Cells gracefully with empty string

        if isinstance(val, (pd.Timestamp, datetime.datetime, datetime.date)):
            return str(val)

        if isinstance(val, (np.integer,)):
            return int(val)

        if isinstance(val, (np.floating,)):
            val = float(val)

        if isinstance(val, float):
            if not math.isfinite(val):
                return ""
            return val

        return val

    def upload_data(self, final: pd.DataFrame):
        """Constructs API requests to overwrite the target Master Data with updated matrix calculations."""
        self.logger.info(f"Preparing upload of {len(final)} matrix records to [SheetID: {self.config.TARGET_SPREADSHEET_ID}]")

        safe_values = [list(final.columns)]
        for row in final.itertuples(index=False):
            safe_values.append([self.to_json_safe(x) for x in row])

        try:
            sheet = self.gc.open_by_key(self.config.TARGET_SPREADSHEET_ID)
            ws = sheet.worksheet(self.config.TARGET_WORKSHEET_NAME)

            # Robust way to clear all older data logic efficiently
            if len(ws.get_all_values()) > 1:
                ws.clear()
                self.logger.info("Historical data wiped cleanly inside Master Data.")

            # Multi-API fallbacks specifically structured for broad backwards gspread flexibility inside generic deployments
            try:
                ws.update(values=safe_values, range_name="A1", value_input_option="USER_ENTERED")
            except TypeError:
                # Older GSpread implementations
                ws.update("A1", safe_values, value_input_option="USER_ENTERED")

            self.logger.info(f"SUCCESS: Operation written structurally into '{self.config.TARGET_WORKSHEET_NAME}'.")
        except Exception as e:
            self.logger.error(f"FAILURE on Upload Call: {e}")
            raise

    def send_heartbeat(self, success: bool):
        """Pings BetterStack heartbeat endpoint depending on success or failure status."""
        if not self.config.HEARTBEAT_URL:
            return

        try:
            url = self.config.HEARTBEAT_URL
            if not success:
                url = f"{url.rstrip('/')}/fail"

            self.logger.info(f"Sending heartbeat to: {url}")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            self.logger.info("Heartbeat acknowledged by BetterStack.")
        except Exception as e:
            self.logger.error(f"Failed to send heartbeat to BetterStack: {e}")

    def run(self):
        """Complete application lifecycle controller with retry and heartbeat logic."""
        start_time = time.time()
        self.logger.info("Initiating Service Application...")

        for attempt in range(1, self.config.MAX_RETRIES + 1):
            try:
                self.authenticate()
                sales, stock, sourcing = self.load_data()
                final_df = self.process_data(sales, stock, sourcing)
                self.upload_data(final_df)

                elapsed = time.time() - start_time
                self.logger.info(f"CRON LIFECYCLE COMPLETED SUCCESSFULLY IN [{elapsed:.2f}s]")
                self.send_heartbeat(success=True)
                return  # Exit function completely on success

            except Exception as e:
                self.logger.error(f"Attempt {attempt}/{self.config.MAX_RETRIES} failed: {e}")
                if attempt < self.config.MAX_RETRIES:
                    self.logger.info(f"Retrying in {self.config.RETRY_DELAY} seconds...")
                    time.sleep(self.config.RETRY_DELAY)
                else:
                    self.logger.error("CRON EXECUTION FATAL ERROR - HALTING AFTER MAX RETRIES")
                    self.send_heartbeat(success=False)
                    raise

# =========================================================
# 3. ENTRY POINT HOOKS
# =========================================================
if __name__ == "__main__":
    conf = Config()

    # Simple operational alerts before crash
    if conf.TARGET_SPREADSHEET_ID.startswith("YOUR_"):
        logger.warning(
            "==========================================================\n"
            "WARNING DOCKER ADMIN:\n"
            "Please configure the TARGET_SPREADSHEET_ID environment\n"
            "variable, currently retaining placeholder value.\n"
            "=========================================================="
        )

    engine = StockAnalyzer(conf)
    engine.run()
