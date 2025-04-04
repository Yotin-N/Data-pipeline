import os
import pandas as pd
import numpy as np 
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

SOURCE_TABLE_A = "DS_BKC_Infineon_TMP-NVD-01_BY_LOT_Daily_6AM"
SOURCE_TABLE_B = "DS_BKC_MSP_Yield_Lot"
TARGET_TABLE = "DS_BKC_PACK_MSP"

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
    logging.error("Database credentials missing in .env file. Please check.")
    exit()


def get_current_week_range_sat_fri():
    """
    Calculates the start (Saturday) and end date (Friday) of the week
    containing the current day.
    """
    today = datetime.now().date()
    # Calculate days to subtract to get to the *previous* Saturday
    # today.weekday(): Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
    # We want Saturday (5) to be day 0 of our target week.
    # Offset: Sat=0, Sun=1, Mon=2, Tue=3, Wed=4, Thu=5, Fri=6
    days_since_last_saturday = (today.weekday() + 2) % 7
    start_of_week = today - timedelta(days=days_since_last_saturday) # Saturday
    end_of_week = start_of_week + timedelta(days=6) # Friday
    logging.info(f"Current Sat-Fri week range: {start_of_week} to {end_of_week}")
    return start_of_week, end_of_week

def run_etl():
    logging.info("Starting ETL process...")

    try:
        db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url)
        with engine.connect() as connection:
             logging.info("Successfully connected to the PostgreSQL database.")
             connection.execute(text("SELECT 1")) 
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return

    try:
        
        start_of_week, end_of_week = get_current_week_range_sat_fri()

        # Extrac and Transform Data from Table A ---
        logging.info(f"Extracting and transforming data from {SOURCE_TABLE_A}...")
        cols_a = ['LOTID', 'PKGLD', 'PACKINGTYPE', 'QTY', 'TYPE', 'PRODUCTNAME', 'DATESTAMP']
        cols_a_str = ", ".join([f'"{col}"' for col in cols_a])
        query_a = f"""
        SELECT {cols_a_str}
        FROM "{SOURCE_TABLE_A}"
        WHERE "DATESTAMP"::date >= '{start_of_week}'
          AND "DATESTAMP"::date <= '{end_of_week}'                                                  
          AND "TYPE" = '16 P/O' -- Filter TYPE in SQL for efficiency
        """
      

        df_a = pd.read_sql(query_a, engine, parse_dates=['DATESTAMP'])
        logging.info(f"Read {len(df_a)} rows from {SOURCE_TABLE_A} matching criteria.")

        if df_a.empty:
            logging.warning(f"No data found in {SOURCE_TABLE_A} for the specified criteria. Skipping aggregation.")
            df_a_agg = pd.DataFrame(columns=cols_a)
        else:
            # Aggregate (Group fields : LOTID) by remaining value is MAX
            logging.info("Aggregating Table A data by LOTID (taking MAX).")
            agg_cols_a = [col for col in df_a.columns if col != 'LOTID']
            agg_dict_a = {col: 'max' for col in agg_cols_a}
            df_a_agg = df_a.groupby('LOTID', as_index=False).agg(agg_dict_a)
            df_a_agg = df_a_agg[['LOTID'] + agg_cols_a] 
            logging.info(f"Aggregated Table A data into {len(df_a_agg)} rows.")

        # Extract and Transform Data from Table B ---
        logging.info(f"Extracting and transforming data from {SOURCE_TABLE_B}...")
        cols_b = ['LOT', 'MOVEOUT', 'SPECNAME', 'MACHINE']
        cols_b_str = ", ".join([f'"{col}"' for col in cols_b])
        specname_filter = ['Pack', 'Pack1', 'Pack1_TR', 'PACK_TR']
        query_b = f"""
        SELECT {cols_b_str}
        FROM "{SOURCE_TABLE_B}"
        WHERE "MOVEOUT"::date >= '{start_of_week}'
          AND "MOVEOUT"::date <= '{end_of_week}'
          AND "SPECNAME" IN ({", ".join([f"'{name}'" for name in specname_filter])})
        """

        df_b = pd.read_sql(query_b, engine, parse_dates=['MOVEOUT'])
        logging.info(f"Read {len(df_b)} rows from {SOURCE_TABLE_B} matching criteria.")

        if df_b.empty:
            logging.warning(f"No data found in {SOURCE_TABLE_B} matching the criteria. Join will likely be empty.")
            # Keep df_b as is (empty df)

        # --- 4. Join A and B ---
        logging.info("Joining transformed data from Table A and Table B...")
        # Ensure LOTID is string if LOT is string, or vice versa, if necessary. Check dtypes if join fails.
        # Example: df_a_agg['LOTID'] = df_a_agg['LOTID'].astype(str)
        # Example: df_b['LOT'] = df_b['LOT'].astype(str)
        df_merged = pd.merge(
            df_a_agg,
            df_b, # Use df_b directly as filtering is done in SQL
            left_on='LOTID',
            right_on='LOT',
            how='inner'
        )
        logging.info(f"Joined data resulted in {len(df_merged)} rows.")

        if df_merged.empty:
            logging.warning("Joined data is empty. No data will be processed further or inserted.")
            return # Exit if join results in no data

        # --- Add Derived Columns (Requirement 3) ---
        logging.info("Adding derived columns: DATE, SHIFT, MACHINE_GROUP, PACKINGTYPE_GROUP")

        # 3.1 "DATE" column: DATE(DATEPARSE("yyyy-MM-dd HH:mm:ss", STR([MOVEOUT]-6.5/24)))
        # Subtract 6.5 hours from MOVEOUT and take the date part
        try:
            df_merged['DATE'] = (df_merged['MOVEOUT'] - pd.Timedelta(hours=6.5)).dt.date
        except TypeError as e:
             logging.error(f"Error calculating 'DATE' column. Is 'MOVEOUT' a datetime column? Error: {e}")
             # Handle error appropriately, maybe skip this row/column or exit
             df_merged['DATE'] = pd.NaT # Assign Not-a-Time for error cases


        # 3.2 "SHIFT" column: IF DATEPART('hour', [MOVEOUT]) >= 6.5 AND DATEPART('hour', [MOVEOUT]) <= 18.5 THEN "DAY" ELSE "NIGHT"
        try:
            # Calculate fractional hour (hour + minute/60)
            fractional_hour = df_merged['MOVEOUT'].dt.hour + (df_merged['MOVEOUT'].dt.minute / 60.0)
            # Apply condition using np.where
            df_merged['SHIFT'] = np.where(
                (fractional_hour >= 6.5) & (fractional_hour <= 18.5),
                'DAY',
                'NIGHT'
            )
        except TypeError as e:
             logging.error(f"Error calculating 'SHIFT' column. Is 'MOVEOUT' a datetime column? Error: {e}")
             df_merged['SHIFT'] = None # Assign None for error cases


        # 3.3 "MACHINE_GROUP" column
        try:
            # Define conditions and choices for np.select
            conditions_machine = [
                df_merged['MACHINE'].str.contains("PACK-TRAY", case=False, na=False),
                df_merged['MACHINE'].str.contains("P98TR", case=False, na=False),
                df_merged['MACHINE'].str.contains("P268TR", case=False, na=False),
                df_merged['MACHINE'].str.contains("KLA", case=False, na=False) # Assuming KLA means KLA7 based on example
            ]
            choices_machine = [
                "PACK-TRAY",
                "P98TR",
                "P268TR",
                "KLA7"
            ]
            # Apply np.select. `default=None` means if no condition matches, assign None.
            # Use 'OTHER' or np.nan if preferred.
            df_merged['MACHINE_GROUP'] = np.select(conditions_machine, choices_machine, default=None)
        except KeyError as e:
            logging.error(f"Error creating 'MACHINE_GROUP'. Is 'MACHINE' column present after join? Error: {e}")
            df_merged['MACHINE_GROUP'] = None


        # 3.4 "PACKINGTYPE_GROUP" column
        try:
            # Define conditions and choices for np.select
            conditions_packing = [
                df_merged['PACKINGTYPE'].str.contains("TRAY", case=False, na=False),
                df_merged['PACKINGTYPE'].str.contains("TUBE", case=False, na=False),
                df_merged['PACKINGTYPE'].str.contains("TAPE", case=False, na=False) # Assumes "TAPE" implies "TAPE&REEL"
            ]
            choices_packing = [
                "TRAY PACK",
                "TUBE PACK",
                "TAPE&REEL PACK"
            ]
            # Apply np.select
            df_merged['PACKINGTYPE_GROUP'] = np.select(conditions_packing, choices_packing, default=None)
        except KeyError as e:
            logging.error(f"Error creating 'PACKINGTYPE_GROUP'. Is 'PACKINGTYPE' column present after join? Error: {e}")
            df_merged['PACKINGTYPE_GROUP'] = None


        # --- 5. Load Data to Target Table ---
        # Optional: Select/Reorder columns for the final table if needed
        # final_columns = ['LOTID', 'PKGLD', ..., 'DATE', 'SHIFT', 'MACHINE_GROUP', 'PACKINGTYPE_GROUP'] # Define the exact order/selection
        # df_to_load = df_merged[final_columns]
        df_to_load = df_merged # Or load all columns from the merge + derived ones

        logging.info(f"Loading data into target table: {TARGET_TABLE}...")
        df_to_load.to_sql(
            TARGET_TABLE,
            engine,
            if_exists='replace', # IMPORTANT: This drops and recreates the table. Use 'append' to add.
            index=False,
            method='multi'
            # Consider chunksize=10000 for very large dataframes
        )
        logging.info(f"Successfully loaded {len(df_to_load)} rows into {TARGET_TABLE}.")

    except Exception as e:
        logging.error(f"An error occurred during the ETL process: {e}", exc_info=True) # Log traceback
    finally:
        if 'engine' in locals() and engine:
            engine.dispose() # Close db connections
            logging.info("Database connection closed.")

# --- Scheduling Information ---
# This script needs to be scheduled externally to run daily at 6 AM.
# Option 1: Linux/macOS using cron
#   1. Open crontab: `crontab -e`
#   2. Add a line like this (adjust python path and script path):
#      `0 6 * * * /usr/bin/python3 /path/to/your/etl_script.py >> /path/to/your/etl_log.log 2>&1`
#      This runs at 6:00 AM every day. `>> ... 2>&1` redirects output and errors to a log file.
# Option 2: Windows using Task Scheduler
#   1. Open Task Scheduler.
#   2. Create a new Basic Task.
#   3. Set the Trigger to "Daily" and specify 6:00:00 AM.
#   4. Set the Action to "Start a program".
#   5. Program/script: Point to your Python executable (e.g., C:\Python39\python.exe).
#   6. Add arguments: Point to your script file (e.g., C:\path\to\your\etl_script.py).
#   7. Start in (optional): Set the directory where the script and .env file reside.
#   8. Configure other settings as needed (e.g., run whether user is logged on or not).

# --- Run the ETL ---
if __name__ == "__main__":
    run_etl()
    logging.info("ETL process finished.")