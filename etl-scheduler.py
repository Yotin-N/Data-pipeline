import os
import time
import schedule
import hashlib
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, DateTime, select
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta
import importlib.util
import sys
from pathlib import Path

# Configure logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"etl_scheduler_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("ETL_Scheduler")

# Load environment variables
load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

# Import the ETL module
def import_etl_module(script_path="etl_processer.py"):
    logger.info(f"Importing ETL module from {script_path}")
    try:
        spec = importlib.util.spec_from_file_location("etl_module", script_path)
        etl_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(etl_module)
        return etl_module
    except Exception as e:
        logger.error(f"Failed to import ETL module: {e}")
        return None

# Database connection
def get_db_connection():
    db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    try:
        engine = create_engine(db_url)
        logger.info("Database connection established")
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return None

# Metadata table to track changes
def create_change_tracking_table(engine):
    metadata = MetaData()
    
    if not engine.dialect.has_table(engine, "etl_change_tracking"):
        change_tracking = Table(
            "etl_change_tracking", metadata,
            Column("table_name", String, primary_key=True),
            Column("last_hash", String),
            Column("last_updated", DateTime)
        )
        metadata.create_all(engine)
        logger.info("Created change tracking table")
    
    logger.info("Change tracking table check completed")

def calculate_data_hash(df):
    if df.empty:
        return "empty_dataframe"
    
    df_sorted = df.sort_values(by=df.columns.tolist())
    
    df_string = df_sorted.to_string()
    return hashlib.md5(df_string.encode()).hexdigest()

def has_data_changed(engine, table_name, new_data_hash):
    """Check if data has changed by comparing hashes"""
    try:
        with engine.connect() as conn:
            query = text("SELECT last_hash FROM etl_change_tracking WHERE table_name = :table")
            result = conn.execute(query, {"table": table_name}).fetchone()
            
            if not result:
                # No previous hash, so data is considered changed
                logger.info(f"No previous hash for {table_name}, considering as changed")
                return True
                
            previous_hash = result[0]
            has_changed = previous_hash != new_data_hash
            logger.info(f"Data change check for {table_name}: {'Changed' if has_changed else 'No change'}")
            return has_changed
    except Exception as e:
        logger.error(f"Error checking data changes: {e}")
        # On error, assume data has changed to be safe
        return True

# Function to update change tracking
def update_change_tracking(engine, table_name, new_hash):
    """Update the hash and timestamp in the change tracking table"""
    try:
        with engine.connect() as conn:
            query = text("""
                INSERT INTO etl_change_tracking (table_name, last_hash, last_updated)
                VALUES (:table, :hash, :updated)
                ON CONFLICT (table_name) 
                DO UPDATE SET last_hash = :hash, last_updated = :updated
            """)
            conn.execute(query, {
                "table": table_name,
                "hash": new_hash,
                "updated": datetime.now()
            })
            conn.commit()
            logger.info(f"Updated change tracking for {table_name}")
    except Exception as e:
        logger.error(f"Error updating change tracking: {e}")

# Full ETL job (run at 6 AM) - updates both tables
def full_etl_job():
    logger.info("Starting full ETL job (6 AM)")
    etl_module = import_etl_module()
    if etl_module:
        try:
            etl_module.run_etl()
            logger.info("Full ETL job completed successfully")
        except Exception as e:
            logger.error(f"Full ETL job failed: {e}")
    else:
        logger.error("ETL module not available, skipping full job")

# Hourly job with lazy loading for DS_BKC_TMSP_WIP
def hourly_lazy_etl_job():
    logger.info("Starting hourly ETL job with lazy loading")
    engine = get_db_connection()
    if not engine:
        logger.error("Database connection failed, skipping hourly job")
        return
    
    etl_module = import_etl_module()
    if not etl_module:
        logger.error("ETL module not available, skipping hourly job")
        return
    
    try:
        create_change_tracking_table(engine)
        
        # Step 1: Extract the data but don't load it yet
        start_of_week, end_of_week = etl_module.get_relevant_week_range_sat_fri()
        
        
        try:
            # Extract data from SOURCE_TABLE_A
            query_a = f"""
            SELECT * FROM "{etl_module.SOURCE_TABLE_A}"
            WHERE "DATESTAMP"::date >= '{start_of_week}'
              AND "DATESTAMP"::date <= '{end_of_week}'
              AND "TYPE" = '16 P/O'
            """
            df_a = pd.read_sql(query_a, engine)
            
            # Extract data from SOURCE_TABLE_B
            specname_filter = ['Pack', 'Pack1', 'Pack1_TR', 'PACK_TR']
            query_b = f"""
            SELECT * FROM "{etl_module.SOURCE_TABLE_B}"
            WHERE "MOVEOUT"::date >= '{start_of_week}'
              AND "MOVEOUT"::date <= '{end_of_week}'
              AND "SPECNAME" IN ({", ".join([f"'{name}'" for name in specname_filter])})
            """
            df_b = pd.read_sql(query_b, engine)
            
            # Extract data from SOURCE_TABLE_C
            query_c = f"""
            SELECT * FROM "{etl_module.SOURCE_TABLE_C}"
            WHERE "ISAVAILABLE" = 'True'
              AND "IFXMANUFACTURINGLEVEL" = 'TEST'
              AND "ISROR" = 'True'
              AND "DEFAULTSTARTOWNER" = 'PROD'
            """
            df_c = pd.read_sql(query_c, engine)
            
            # Step 2: Calculate a hash of the combined source data
            # This is a simplified approach - in practice, you might need a more sophisticated method
            combined_hash = hashlib.md5((
                calculate_data_hash(df_a) + 
                calculate_data_hash(df_b) + 
                calculate_data_hash(df_c)
            ).encode()).hexdigest()
            
            # Step 3: Check if data has changed
            if has_data_changed(engine, etl_module.NEW_TARGET_TABLE, combined_hash):
                logger.info("Data has changed, running ETL for DS_BKC_TMSP_WIP")
                
                # Read CSV data - this should be extracted to a function in your main ETL module
                try:
                    df_shift = pd.read_csv(etl_module.SHIFT_TOMORROW_CSV)
                except Exception as e:
                    logger.error(f"Error reading shift data: {e}")
                    df_shift = pd.DataFrame()
                
                etl_module.run_etl()
                
                # Update the tracking table with the new hash
                update_change_tracking(engine, etl_module.NEW_TARGET_TABLE, combined_hash)
                logger.info("DS_BKC_TMSP_WIP updated successfully")
            else:
                logger.info("No changes detected, skipping DS_BKC_TMSP_WIP update")
                
        except Exception as e:
            logger.error(f"Error during data extraction/comparison: {e}")
            # Run full ETL as fallback if the lazy loading logic fails
            logger.info("Running full ETL as fallback")
            etl_module.run_etl()
    except Exception as e:
        logger.error(f"Hourly ETL job failed: {e}")
    finally:
        if 'engine' in locals() and engine:
            engine.dispose()

# Schedule jobs
def setup_schedule():
    # Full ETL job at 6 AM daily
    schedule.every().day.at("06:00").do(full_etl_job)
    
    # Hourly job for incremental updates
    schedule.every().hour.do(hourly_lazy_etl_job)
    
    logger.info("Scheduler initialized with jobs")
    logger.info("Full ETL job scheduled for 6 AM daily")
    logger.info("Incremental ETL job scheduled hourly")

def run_scheduler():
    setup_schedule()
    
    logger.info("Scheduler running...")
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    try:
        run_scheduler()
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
    except Exception as e:
        logger.error(f"Scheduler failed: {e}")