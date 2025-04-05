import os
import pandas as pd
import numpy as np 
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging
import hashlib


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

SOURCE_TABLE_A = "DS_BKC_Infineon_TMP-NVD-01_BY_LOT_Daily_6AM"
SOURCE_TABLE_B = "DS_BKC_MSP_Yield_Lot"
SOURCE_TABLE_C = "DS_BKC_MD_Product"  
TARGET_TABLE = "DS_BKC_PACK_MSP"
NEW_TARGET_TABLE = "DS_BKC_TMSP_WIP"  
SHIFT_TOMORROW_CSV = "SHIFT_TOMORROW.csv"  

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
    logging.error("Database credentials missing in .env file. Please check.")
    exit()

def get_relevant_week_range_sat_fri():
    
    latest_data_date_str = "2025-04-03"
    try:
        reference_day = datetime.strptime(latest_data_date_str, '%Y-%m-%d').date()
    except ValueError:
        logging.error("Invalid date format provided. Please use YYYY-MM-DD.")
        return None, None 
    
    days_since_last_saturday = (reference_day.weekday() + 2) % 7
    start_of_week = reference_day - timedelta(days=days_since_last_saturday) 
    end_of_week = start_of_week + timedelta(days=6) 

    logging.info(f"Relevant Sat-Fri week range based on data ending {reference_day}: {start_of_week} to {end_of_week}")
    return start_of_week, end_of_week

def extract_data(engine, start_of_week, end_of_week):

    data_sources = {}
    
    try:
        # Extract data from Table A
        logging.info(f"Extracting data from {SOURCE_TABLE_A}...")
        cols_a = ['LOTID', 'PKGLD', 'PACKINGTYPE', 'QTY', 'TYPE', 'PRODUCTNAME', 'DATESTAMP']
        cols_a_str = ", ".join([f'"{col}"' for col in cols_a])
        query_a = f"""
        SELECT {cols_a_str}
        FROM "{SOURCE_TABLE_A}"
        WHERE "DATESTAMP"::date >= '{start_of_week}'
          AND "DATESTAMP"::date <= '{end_of_week}'                                                  
          AND "TYPE" = '16 P/O'
        """
        df_a = pd.read_sql(query_a, engine, parse_dates=['DATESTAMP'])
        logging.info(f"Read {len(df_a)} rows from {SOURCE_TABLE_A}")
        data_sources['table_a'] = df_a
        
        # Extract data from Table B
        logging.info(f"Extracting data from {SOURCE_TABLE_B}...")
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
        logging.info(f"Read {len(df_b)} rows from {SOURCE_TABLE_B}")
        data_sources['table_b'] = df_b
        
        # Extract data from Table C
        logging.info(f"Extracting data from {SOURCE_TABLE_C}...")
        cols_c = ['PRODUCT', 'WORKFLOWNAME', 'ISROR', 'IFXFUNCTIONALPACK', 'DEFAULTSTARTOWNER', 'ISAVAILABLE', 'IFXMANUFACTURINGLEVEL']
        cols_c_str = ", ".join([f'"{col}"' for col in cols_c])
        query_c = f"""
        SELECT {cols_c_str}
        FROM "{SOURCE_TABLE_C}"
        WHERE "ISAVAILABLE" = 'True'
          AND "IFXMANUFACTURINGLEVEL" = 'TEST'
          AND "ISROR" = 'True'
          AND "DEFAULTSTARTOWNER" = 'PROD'
        """
        df_c = pd.read_sql(query_c, engine)
        logging.info(f"Read {len(df_c)} rows from {SOURCE_TABLE_C}")
        data_sources['table_c'] = df_c
        
        # Read CSV data
        try:
            logging.info(f"Reading data from CSV file: {SHIFT_TOMORROW_CSV}...")
            df_shift = pd.read_csv(SHIFT_TOMORROW_CSV)
            logging.info(f"Read {len(df_shift)} rows from {SHIFT_TOMORROW_CSV}")
            data_sources['shift_data'] = df_shift
        except Exception as e:
            logging.error(f"Error reading CSV file {SHIFT_TOMORROW_CSV}: {e}")
            data_sources['shift_data'] = pd.DataFrame(columns=['LOT_ID'])
        
        return data_sources
        
    except Exception as e:
        logging.error(f"Error extracting data: {e}")
        return None

def transform_data(data_sources):
    """Process and transform the extracted data"""
    try:
        df_a = data_sources['table_a']
        df_b = data_sources['table_b']
        df_c = data_sources['table_c']
        df_shift = data_sources['shift_data']
        
        if df_a.empty:
            logging.warning(f"No data found in {SOURCE_TABLE_A}. Skipping transformations.")
            return None
            
        # Aggregate Table A data by LOTID
        logging.info("Aggregating Table A data by LOTID (taking MAX).")
        agg_cols_a = [col for col in df_a.columns if col != 'LOTID']
        agg_dict_a = {col: 'max' for col in agg_cols_a}
        df_a_agg = df_a.groupby('LOTID', as_index=False).agg(agg_dict_a)
        df_a_agg = df_a_agg[['LOTID'] + agg_cols_a]
        
        # Join A and C tables
        logging.info("Joining Table A and Table C...")
        df_a_c = pd.merge(
            df_a_agg,
            df_c,
            left_on=['PRODUCTNAME', 'PACKINGTYPE'],
            right_on=['PRODUCT', 'IFXFUNCTIONALPACK'],
            how='left'
        )
        
        # Join with Table B
        logging.info("Joining with Table B...")
        df_merged_ac_b = pd.merge(
            df_a_c,
            df_b,
            left_on='LOTID',
            right_on='LOT',
            how='inner'
        )
        
        if df_merged_ac_b.empty:
            logging.warning("No data after joining tables A, B, and C. Returning None.")
            return None
            
        # Join with SHIFT_TOMORROW data
        logging.info("Joining with SHIFT_TOMORROW data...")
        df_merged_final = pd.merge(
            df_merged_ac_b,
            df_shift,
            left_on='LOTID',
            right_on='LOT_ID',
            how='left'
        )
        
        # Add derived columns
        logging.info("Adding derived columns...")
        
        # Calculate DATE
        try:
            df_merged_final['DATE'] = (df_merged_final['MOVEOUT'] - pd.Timedelta(hours=6.5)).dt.date
        except Exception as e:
            logging.error(f"Error calculating DATE column: {e}")
            df_merged_final['DATE'] = pd.NaT
            
        # Calculate SHIFT
        try:
            fractional_hour = df_merged_final['MOVEOUT'].dt.hour + (df_merged_final['MOVEOUT'].dt.minute / 60.0)
            df_merged_final['SHIFT'] = np.where(
                (fractional_hour >= 6.5) & (fractional_hour <= 18.5),
                'DAY',
                'NIGHT'
            )
        except Exception as e:
            logging.error(f"Error calculating SHIFT column: {e}")
            df_merged_final['SHIFT'] = None
            
        # Calculate MACHINE_GROUP
        try:
            conditions_machine = [
                df_merged_final['MACHINE'].str.contains("PACK-TRAY", case=False, na=False),
                df_merged_final['MACHINE'].str.contains("P98TR", case=False, na=False),
                df_merged_final['MACHINE'].str.contains("P268TR", case=False, na=False),
                df_merged_final['MACHINE'].str.contains("KLA", case=False, na=False)
            ]
            choices_machine = [
                "PACK-TRAY",
                "P98TR",
                "P268TR",
                "KLA7"
            ]
            df_merged_final['MACHINE_GROUP'] = np.select(conditions_machine, choices_machine, default=None)
        except Exception as e:
            logging.error(f"Error calculating MACHINE_GROUP column: {e}")
            df_merged_final['MACHINE_GROUP'] = None
            
        # Calculate PACKINGTYPE_GROUP
        try:
            conditions_packing = [
                df_merged_final['PACKINGTYPE'].str.contains("TRAY", case=False, na=False),
                df_merged_final['PACKINGTYPE'].str.contains("TUBE", case=False, na=False),
                df_merged_final['PACKINGTYPE'].str.contains("TAPE", case=False, na=False)
            ]
            choices_packing = [
                "TRAY PACK",
                "TUBE PACK",
                "TAPE&REEL PACK"
            ]
            df_merged_final['PACKINGTYPE_GROUP'] = np.select(conditions_packing, choices_packing, default=None)
        except Exception as e:
            logging.error(f"Error calculating PACKINGTYPE_GROUP column: {e}")
            df_merged_final['PACKINGTYPE_GROUP'] = None
            
        return df_merged_final
        
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        return None

def load_data(engine, df, target_table=TARGET_TABLE, if_exists='replace'):
    """Load data to target table"""
    if df is None or df.empty:
        logging.warning(f"No data to load to {target_table}")
        return False
        
    try:
        logging.info(f"Loading data into {target_table}...")
        df.to_sql(
            target_table,
            engine,
            if_exists=if_exists,
            index=False,
            method='multi'
        )
        logging.info(f"Successfully loaded {len(df)} rows into {target_table}")
        return True
    except Exception as e:
        logging.error(f"Error loading data to {target_table}: {e}")
        return False

def calculate_data_hash(df):
    """Calculate a hash for the dataframe to detect changes"""
    if df is None or df.empty:
        return "empty_dataframe"
    
    # Sort dataframe to ensure consistent hash calculation
    try:
        df_sorted = df.sort_values(by=df.columns.tolist())
        # Convert to string and hash
        df_string = df_sorted.to_string()
        return hashlib.md5(df_string.encode()).hexdigest()
    except Exception as e:
        logging.error(f"Error calculating data hash: {e}")
        # Return a unique timestamp-based hash on error
        return f"error_hash_{datetime.now().strftime('%Y%m%d%H%M%S')}"

def has_data_changed(engine, table_name, new_data_hash):
    """Check if data has changed by comparing hashes"""
    try:
        with engine.connect() as conn:
            # Check if tracking table exists
            metadata_query = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'etl_change_tracking'
                )
            """)
            table_exists = conn.execute(metadata_query).scalar()
            
            if not table_exists:
                logging.info("Change tracking table doesn't exist yet")
                return True
                
            # Check for previous hash
            query = text("SELECT last_hash FROM etl_change_tracking WHERE table_name = :table")
            result = conn.execute(query, {"table": table_name}).fetchone()
            
            if not result:
                # No previous hash, so data is considered changed
                logging.info(f"No previous hash for {table_name}, considering as changed")
                return True
                
            previous_hash = result[0]
            has_changed = previous_hash != new_data_hash
            
            if has_changed:
                logging.info(f"Data for {table_name} has changed")
            else:
                logging.info(f"No changes detected for {table_name}")
                
            return has_changed
    except Exception as e:
        logging.error(f"Error checking data changes: {e}")
        # On error, assume data has changed to be safe
        return True

def update_change_tracking(engine, table_name, new_hash):
    """Update the hash and timestamp in the change tracking table"""
    try:
        with engine.connect() as conn:
            # Create tracking table if it doesn't exist
            create_table_query = text("""
                CREATE TABLE IF NOT EXISTS etl_change_tracking (
                    table_name VARCHAR(255) PRIMARY KEY,
                    last_hash VARCHAR(255),
                    last_updated TIMESTAMP
                )
            """)
            conn.execute(create_table_query)
            
            # Update hash information
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
            logging.info(f"Updated change tracking for {table_name}")
    except Exception as e:
        logging.error(f"Error updating change tracking: {e}")

def run_etl(target_table=None, force_update=False):

    logging.info(f"Starting ETL process{' for ' + target_table if target_table else ''}...")

    try:
        # Connect to database
        db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url)
        with engine.connect() as connection:
             logging.info("Successfully connected to the PostgreSQL database.")
             connection.execute(text("SELECT 1")) 
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return False

    try:
        start_of_week, end_of_week = get_relevant_week_range_sat_fri()
        
        data_sources = extract_data(engine, start_of_week, end_of_week)
        if not data_sources:
            logging.error("Data extraction failed")
            return False
            
        transformed_data = transform_data(data_sources)
        if transformed_data is None or transformed_data.empty:
            logging.warning("No data available after transformation")
            return False
            
        data_hash = calculate_data_hash(transformed_data)
        
        if target_table == TARGET_TABLE or target_table is None:
            load_data(engine, transformed_data, TARGET_TABLE)
        
        if target_table == NEW_TARGET_TABLE or target_table is None:
            if force_update or has_data_changed(engine, NEW_TARGET_TABLE, data_hash):
                load_status = load_data(engine, transformed_data, NEW_TARGET_TABLE)
                if load_status:
                    update_change_tracking(engine, NEW_TARGET_TABLE, data_hash)
            else:
                logging.info(f"No changes detected for {NEW_TARGET_TABLE}, skipping update")
                
        return True
    except Exception as e:
        logging.error(f"An error occurred during the ETL process: {e}", exc_info=True)
        return False
    finally:
        if 'engine' in locals() and engine:
            engine.dispose() 
            logging.info("Database connection closed.")

if __name__ == "__main__":
    run_etl()
    logging.info("ETL process finished.")