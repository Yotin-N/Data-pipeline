import pandas as pd
from sqlalchemy import create_engine
import sys
import getpass
import os

def import_excel_to_postgres():
    # Get input parameters
    if len(sys.argv) < 2:
        excel_file = input("Enter Excel file path: ")
    else:
        excel_file = sys.argv[1]
    
    # Database connection parameters
    db_name = input("Enter database name: ")
    db_user = input("Enter database username: ")
    db_password = getpass.getpass("Enter database password: ")
    db_host = input("Enter database host (default: localhost): ") or "localhost"
    db_port = input("Enter database port (default: 5432): ") or "5432"
    
    # Table name - default to Excel filename without extension
    default_table = os.path.splitext(os.path.basename(excel_file))[0]
    table_name = input(f"Enter table name (default: {default_table}): ") or default_table
    
    # Schema name
    schema = input("Enter schema name (default: public): ") or "public"
    
    print(f"\nReading Excel file: {excel_file}")
    try:
        # Read Excel file
        df = pd.read_csv(excel_file)
        
        # Show sample of the data
        print("\nSample data (first 5 rows):")
        print(df.head())
        
        # Show inferred data types
        print("\nInferred column data types:")
        for col, dtype in df.dtypes.items():
            print(f"  {col}: {dtype}")
        
        # Connect to PostgreSQL
        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        print(f"\nConnecting to PostgreSQL database: {db_name} on {db_host}:{db_port}")
        engine = create_engine(connection_string)
        
        # Ask if user wants to proceed
        proceed = input("\nProceed with table creation? (y/n): ").lower()
        if proceed != 'y':
            print("Operation cancelled")
            return
        
        # Upload to PostgreSQL with automatic type detection
        print(f"\nCreating table '{schema}.{table_name}' with {len(df.columns)} columns...")
        df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)
        
        # Confirm success and show row count
        print(f"Success! Imported {len(df)} rows into {schema}.{table_name}")
        
        # Show SQL to view the table
        print(f"\nTo view your table, run this SQL command:")
        print(f"SELECT * FROM {schema}.{table_name} LIMIT 10;")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    import_excel_to_postgres()
