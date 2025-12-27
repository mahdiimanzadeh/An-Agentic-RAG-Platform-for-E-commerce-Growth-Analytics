from src.database.manager import DatabaseManager

def check_status():
    db = DatabaseManager()
    print("Testing connection...")
    if db.connect():
        print("Connection successful.")
        status = db.test_connection()
        print(f"Status: {status}")
        
        info = db.get_table_info()
        print("Table Info:")
        for table, count in info.items():
            print(f" - {table}: {count} rows")
        
        db.close_connection()
    else:
        print("Connection failed.")

if __name__ == "__main__":
    check_status()
