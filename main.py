from src.database.manager import DatabaseManager
from src.etl.importer import CSVImporter

def run_migration(db_instance):
    print("Starting Data Migration Process...")
    print("=" * 50)
    
    # Initialize Importer with the connected database manager
    importer = CSVImporter(db_manager=db_instance, archive_path='archive')
    
    # 1. Import Independent Tables (No Foreign Keys)
    print("\nPhase 1: Importing Independent Tables...")
    steps_phase_1 = [
        (importer.import_geolocation, "Geolocation"),
        (importer.import_category_translation, "Category Translations"),
        (importer.import_customers, "Customers"),
        (importer.import_sellers, "Sellers"),
        (importer.import_products, "Products")
    ]
    
    for func, name in steps_phase_1:
        print(f"   Importing {name}...", end="\r")
        if func():
            print(f"   {name} Imported    ")
        else:
            print(f"   Failed to import {name}")
            return False

    # 2. Import Dependent Tables (With Foreign Keys)
    print("\nPhase 2: Importing Dependent Tables...")
    steps_phase_2 = [
        (importer.import_orders, "Orders"),
        (importer.import_order_items, "Order Items"),
        (importer.import_payments, "Payments"),
        (importer.import_reviews, "Reviews"),
        (importer.import_seller_products, "Seller Products")
    ]
    
    for func, name in steps_phase_2:
        print(f"   Importing {name}...", end="\r")
        if func():
            print(f"   {name} Imported    ")
        else:
            print(f"   Failed to import {name}")
            return False
            
    print("\n" + "=" * 50)
    print("Migration Completed Successfully!")
    return True

if __name__ == "__main__":
    # Initialize and connect to database
    db = DatabaseManager()
    
    print("Connecting to database...")
    if db.connect():
        # Create tables
        print("Ensuring tables exist (Recreating to apply schema fixes)...")
        # Force recreation of tables to apply schema changes (e.g. removing unique constraint)
        if db.create_tables(drop_existing=True):
            # Run migration
            success = run_migration(db)
            if success:
                print("\nDatabase is ready for use!")
            else:
                print("\nMigration completed with errors.")
        else:
            print("\nFailed to create tables.")
    else:
        print("\nDatabase connection failed. Check Docker container.")
