import os
import pandas as pd
import numpy as np
from typing import Dict
from src.database.manager import DatabaseManager
from src.models.models import (
    Customer, Product, Seller, Order, OrderItem, 
    Payment, Review, Geolocation, CategoryTranslation, SellerProduct
)

class CSVImporter:
    """
    Comprehensive class for importing CSV data into the database
    """
    
    def __init__(self, db_manager: DatabaseManager, archive_path: str):
        """
        Initialize CSV Importer
        
        Args:
            db_manager: DatabaseManager instance
            archive_path: Path to archive folder
        """
        self.db_manager = db_manager
        self.archive_path = archive_path
        self.logger = db_manager.logger
        
        # File settings
        self.csv_files = {
            'customers': 'olist_customers_dataset.csv',
            'orders': 'olist_orders_dataset.csv',
            'products': 'olist_products_dataset.csv',
            'order_items': 'olist_order_items_dataset.csv',
            'sellers': 'olist_sellers_dataset.csv',
            'payments': 'olist_order_payments_dataset.csv',
            'reviews': 'olist_order_reviews_dataset.csv',
            'geolocation': 'olist_geolocation_dataset.csv',
            'category_translation': 'product_category_name_translation.csv'
        }
    
    def clean_and_prepare_data(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Clean and prepare data for import
        
        Args:
            df: Raw DataFrame
            table_name: Target table name
            
        Returns:
            Cleaned DataFrame
        """
        df_clean = df.copy()
        
        # Convert datetime columns
        datetime_columns = {
            'orders': [
                'order_purchase_timestamp',
                'order_approved_at', 
                'order_delivered_carrier_date',
                'order_delivered_customer_date',
                'order_estimated_delivery_date'
            ],
            'order_items': ['shipping_limit_date'],
            'reviews': ['review_creation_date', 'review_answer_timestamp']
        }
        
        if table_name in datetime_columns:
            for col in datetime_columns[table_name]:
                if col in df_clean.columns:
                    df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
        
        # Convert numeric columns
        numeric_columns = {
            'products': [
                'product_name_length', 'product_description_length',
                'product_photos_qty', 'product_weight_g', 'product_length_cm',
                'product_height_cm', 'product_width_cm'
            ],
            'order_items': ['order_item_id', 'price', 'freight_value'],
            'payments': ['payment_sequential', 'payment_installments', 'payment_value'],
            'reviews': ['review_score'],
            'geolocation': ['geolocation_lat', 'geolocation_lng']
        }
        
        if table_name in numeric_columns:
            for col in numeric_columns[table_name]:
                if col in df_clean.columns:
                    # Handle typo in products dataset if present
                    if table_name == 'products':
                        if col == 'product_name_length' and 'product_name_lenght' in df_clean.columns:
                             df_clean.rename(columns={'product_name_lenght': 'product_name_length'}, inplace=True)
                        if col == 'product_description_length' and 'product_description_lenght' in df_clean.columns:
                             df_clean.rename(columns={'product_description_lenght': 'product_description_length'}, inplace=True)
                    
                    if col in df_clean.columns:
                        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        
        # Remove null values in primary keys
        primary_keys = {
            'customers': 'customer_id',
            'orders': 'order_id',
            'products': 'product_id',
            'sellers': 'seller_id',
            'reviews': 'review_id'
        }
        
        if table_name in primary_keys:
            pk_col = primary_keys[table_name]
            if pk_col in df_clean.columns:
                df_clean = df_clean.dropna(subset=[pk_col])
        
        df_clean = df_clean.replace([np.inf, -np.inf], np.nan)
        
        # Convert ALL columns to object to ensure we can store None
        df_clean = df_clean.astype(object)
        
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        
        return df_clean
    
    def import_customers(self) -> bool:
        """Import customers table"""
        file_path = os.path.join(self.archive_path, self.csv_files['customers'])
        df = pd.read_csv(file_path)
        df_clean = self.clean_and_prepare_data(df, 'customers')
        
        with self.db_manager.get_db_session() as session:
            for _, row in df_clean.iterrows():
                customer = Customer(
                    customer_id=row['customer_id'],
                    customer_unique_id=row['customer_unique_id'],
                    customer_zip_code_prefix=str(row['customer_zip_code_prefix']),
                    customer_city=row['customer_city'],
                    customer_state=row['customer_state']
                )
                session.add(customer)
            
        self.logger.info(f"{len(df_clean)} customers imported")
        return True
    
    def import_products(self) -> bool:
        """Import products table"""
        file_path = os.path.join(self.archive_path, self.csv_files['products'])
        df = pd.read_csv(file_path)
        df_clean = self.clean_and_prepare_data(df, 'products')
        
        with self.db_manager.get_db_session() as session:
            for _, row in df_clean.iterrows():
                # Handle potential column name mismatches (typos in CSV)
                name_len = row.get('product_name_length') if 'product_name_length' in row else row.get('product_name_lenght')
                desc_len = row.get('product_description_length') if 'product_description_length' in row else row.get('product_description_lenght')
                
                product = Product(
                    product_id=row['product_id'],
                    product_category_name=row.get('product_category_name'),
                    product_name_length=name_len,
                    product_description_length=desc_len,
                    product_photos_qty=row.get('product_photos_qty'),
                    product_weight_g=row.get('product_weight_g'),
                    product_length_cm=row.get('product_length_cm'),
                    product_height_cm=row.get('product_height_cm'),
                    product_width_cm=row.get('product_width_cm')
                )
                session.add(product)
        
        self.logger.info(f"{len(df_clean)} products imported")
        return True
    
    def import_sellers(self) -> bool:
        """Import sellers table"""
        file_path = os.path.join(self.archive_path, self.csv_files['sellers'])
        df = pd.read_csv(file_path)
        df_clean = self.clean_and_prepare_data(df, 'sellers')
        
        with self.db_manager.get_db_session() as session:
            for _, row in df_clean.iterrows():
                seller = Seller(
                    seller_id=row['seller_id'],
                    seller_zip_code_prefix=str(row['seller_zip_code_prefix']),
                    seller_city=row['seller_city'],
                    seller_state=row['seller_state']
                )
                session.add(seller)
        
        self.logger.info(f"{len(df_clean)} sellers imported")
        return True
    
    def import_orders(self) -> bool:
        """Import orders table"""
        file_path = os.path.join(self.archive_path, self.csv_files['orders'])
        df = pd.read_csv(file_path)
        df_clean = self.clean_and_prepare_data(df, 'orders')
        
        with self.db_manager.get_db_session() as session:
            for _, row in df_clean.iterrows():
                order = Order(
                    order_id=row['order_id'],
                    customer_id=row['customer_id'],
                    order_status=row['order_status'],
                    order_purchase_timestamp=row.get('order_purchase_timestamp'),
                    order_approved_at=row.get('order_approved_at'),
                    order_delivered_carrier_date=row.get('order_delivered_carrier_date'),
                    order_delivered_customer_date=row.get('order_delivered_customer_date'),
                    order_estimated_delivery_date=row.get('order_estimated_delivery_date')
                )
                session.add(order)
        
        self.logger.info(f"{len(df_clean)} orders imported")
        return True

    def import_order_items(self) -> bool:
        """Import order_items table"""
        file_path = os.path.join(self.archive_path, self.csv_files['order_items'])
        df = pd.read_csv(file_path)
        df_clean = self.clean_and_prepare_data(df, 'order_items')
        
        with self.db_manager.get_db_session() as session:
            for _, row in df_clean.iterrows():
                order_item = OrderItem(
                    order_id=row['order_id'],
                    order_item_id=row['order_item_id'],
                    product_id=row['product_id'],
                    seller_id=row['seller_id'],
                    shipping_limit_date=row.get('shipping_limit_date'),
                    price=row.get('price'),
                    freight_value=row.get('freight_value')
                )
                session.add(order_item)
        
        self.logger.info(f"{len(df_clean)} order items imported")
        return True

    def import_payments(self) -> bool:
        """Import payments table"""
        file_path = os.path.join(self.archive_path, self.csv_files['payments'])
        df = pd.read_csv(file_path)
        df_clean = self.clean_and_prepare_data(df, 'payments')
        
        with self.db_manager.get_db_session() as session:
            for _, row in df_clean.iterrows():
                payment = Payment(
                    order_id=row['order_id'],
                    payment_sequential=row['payment_sequential'],
                    payment_type=row['payment_type'],
                    payment_installments=row['payment_installments'],
                    payment_value=row['payment_value']
                )
                session.add(payment)
        
        self.logger.info(f"{len(df_clean)} payments imported")
        return True

    def import_reviews(self) -> bool:
        """Import reviews table"""
        file_path = os.path.join(self.archive_path, self.csv_files['reviews'])
        df = pd.read_csv(file_path)
        df_clean = self.clean_and_prepare_data(df, 'reviews')
        
        with self.db_manager.get_db_session() as session:
            for _, row in df_clean.iterrows():
                review = Review(
                    review_id=row['review_id'],
                    order_id=row['order_id'],
                    review_score=row.get('review_score'),
                    review_comment_title=row.get('review_comment_title'),
                    review_comment_message=row.get('review_comment_message'),
                    review_creation_date=row.get('review_creation_date'),
                    review_answer_timestamp=row.get('review_answer_timestamp')
                )
                session.add(review)
        
        self.logger.info(f"{len(df_clean)} reviews imported")
        return True

    def import_geolocation(self) -> bool:
        """Import geolocation table"""
        file_path = os.path.join(self.archive_path, self.csv_files['geolocation'])
        df = pd.read_csv(file_path)
        df_clean = self.clean_and_prepare_data(df, 'geolocation')
        
        # Remove duplicates based on zip_code
        df_clean = df_clean.drop_duplicates(subset=['geolocation_zip_code_prefix'])
        
        with self.db_manager.get_db_session() as session:
            for _, row in df_clean.iterrows():
                geolocation = Geolocation(
                    geolocation_zip_code_prefix=str(row['geolocation_zip_code_prefix']),
                    geolocation_lat=row.get('geolocation_lat'),
                    geolocation_lng=row.get('geolocation_lng'),
                    geolocation_city=row.get('geolocation_city'),
                    geolocation_state=row.get('geolocation_state')
                )
                session.add(geolocation)
        
        self.logger.info(f"{len(df_clean)} geolocations imported")
        return True

    def import_category_translation(self) -> bool:
        """Import category_translation table"""
        file_path = os.path.join(self.archive_path, self.csv_files['category_translation'])
        df = pd.read_csv(file_path)
        df_clean = self.clean_and_prepare_data(df, 'category_translation')
        
        with self.db_manager.get_db_session() as session:
            for _, row in df_clean.iterrows():
                category = CategoryTranslation(
                    product_category_name=row['product_category_name'],
                    product_category_name_english=row['product_category_name_english']
                )
                session.add(category)
        
        self.logger.info(f"{len(df_clean)} category translations imported")
        return True
    
    def import_seller_products(self) -> bool:
        """Derive seller-product relationships from order_items"""
        file_path = os.path.join(self.archive_path, self.csv_files['order_items'])
        df = pd.read_csv(file_path)
        
        # Extract unique pairs of seller_id and product_id
        df_unique = df[['seller_id', 'product_id']].drop_duplicates()
        
        with self.db_manager.get_db_session() as session:
            for _, row in df_unique.iterrows():
                seller_product = SellerProduct(
                    seller_id=row['seller_id'],
                    product_id=row['product_id']
                )
                session.add(seller_product)
        
        self.logger.info(f"{len(df_unique)} seller-product relationships imported")
        return True
    
    def import_all_data(self) -> Dict[str, bool]:
        """
        Import all data in proper order
        
        Returns:
            Dict: Import result for each table
        """
        results = {}
        
        # Import order based on foreign key dependencies
        import_order = [
            ('customers', self.import_customers),
            ('products', self.import_products),
            ('sellers', self.import_sellers),
            ('orders', self.import_orders),
            ('order_items', self.import_order_items),
            ('payments', self.import_payments),
            ('reviews', self.import_reviews),
            ('geolocation', self.import_geolocation),
            ('category_translation', self.import_category_translation),
            ('seller_products', self.import_seller_products)
        ]
        
        self.logger.info("Starting import of all data...")
        
        for table_name, import_func in import_order:
            self.logger.info(f"Importing {table_name}...")
            results[table_name] = import_func()
            
            if not results[table_name]:
                self.logger.warning(f"Error importing {table_name} - continuing with next table")
        
        return results
