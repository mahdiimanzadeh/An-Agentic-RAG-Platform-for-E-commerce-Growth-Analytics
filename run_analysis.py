import logging
from src.database.manager import DatabaseManager
from src.analysis.analyzer import ECommerceAnalyzer
from src.etl.rag_extractor import RAGDataExtractor

def main():
    # Initialize Database Manager
    db_manager = DatabaseManager()
    
    if not db_manager.connect():
        print("Failed to connect to the database. Exiting.")
        return

    # Run Exploratory Data Analysis
    print("Starting Exploratory Data Analysis...")
    analyzer = ECommerceAnalyzer(db_manager)
    
    print("Analyzing Customer Demographics...")
    analyzer.analyze_customer_demographics()
    
    print("Analyzing Product Categories...")
    analyzer.analyze_product_categories()
    
    print("Analyzing Order Trends...")
    analyzer.analyze_order_trends()
    
    print("EDA Completed. Check 'analysis_outputs' directory for plots.")

    # Run RAG Data Extraction
    print("\nStarting RAG Data Extraction...")
    extractor = RAGDataExtractor(db_manager)
    extractor.extract_reviews(limit=1000, output_path='rag_text_data.csv')
    
    print("RAG Data Extraction Completed.")

if __name__ == "__main__":
    main()
