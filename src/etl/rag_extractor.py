import pandas as pd
import os
from src.database.manager import DatabaseManager

class RAGDataExtractor:
    """
    Class for extracting and preparing text data for the RAG agent.
    """

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = db_manager.logger

    def extract_reviews(self, limit: int = 1000, output_path: str = 'rag_text_data.csv') -> pd.DataFrame:
        """
        Extract review comments and titles for RAG processing.
        """
        query = f"""
        SELECT 
            r.review_id,
            r.review_comment_title,
            r.review_comment_message,
            r.review_score,
            p.product_category_name
        FROM reviews r
        JOIN orders o ON r.order_id = o.order_id
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN products p ON oi.product_id = p.product_id
        WHERE r.review_comment_message IS NOT NULL
        LIMIT {limit};
        """
        
        try:
            df = pd.read_sql(query, self.db_manager.engine)
            self.logger.info(f"Extracted {len(df)} reviews for RAG processing")
            
            if not df.empty:
                df.to_csv(output_path, index=False)
                self.logger.info(f"Saved extracted text to '{output_path}'")
            else:
                self.logger.warning("No reviews found to extract.")
                
            return df
        except Exception as e:
            self.logger.error(f"Error extracting reviews: {e}")
            return pd.DataFrame()
