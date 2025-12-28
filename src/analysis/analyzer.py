import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from src.database.manager import DatabaseManager

class ECommerceAnalyzer:
    """
    Class for performing Exploratory Data Analysis (EDA) on the E-commerce dataset.
    """

    def __init__(self, db_manager: DatabaseManager, output_dir: str = "analysis_outputs"):
        
        self.db_manager = db_manager
        self.output_dir = output_dir
        self.logger = db_manager.logger
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            
        # Set plot style
        sns.set_theme(style="whitegrid")
        plt.rcParams['figure.figsize'] = (12, 6)

    def analyze_customer_demographics(self):
        """
        Analyze and visualize customer distribution by state.
        """
        query = """
        SELECT customer_state, COUNT(*) as customer_count
        FROM customers
        GROUP BY customer_state
        ORDER BY customer_count DESC
        LIMIT 10;
        """
        
        try:
            df = pd.read_sql(query, self.db_manager.engine)
            self.logger.info(f"Fetched customer demographics: {len(df)} rows")
            
            plt.figure(figsize=(12, 6))
            sns.barplot(data=df, x='customer_state', y='customer_count', palette='viridis')
            plt.title('Top 10 States by Customer Count')
            plt.xlabel('State')
            plt.ylabel('Number of Customers')
            
            output_path = os.path.join(self.output_dir, 'customer_demographics.png')
            plt.savefig(output_path)
            plt.close()
            self.logger.info(f"Saved customer demographics plot to {output_path}")
            
            return df
        except Exception as e:
            self.logger.error(f"Error analyzing customer demographics: {e}")
            return pd.DataFrame()

    def analyze_product_categories(self):
        """
        Analyze and visualize top product categories.
        """
        query = """
        SELECT 
            t.product_category_name_english as category,
            COUNT(p.product_id) as product_count
        FROM products p
        JOIN category_translations t ON p.product_category_name = t.product_category_name
        GROUP BY t.product_category_name_english
        ORDER BY product_count DESC
        LIMIT 10;
        """
        
        try:
            df = pd.read_sql(query, self.db_manager.engine)
            self.logger.info(f"Fetched product categories: {len(df)} rows")
            
            # Visualization
            plt.figure(figsize=(12, 8))
            sns.barplot(data=df, y='category', x='product_count', palette='magma')
            plt.title('Top 10 Product Categories')
            plt.xlabel('Number of Products')
            plt.ylabel('Category')
            
            output_path = os.path.join(self.output_dir, 'product_categories.png')
            plt.savefig(output_path)
            plt.close()
            self.logger.info(f"Saved product categories plot to {output_path}")
            
            return df
        except Exception as e:
            self.logger.error(f"Error analyzing product categories: {e}")
            return pd.DataFrame()

    def analyze_order_trends(self):
        """
        Analyze and visualize monthly order trends.
        """
        query = """
        SELECT 
            DATE_TRUNC('month', order_purchase_timestamp) as month,
            COUNT(order_id) as order_count
        FROM orders
        WHERE order_purchase_timestamp IS NOT NULL
        GROUP BY month
        ORDER BY month;
        """
        
        try:
            df = pd.read_sql(query, self.db_manager.engine)
            self.logger.info(f"Fetched order trends: {len(df)} rows")
            
            # Visualization
            plt.figure(figsize=(14, 6))
            sns.lineplot(data=df, x='month', y='order_count', marker='o', color='b')
            plt.title('Monthly Order Volume')
            plt.xlabel('Date')
            plt.ylabel('Number of Orders')
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            output_path = os.path.join(self.output_dir, 'order_trends.png')
            plt.savefig(output_path)
            plt.close()
            self.logger.info(f"Saved order trends plot to {output_path}")
            
            return df
        except Exception as e:
            self.logger.error(f"Error analyzing order trends: {e}")
            return pd.DataFrame()
