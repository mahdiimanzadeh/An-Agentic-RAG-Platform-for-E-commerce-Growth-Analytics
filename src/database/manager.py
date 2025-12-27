import logging
from contextlib import contextmanager
from typing import Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from src.config.settings import settings
from src.models.models import Base

class DatabaseManager:
    """
    Comprehensive class for managing PostgreSQL database
    """
    
    def __init__(self):
        """
        Initialize database using settings
        """
        self.database_url = settings.DATABASE_URL
        self.engine = None
        self.SessionLocal = None
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging to monitor database operations"""
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> bool:
        """
        Connect to database and create engine
        
        Returns:
            bool: Connection success status
        """
        try:
            # Specific settings for PostgreSQL
            engine_kwargs = {
                "echo": False,
                "pool_size": 10,
                "max_overflow": 20
            }
            
            self.engine = create_engine(self.database_url, **engine_kwargs)
            
            # Test connection
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            self.logger.info("Successfully connected to PostgreSQL database")
            return True
            
        except Exception as e:
            self.logger.error(f"Error connecting to database: {str(e)}")
            self.logger.info("Is the Database Docker Container running?")
            self.logger.info("   Run command: cd docker && docker-compose up -d")
            return False
    
    def create_tables(self, drop_existing: bool = False) -> bool:
        """
        Create tables in database
        
        Args:
            drop_existing: Drop existing tables before recreating
            
        Returns:
            bool: Table creation success status
        """
        if self.engine is None:
            self.logger.error("Connect to the database first")
            return False
        
        if drop_existing:
            self.logger.info("Dropping existing tables...")
            Base.metadata.drop_all(bind=self.engine)
        
        self.logger.info("Creating tables...")
        Base.metadata.create_all(bind=self.engine)
        
        self.logger.info("Tables created successfully")
        return True
    
    @contextmanager
    def get_db_session(self):
        """
        Context manager for database session management
        """
        if self.SessionLocal is None:
            raise Exception("Database is not connected")
        
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(f"Error in database session: {str(e)}")
            raise
        finally:
            session.close()
    
    def test_connection(self) -> Dict[str, Any]:
        """
        Full connection and database status test
        
        Returns:
            Dict: Database status information
        """
        status = {
            "connected": False,
            "tables_exist": False,
            "table_count": 0,
            "sample_data": False
        }
        
        try:
            if self.engine is None:
                return status
            
            with self.engine.connect() as connection:
                status["connected"] = True
                
                # Check for tables existence
                from sqlalchemy import inspect
                inspector = inspect(self.engine)
                tables = inspector.get_table_names()
                status["table_count"] = len(tables)
                status["tables_exist"] = len(tables) > 0
                
                # Check for sample data
                if "customers" in tables:
                    result = connection.execute(text("SELECT COUNT(*) FROM customers")).fetchone()
                    status["sample_data"] = result[0] > 0 if result else False
                
        except Exception as e:
            self.logger.error(f"Error in connection test: {str(e)}")
        
        return status
    
    def get_table_info(self) -> Dict[str, int]:
        """
        Information on record counts for each table
        
        Returns:
            Dict: Table name and record count
        """
        table_info = {}
        
        try:
            if self.engine is None:
                return table_info
            
            with self.get_db_session() as session:
                for table_name in Base.metadata.tables.keys():
                    try:
                        count = session.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()[0]
                        table_info[table_name] = count
                    except Exception as e:
                        table_info[table_name] = f"Error: {str(e)}"
        
        except Exception as e:
            self.logger.error(f"Error retrieving table info: {str(e)}")
        
        return table_info
    
    def close_connection(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            self.logger.info("Database connection closed")
