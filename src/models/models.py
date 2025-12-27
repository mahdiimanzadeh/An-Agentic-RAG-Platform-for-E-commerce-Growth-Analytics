from sqlalchemy import Column, Integer, String, Float, DateTime, Text, ForeignKey
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

# Customers Table
class Customer(Base):
    __tablename__ = 'customers'
    
    customer_id = Column(String, primary_key=True, index=True)
    customer_unique_id = Column(String, index=True) # Removed unique=True to allow multiple orders per customer
    customer_zip_code_prefix = Column(String)
    customer_city = Column(String)
    customer_state = Column(String)
    
    # Relationships
    orders = relationship("Order", back_populates="customer")

# Orders Table
class Order(Base):
    __tablename__ = 'orders'
    
    order_id = Column(String, primary_key=True, index=True)
    customer_id = Column(String, ForeignKey('customers.customer_id'), index=True)
    order_status = Column(String)
    order_purchase_timestamp = Column(DateTime)
    order_approved_at = Column(DateTime)
    order_delivered_carrier_date = Column(DateTime)
    order_delivered_customer_date = Column(DateTime)
    order_estimated_delivery_date = Column(DateTime)
    
    # Relationships
    customer = relationship("Customer", back_populates="orders")
    order_items = relationship("OrderItem", back_populates="order")
    payments = relationship("Payment", back_populates="order")
    reviews = relationship("Review", back_populates="order")

# Products Table
class Product(Base):
    __tablename__ = 'products'
    
    product_id = Column(String, primary_key=True, index=True)
    product_category_name = Column(String, index=True)
    product_name_length = Column(Integer)
    product_description_length = Column(Integer)
    product_photos_qty = Column(Integer)
    product_weight_g = Column(Float)
    product_length_cm = Column(Float)
    product_height_cm = Column(Float)
    product_width_cm = Column(Float)
    
    # Relationships
    order_items = relationship("OrderItem", back_populates="product")
    sellers = relationship("SellerProduct", back_populates="product")

# Order Items Table
class OrderItem(Base):
    __tablename__ = 'order_items'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String, ForeignKey('orders.order_id'), index=True)
    order_item_id = Column(Integer)
    product_id = Column(String, ForeignKey('products.product_id'), index=True)
    seller_id = Column(String, ForeignKey('sellers.seller_id'), index=True)
    shipping_limit_date = Column(DateTime)
    price = Column(Float)
    freight_value = Column(Float)
    
    # Relationships
    order = relationship("Order", back_populates="order_items")
    product = relationship("Product", back_populates="order_items")
    seller = relationship("Seller", back_populates="order_items")

# Sellers Table
class Seller(Base):
    __tablename__ = 'sellers'
    
    seller_id = Column(String, primary_key=True, index=True)
    seller_zip_code_prefix = Column(String)
    seller_city = Column(String)
    seller_state = Column(String)
    
    # Relationships
    order_items = relationship("OrderItem", back_populates="seller")
    products = relationship("SellerProduct", back_populates="seller")

# Many-to-Many Table for Seller-Product
class SellerProduct(Base):
    __tablename__ = 'seller_products'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    seller_id = Column(String, ForeignKey('sellers.seller_id'))
    product_id = Column(String, ForeignKey('products.product_id'))
    
    # Relationships
    seller = relationship("Seller", back_populates="products")
    product = relationship("Product", back_populates="sellers")

# Payments Table
class Payment(Base):
    __tablename__ = 'payments'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String, ForeignKey('orders.order_id'), index=True)
    payment_sequential = Column(Integer)
    payment_type = Column(String)
    payment_installments = Column(Integer)
    payment_value = Column(Float)
    
    # Relationships
    order = relationship("Order", back_populates="payments")

# Reviews Table
class Review(Base):
    __tablename__ = 'reviews'
    
    id = Column(Integer, primary_key=True, autoincrement=True) # Added surrogate key
    review_id = Column(String, index=True) # Removed primary_key=True
    order_id = Column(String, ForeignKey('orders.order_id'), index=True)
    review_score = Column(Integer)
    review_comment_title = Column(Text)
    review_comment_message = Column(Text)
    review_creation_date = Column(DateTime)
    review_answer_timestamp = Column(DateTime)
    
    # Relationships
    order = relationship("Order", back_populates="reviews")

# Geolocation Table
class Geolocation(Base):
    __tablename__ = 'geolocation'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    geolocation_zip_code_prefix = Column(String, index=True)
    geolocation_lat = Column(Float)
    geolocation_lng = Column(Float)
    geolocation_city = Column(String)
    geolocation_state = Column(String)

# Category Translation Table
class CategoryTranslation(Base):
    __tablename__ = 'category_translations'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    product_category_name = Column(String, unique=True, index=True)
    product_category_name_english = Column(String)
