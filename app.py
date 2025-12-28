import streamlit as st
import plotly.express as px
import pandas as pd
from src.database.manager import DatabaseManager
from src.llm.agent import SQLAgent
from src.config.settings import settings
import os

st.set_page_config(page_title="E-commerce RAG Analytics", layout="wide")
st.title("E-commerce Growth Analytics (Agentic RAG)")
os.environ["OPENAI_API_KEY"] = settings.OPENAI_API_KEY

# Initialize DB and Agent (cache for performance)
@st.cache_resource
def get_agent():
    db_manager = DatabaseManager()
    db_manager.connect()
    return SQLAgent(db_manager)

agent = get_agent()

st.sidebar.header("Ask a Question")
user_question = st.sidebar.text_area("Enter your business question (English or Persian):", height=80)

if st.sidebar.button("Submit") and user_question.strip():
    with st.spinner("Processing..."):
        result = agent.run(user_question)
        # Generate business insight
        insight = agent.generate_insight(result.get('query_result', ''), user_question)
        if result.get("error"):
            st.error(f"Error: {result['error']}")
        else:
            st.subheader("Business Insight:")
            st.markdown(insight)

st.sidebar.markdown("---")
st.sidebar.header("Show Analytics Charts")

# Example: Show Top 10 States by Customer Count
if st.sidebar.button("Top 10 States by Customers"):
    db_manager = agent.db_manager
    query = """
    SELECT customer_state, COUNT(*) as customer_count
    FROM customers
    GROUP BY customer_state
    ORDER BY customer_count DESC
    LIMIT 10;
    """
    df = pd.read_sql(query, db_manager.engine)
    fig = px.bar(df, x='customer_state', y='customer_count', title='Top 10 States by Customer Count')
    st.plotly_chart(fig, use_container_width=True)

# Example: Show Top 10 Product Categories
if st.sidebar.button("Top 10 Product Categories"):
    db_manager = agent.db_manager
    query = """
    SELECT t.product_category_name_english as category, COUNT(p.product_id) as product_count
    FROM products p
    JOIN category_translations t ON p.product_category_name = t.product_category_name
    GROUP BY t.product_category_name_english
    ORDER BY product_count DESC
    LIMIT 10;
    """
    df = pd.read_sql(query, db_manager.engine)
    fig = px.bar(df, y='category', x='product_count', orientation='h', title='Top 10 Product Categories')
    st.plotly_chart(fig, use_container_width=True)

# Example: Show Monthly Order Trend
if st.sidebar.button("Monthly Order Trend"):
    db_manager = agent.db_manager
    query = """
    SELECT DATE_TRUNC('month', order_purchase_timestamp) as month, COUNT(order_id) as order_count
    FROM orders
    WHERE order_purchase_timestamp IS NOT NULL
    GROUP BY month
    ORDER BY month;
    """
    df = pd.read_sql(query, db_manager.engine)
    fig = px.line(df, x='month', y='order_count', markers=True, title='Monthly Order Volume')
    st.plotly_chart(fig, use_container_width=True)
