from src.database.manager import DatabaseManager
from src.llm.agent import SQLAgent
from src.config.settings import settings
import os

def main():

    if not os.getenv("OPENAI_API_KEY"):
        os.environ["OPENAI_API_KEY"] = settings.OPENAI_API_KEY

    db_manager = DatabaseManager()
    if not db_manager.connect():
        print("Failed to connect to database.")
        return

    agent = SQLAgent(db_manager)

    # Test Questions
    questions = [
        "How many customers are there?",
        "Show me the top 5 product categories by number of products.",
        "What is the total revenue (sum of payments)?",
        "لیست ۵ ایالت که بیشترین مشتری را دارند بده" # Persian test
    ]

    for q in questions:
        print(f"\n\nQuery: {q}")
        print("-" * 30)
        result = agent.run(q)
        
        if result.get("error"):
            print(f"Final Error: {result['error']}")
        else:
            print("SQL Generated:")
            print(result['sql_query'])
            print("\nResult:")
            print(result['query_result'])

if __name__ == "__main__":
    main()
