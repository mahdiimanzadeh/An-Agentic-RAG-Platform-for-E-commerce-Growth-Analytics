from typing import TypedDict, Annotated, List, Union
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, END
from sqlalchemy import text
import pandas as pd
from src.monitoring.metrics import llm_tokens_total, agent_response_time_seconds
from prometheus_client import start_http_server
import os
import time

from src.database.manager import DatabaseManager
from src.llm.prompts import get_system_prompt
from src.llm.schema_generator import generate_schema_description
from src.config.settings import settings

# Define the State
class AgentState(TypedDict):
    question: str
    sql_query: str
    query_result: str
    error: str
    attempts: int

class SQLAgent:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.llm = ChatOpenAI(model="gpt-4o", temperature=0)
        
        if os.environ.get("PROMETHEUS_METRICS", "1") == "1":
            start_http_server(8000)
 
        
        # Generate Schema Context once during initialization
        self.schema_context = generate_schema_description(self.db_manager.engine)
        self.system_prompt = get_system_prompt(self.schema_context)
        
        # Build the Graph
        self.workflow = self._build_graph()

    def _build_graph(self):
        workflow = StateGraph(AgentState)

        # Add Nodes
        workflow.add_node("analyze_question", self.analyze_question)
        workflow.add_node("generate_sql", self.generate_sql)
        workflow.add_node("execute_query", self.execute_query)
        workflow.add_node("validate_result", self.validate_result)

        # Add Edges
        workflow.set_entry_point("analyze_question")
        workflow.add_edge("analyze_question", "generate_sql")
        workflow.add_edge("generate_sql", "execute_query")
        workflow.add_edge("execute_query", "validate_result")
        
        # Conditional Edge for Validation
        workflow.add_conditional_edges(
            "validate_result",
            self.check_validity,
            {
                "valid": END,
                "invalid": "generate_sql" 
            }
        )

        return workflow.compile()

    # Node Functions

    def analyze_question(self, state: AgentState):
        """
        Node 1: Analyze the user's question to ensure it's relevant to the database.
        (Currently a pass-through, but can be expanded to filter non-SQL questions)
        """
        # In a real scenario, we might check if the question is about e-commerce.
        return {"attempts": 0}

    def generate_sql(self, state: AgentState):
        """
        Node 2: Generate SQL query based on the question and schema.
        """
        question = state['question']
        error = state.get('error')
        
        messages = [
            SystemMessage(content=self.system_prompt),
            HumanMessage(content=question)
        ]
        
        if error:
            messages.append(HumanMessage(content=f"The previous query resulted in an error: {error}. Please fix the SQL."))

        start_time = time.time()
        response = self.llm.invoke(messages)
        duration = time.time() - start_time
        
        # Try to log token usage if available
        token_count = getattr(response, 'usage', {}).get('total_tokens', 0)
        llm_tokens_total.labels(model="gpt-4o", operation="completion").inc(token_count)
        agent_response_time_seconds.labels(operation="generate_sql").observe(duration)
        
        sql_query = response.content.strip().replace("```sql", "").replace("```", "")
        
        return {"sql_query": sql_query, "attempts": state.get("attempts", 0) + 1}

    def execute_query(self, state: AgentState):
        """
        Node 3: Execute the generated SQL query against the database.
        """
        sql_query = state['sql_query']
        
        try:
            df = pd.read_sql(sql_query, self.db_manager.engine)
            
            if df.empty:
                return {"query_result": "No data found.", "error": None}
            
            return {"query_result": df.to_markdown(index=False), "error": None}
            
        except Exception as e:
            return {"query_result": None, "error": str(e)}

    def validate_result(self, state: AgentState):
        """
        Node 4: Check if the execution was successful.
        """
        return {}

    def check_validity(self, state: AgentState):
        """
        Conditional logic to determine next step.
        """
        if state.get("error"):
            if state["attempts"] >= 3:
                return "valid" 
            return "invalid"
        
        return "valid"

    def run(self, question: str):
        """
        Entry point to run the agent.
        """
        with agent_response_time_seconds.labels(operation="agent_run").time():
            initial_state = {"question": question, "attempts": 0, "error": None}
            result = self.workflow.invoke(initial_state)
        return result

    def generate_insight(self, sql_result: str, question: str) -> str:
        """
        Generate a business insight based on the SQL result and user question using LLM.
        """
        system_prompt = (
            "You are a business analyst. Based on the following SQL result and user question, "
            "write a short, clear business insight in Persian or English. "
            "Focus on trends, anomalies, or actionable findings."
        )
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"User Question: {question}\nSQL Result:\n{sql_result}")
        ]
        response = self.llm.invoke(messages)
        return response.content.strip()
