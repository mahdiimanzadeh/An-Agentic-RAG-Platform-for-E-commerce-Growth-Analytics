# An-Agentic-RAG-Platform-for-E-commerce-Growth-Analytics

A modular Python platform for E-commerce analytics using RAG (Retrieval-Augmented Generation) and LLMs. The system loads raw CSV data, builds a PostgreSQL database, analyzes business metrics, and answers user questions with business insights via an agentic workflow.

## Pipeline Overview
- `main.py`: Load data into the database
- `run_analysis.py`: Analyze data and extract text for RAG
- `test_prompt_generation.py`: Generate System Prompt from schema
- `test_agent.py`: Run the Agent and answer user questions

## Streamlit UI
- Ask business questions (English or Persian)
- See business insights (not just numbers)
- View interactive analytics charts (Plotly)

**To run the UI:**
```bash
pip install streamlit plotly
streamlit run app.py
```

## Monitoring
- Prometheus integration for LLM token usage and agent response time
- Metrics available at http://localhost:8000/metrics

## Docker
- Full stack containerized (Database, API, UI, Monitoring)
- Use `docker compose -f docker/docker-compose.yml up --build` from project root