from prometheus_client import Counter, Histogram, start_http_server
import time

# Counter for LLM token usage
llm_tokens_total = Counter(
    'llm_tokens_total',
    'Total number of LLM tokens used',
    ['model', 'operation']
)

# Histogram for Agent response time
agent_response_time_seconds = Histogram(
    'agent_response_time_seconds',
    'Agent response time in seconds',
    ['operation']
)

def start_metrics_server(port: int = 8000):
    start_http_server(port)
    print(f"Prometheus metrics server started on port {port}")

# Example usage:
# llm_tokens_total.labels(model="gpt-4o", operation="completion").inc(token_count)
# with agent_response_time_seconds.labels(operation="agent_run").time():
#     ... run agent ...
