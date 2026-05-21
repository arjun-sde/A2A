FROM python:3.13-slim

WORKDIR /app

COPY services/analyst_agent/requirements.txt /app/services/analyst_agent/requirements.txt
RUN pip install --no-cache-dir -r /app/services/analyst_agent/requirements.txt

COPY services /app/services
COPY shared /app/shared
COPY dev_auth /app/dev_auth

ENV PYTHONPATH=/app

CMD ["uvicorn", "services.analyst_agent.app.main:app", "--host", "0.0.0.0", "--port", "8002"]
