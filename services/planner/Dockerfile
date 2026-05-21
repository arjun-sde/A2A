FROM python:3.13-slim

WORKDIR /app

COPY services/planner/requirements.txt /app/services/planner/requirements.txt
RUN pip install --no-cache-dir -r /app/services/planner/requirements.txt

COPY services /app/services
COPY shared /app/shared
COPY dev_auth /app/dev_auth

ENV PYTHONPATH=/app

CMD ["uvicorn", "services.planner.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
