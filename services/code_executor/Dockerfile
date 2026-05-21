FROM python:3.13-slim

WORKDIR /app

COPY services/code_executor/requirements.txt /app/services/code_executor/requirements.txt
RUN pip install --no-cache-dir -r /app/services/code_executor/requirements.txt

COPY services /app/services
COPY shared /app/shared
COPY dev_auth /app/dev_auth

ENV PYTHONPATH=/app

CMD ["uvicorn", "services.code_executor.app.main:app", "--host", "0.0.0.0", "--port", "8001"]
