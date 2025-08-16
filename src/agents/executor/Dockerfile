FROM python:3.11-slim

WORKDIR /app

COPY agents/executor/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY infrastructure /app/infrastructure
COPY agents/executor/main.py /app/main.py

CMD ["python", "main.py"]

