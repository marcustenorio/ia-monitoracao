FROM python:3.11-slim

WORKDIR /app

COPY agents/analyzer_timeseries/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# copiamos apenas o main (não precisa infra extra aqui)
COPY agents/analyzer_timeseries/main.py /app/main.py

CMD ["python", "main.py"]

