FROM python:3.11-slim

WORKDIR /app

# Copiamos só o requirements primeiro para cachear as deps
COPY agents/ml_trainer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos o restante da infra (para importar o serviço)
COPY infrastructure /app/infrastructure

# Copiamos o main do agente
COPY agents/ml_trainer/main.py /app/main.py

CMD ["python", "main.py"]

