FROM python:3.11-slim

WORKDIR /app

# Dependências de compilação mínimas (precisa para sklearn, numpy, pandas)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements específico do agente
COPY agents/ml_trainer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia a infraestrutura (serviços utilitários)
COPY infrastructure /app/infrastructure

# Copia o main do agente
COPY agents/ml_trainer/main.py /app/main.py

# Padrão: roda o treinamento
CMD ["python", "main.py"]


