# src/agents/collector/Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Copia as dependências
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copia o código-fonte do agente
COPY . .

# Comando de execução
CMD ["python", "main.py"]