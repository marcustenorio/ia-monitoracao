# Dockerfile do collector
FROM python:3.11-slim

WORKDIR /app

# Copia o código
COPY . /app

# Instala dependências
RUN pip install --no-cache-dir pyzabbix pandas requests

# Comando padrão
CMD ["python", "main.py"]
