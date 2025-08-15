FROM python:3.11-slim

# Definir diretório de trabalho
WORKDIR /app

# Copiar arquivos de módulos comuns
COPY common /app/common
COPY infrastructure /app/infrastructure

# Copiar arquivos específicos do analyzer
COPY agents/analyzer/requirements.txt /app/
COPY agents/analyzer/main.py /app/

# Instalar dependências
RUN pip install --no-cache-dir -r requirements.txt

# Comando padrão
CMD ["python", "main.py"]
