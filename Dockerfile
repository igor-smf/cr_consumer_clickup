FROM python:3.12-slim

# Otimizações básicas
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Inicia servidor ouvindo em 0.0.0.0:$PORT (Cloud Run define PORT)
# Uvicorn cai para 8080 quando PORT não existe (local)
CMD ["bash", "-lc", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}"]
