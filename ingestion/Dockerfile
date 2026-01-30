FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# O comando COPY . . joga TUDO da sua pasta para dentro do container
# Isso inclui os scripts .py E os arquivos .json mockados
COPY . .
