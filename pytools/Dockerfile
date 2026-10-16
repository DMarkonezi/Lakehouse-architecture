# "Treca masina": Python bez Spark-a. Isti image koriste explorer (Jupyter) i console (Streamlit).
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends graphviz curl \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
ENV PYTHONPATH=/app/explorer:/app/generator
