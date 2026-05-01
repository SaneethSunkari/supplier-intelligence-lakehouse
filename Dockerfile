FROM python:3.11-slim-bookworm

WORKDIR /opt/sourceiq

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/opt/sourceiq
ENV LAKEHOUSE_FORMAT=parquet

CMD ["python", "-m", "src.pipeline"]
