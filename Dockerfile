FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Railway 會用 /data 作持久化目錄
ENV SNAPSHOT_DIR=/data/snapshots

CMD ["python", "bot.py"]
