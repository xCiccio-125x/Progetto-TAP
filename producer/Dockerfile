FROM python:3.9-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir requests

COPY partita_raw.json .
COPY nba_producer_B.py .

CMD ["python", "nba_producer_B.py"]