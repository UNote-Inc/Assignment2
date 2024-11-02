FROM python:3.11-alpine

ENV FLASK_APP=/app/KV.py
ENV FLASK_RUN_HOST=127.0.0.1
ENV FLASK_RUN_PORT=5000

RUN apk add --no-cache curl && pip install flask

COPY . /app

WORKDIR /app

EXPOSE 5000

CMD ["python", "KV.py"]