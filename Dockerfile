FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Europe/Zurich

COPY requirements.txt ./
RUN apt-get update \
    && apt-get install -y --no-install-recommends git openssh-client \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt

COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["docker-entrypoint.sh"]

COPY . .

EXPOSE 5000
CMD ["sh", "-c", "exec gunicorn --bind 0.0.0.0:5000 --timeout ${GUNICORN_TIMEOUT:-180} --graceful-timeout ${GUNICORN_GRACEFUL_TIMEOUT:-30} app:app"]
