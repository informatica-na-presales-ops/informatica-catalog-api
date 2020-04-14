FROM python:3.8.2-alpine3.11

COPY requirements.txt /informatica-catalog-api/requirements.txt

RUN /sbin/apk add --no-cache --virtual .deps gcc libxml2-dev libxslt-dev musl-dev \
 && /sbin/apk add --no-cache libxslt \
 && /usr/local/bin/pip install --no-cache-dir --requirement /informatica-catalog-api/requirements.txt \
 && /sbin/apk del --no-cache .deps

ENV VERSION="2020.1" \
    PYTHONUNBUFFERED="1" \
    TZ="Etc/UTC"

LABEL org.opencontainers.image.authors="William Jackson <wjackson@informatica.com>" \
      org.opencontainers.image.version="${VERSION}"

COPY get-catalog-statistics.py /informatica-catalog-api/get-catalog-statistics.py
