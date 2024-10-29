# syntax=docker/dockerfile:1

FROM python:3.9-slim-buster

WORKDIR /python-docker

COPY requirements.txt requirements.txt

RUN apt-get update && \
    apt-get install --yes --no-install-recommends \
    openssh-client \ 
    git \
    && apt-get clean 

RUN pip3 install -r requirements.txt

RUN adduser --system --no-create-home nonroot

USER nonroot

COPY --chown=nonroot . .

CMD fastapi run app.py --host 0.0.0.0 --port 5000
