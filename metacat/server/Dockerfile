# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

#ENV SCICAT_HOST  host.docker.internal

#ENV MONGODB_HOST  host.docker.internal

WORKDIR /python-docker

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

CMD [ "python3", "-m" , "flask", "--app", "main", "run", "--host=0.0.0.0"]
