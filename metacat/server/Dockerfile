# syntax=docker/dockerfile:1

FROM python:3.9-slim-buster

#ENV SCICAT_HOST  host.docker.internal

#ENV MONGODB_HOST  host.docker.internal

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

#RUN echo $(pwd) && echo $(ls -laF)

CMD [ "fastapi", "run" , "app.py", "--host", "0.0.0.0", "--port", "5000"]
#CMD [ "python3", "-m" , "flask", "--app", "app", "run", "--host=0.0.0.0"]
#CMD [ "uwsgi", "--http" , "0.0.0.0:5000", "--module", "app:app"]
#CMD [ "uwsgi", "uswgi.ini"]
