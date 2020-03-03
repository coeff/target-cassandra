FROM python:3.7-slim-buster
RUN mkdir /target-cassandra


RUN apt-get update
COPY . /target-cassandra
WORKDIR /target-cassandra

RUN apt -y install build-essential && python setup.py install && apt -y autoremove && apt-get -y clean
CMD ["target-cassandra"]
