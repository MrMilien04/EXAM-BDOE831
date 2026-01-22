FROM spark:latest

USER root

RUN pip install pandas

USER spark

WORKDIR /opt/spark/work-dir