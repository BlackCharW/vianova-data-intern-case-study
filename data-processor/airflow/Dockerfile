# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq wget

RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary 

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID