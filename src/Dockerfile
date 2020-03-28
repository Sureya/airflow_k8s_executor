FROM python:3.7-slim-buster
LABEL maintainer="SureyaSathiamoorthi"

ARG S3_FILE_NAME
ARG TASK_NAME
ENV AWS_CONFIG_FILE /app/credentials/config
ENV AWS_SHARED_CREDENTIALS_FILE /app/credentials/credentials

RUN mkdir /app

COPY requirements.txt /app
COPY country_processing.py /app

WORKDIR /app

RUN pip3 install -r requirements.txt

CMD python3 country_processing.py --s3_file=${S3_FILE_NAME} --task_name=${TASK_NAME}

#docker run -v ~/.aws:/app/credentials -e S3_FILE_NAME="sks-airflow-logs/airflow-poc/run_001/input/countries.csv" -e TASK_NAME="apply_lower"  country