# Use the official Airflow image
FROM apache/airflow:2.9.3

USER root

# Install git and other necessary tools
RUN apt-get update && apt-get install -y git && apt-get clean
#Create directory for dbt config
RUN mkdir -p /root/.dbt
# Copy dbt profile
COPY ./dags/dbt_transform/profiles.yml /root/.dbt/profiles.yml

USER airflow
# Install specific versions of dbt-bigquery and dbt-core
RUN pip install --no-cache-dir "dbt-bigquery==1.8.3" "dbt-core" "protobuf<4.22"


ENV AIRFLOW_HOME=/opt/airflow
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
#give permission to access logs
RUN chown -R airflow: /opt/airflow/logs 

