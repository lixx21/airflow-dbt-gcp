from airflow.utils.dates import days_ago
from datetime import timedelta
from src.extract_data import extract
from src.upload_to_gcs import upload_data
from src.load_to_bq import start_load_data
from dotenv import load_dotenv
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os

load_dotenv(dotenv_path="./dags/.env" , verbose=True, override=True)

default_dag = {
    "owner": "Felix Pratamasan",
    "start_date": days_ago(1),
    "email": ["felixpratama242@gmail.com"],
    "email_on_failure": True,
    "email_on_entry": True,
    "retries":1,
    "retry_delay": timedelta(hours=1),
}

bucket_name = os.getenv('BUCKET_NAME')
credentials = os.getenv("CREDENTIAL_KEY")
gcp_conn_id = os.getenv("GCP_CONN_ID")
project_id = os.getenv("PROJECT_ID")
gcs_filename = "sg_shopping_data"
bucket_filename = f"{gcs_filename}.json"
destination_dataset_table = f"{project_id}.shopping_data.{gcs_filename}"

with DAG (dag_id="shopping_data_pipeline",
    schedule_interval= "0 1 * * *",
    default_args= default_dag,
    description="Create Data Pipeline with Airflow and DBT",
    max_active_runs=1
) as dag:


    def extract_data(**kwargs):
        data = extract(file="./dags/data/shopping_trends.csv")
        # print(data[0])        
        kwargs['ti'].xcom_push(key="extract_data", value=data)
        return "Success Extract Data"

    def upload_gcs(**kwargs):
        
        data = kwargs['ti'].xcom_pull(task_ids='extract_data', key="extract_data")

        upload_data(data, gcs_filename, bucket_name, credentials)
        
        return "Success Upload file to GCS"    


    extract_data_source = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        provide_context=True,
    )

    upload_gcs_file = PythonOperator(
        task_id="upload_gcs",
        python_callable=upload_gcs,
        provide_context=True,
    )

    load_data_to_staging_layer = GCSToBigQueryOperator(
        task_id="load_to_BQ", 
        bucket=bucket_name, 
        gcp_conn_id  = gcp_conn_id,
        source_objects=[bucket_filename], 
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        # schema_object=schema,
        destination_project_dataset_table=destination_dataset_table,
    )

    create_data_warehouse = BashOperator(
        task_id = "dbt_transform_data_warehouse",
        bash_command = "cd /opt/airflow/dags/dbt_transform/ && dbt run --select warehouse --full-refresh"
    )

    create_customer_data_mart = BashOperator(
        task_id = "dbt_transform_customer_data_mart",
        bash_command = "cd /opt/airflow/dags/dbt_transform/ && dbt run --select mart_customers --full-refresh"
    )

    create_sales_data_mart = BashOperator(
        task_id = "dbt_sales_data_mart",
        bash_command = "cd /opt/airflow/dags/dbt_transform/ && dbt run --select mart_item_sales --full-refresh"
    )

    extract_data_source >> upload_gcs_file >> load_data_to_staging_layer >> create_data_warehouse >> [create_customer_data_mart, create_sales_data_mart]



