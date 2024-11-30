from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

def start_load_data(bucket_name, filename, gcp_conn_id, destination_dataset_table, write_disposition, **kwargs):
    
    try:
        print("Load data from GCS to BigQuery")
        gcs_to_gbq = GCSToBigQueryOperator(
                task_id="load_to_BQ", 
                bucket=bucket_name, 
                gcp_conn_id  = gcp_conn_id,
                source_objects=[filename], 
                source_format="NEWLINE_DELIMITED_JSON",
                create_disposition="CREATE_IF_NEEDED",
                write_disposition=write_disposition,
                # schema_object=schema,
                destination_project_dataset_table=destination_dataset_table,
            )
        print("Success Loaad Data")
    except Exception as e:
        print(e)
        print("Cannot load data")