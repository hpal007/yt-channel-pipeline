from airflow.sdk import dag, task
from airflow.providers.microsoft.azure.transfers.local_to_wasb import (
    LocalFilesystemToWasbOperator,
)
from src.yt_utils import drop_location


@dag(
    dag_id="upload_to_azure_storage_dag",
    description="Upload files to Azure Blob Storage",
)
def upload_to_azure_storage_dag():

    @task()
    def print_received_data(**kwargs):
        # Access the data passed from DAG A
        dag_run = kwargs["dag_run"]
        if dag_run and dag_run.conf:
            received_data = dag_run.conf
            print(f"Received data: {received_data}")
            message = received_data.get("message")
            value = received_data.get("value")
            print(f"Message: {message}, Value: {value}")
        else:
            print("No data received from DAG A.")

    upload_file = LocalFilesystemToWasbOperator(
        task_id="upload_file",
        file_path=f"{drop_location}/abc.txt",  # Local file path to upload
        container_name="yt-channel-data",  # Azure container name
        blob_name="uploaded/file.txt",  # Name for the blob in Azure
        wasb_conn_id="azure_blob_storage_default",  # Your Airflow connection ID
    )

    print_received_data() >> upload_file


upload_to_azure_storage_dag()
