from airflow.sdk import dag, task
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator

# Default arguments
default_args = {'owner': 'data-engineer'}

# Define the DAG
@dag(
    dag_id='xc',
    default_args=default_args,
    description='Upload files to Azure Blob Storage'
)
def upload_to_azure_storage_dag():

    # Create a sample file
    @task
    def create_sample_file():
        with open('/tmp/sample_data.txt', 'w') as f:
            f.write('Hello from Airflow to Azure!')
        return '/tmp/sample_data.txt'

    # Upload to Azure Storage
    upload_file_task = LocalFilesystemToWasbOperator(
            task_id='upload_to_azure',
            file_path='/tmp/sample_data.txt',
            container_name='yt-channel-data',
            blob_name='uploaded_files/sample_data.txt',
            wasb_conn_id='azure_blob_storage_default',
        )

    # Set task dependencies
    create_sample_file() >> upload_file_task

upload_to_azure_storage_dag()
