from airflow.sdk import dag, task
from airflow.providers.microsoft.azure.transfers.local_to_wasb import (
    LocalFilesystemToWasbOperator,
)
from src.yt_utils import LOCAL_DATA_DIR, logger
import os
import shutil  # Import the shutil module for directory removal


@dag(
    # Define the DAG ID and description
    # This DAG is triggered by the youtube_data_pipeline DAG
    dag_id="save_to_cloud",
    description="Upload files to Azure Blob Storage",
)
def save_to_cloud():
    CONTAINER = "yt-channel-data"
    WASB_CONN_ID = "azure_blob_storage_default"

    # Task to retrieve data (channel_id) passed from the triggering DAG
    @task()
    def get_data_from_dag_task(**kwargs):
        # Access the data passed from DAG A
        dag_run = kwargs["dag_run"]
        if dag_run and dag_run.conf:
            received_data = dag_run.conf
            logger.info(f"Received data: {received_data}")
            channel_id = received_data.get("channelId")
            logger.info(f"Channel Id: {channel_id}")
            return channel_id
        else:
            logger.info("No data received from DAG.")
            raise ValueError("No data received from DAG, cannot continue.")

    # Task to list all files within the channel's data directory

    @task
    def list_files(channel_id):
        file_paths = []
        base_dir = os.path.join(LOCAL_DATA_DIR, channel_id)
        for root, _, files in os.walk(base_dir):
            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, base_dir)
                file_paths.append(
                    {
                        "file_path": full_path,
                        "blob_name": os.path.join(channel_id, rel_path),
                    }
                )
        for el in file_paths:
            logger.info(f"All the files to be uploaded are: {file_paths}")
        return file_paths

    # Uncomment it when testing to use this dag in direct run.
    # @task()
    # def get_data_from_dag_task():
    #     # Access the data passed from DAG A
    #     return "UCb4XiEOeJulWu4WGhwnDqjw"

    # Task to remove the local data directory after successful upload
    @task()
    def remove_files_from_local(channel_id):
        """
        Removes a folder and all its contents.

        Args:
            channel_id (str): The path to the folder to remove.
        """
        folder_path = os.path.join(LOCAL_DATA_DIR, channel_id)
        try:
            shutil.rmtree(folder_path)
            print(f"Successfully removed folder: {folder_path}")
        except OSError as e:
            print(f"Error removing folder {folder_path}: {e}")

    # Define the task dependencies and workflow
    channel_id = get_data_from_dag_task()
    files = list_files(channel_id)

    upload_file = LocalFilesystemToWasbOperator.partial(
        # Use .partial() and .expand_kwargs() for dynamic task creation based on the list of files
        task_id="upload_file",
        container_name=CONTAINER,
        wasb_conn_id=WASB_CONN_ID,
    ).expand_kwargs(files)
    upload_file >> remove_files_from_local(channel_id)


save_to_cloud()
