from airflow.sdk import dag, task

import sqlite3
import os
import json

from src.db_utils import (
    create_connection,
    create_tables,
    process_json_files,
    LOCAL_DATA_DIR,
    logger,
)

DATABASE_NAME = f"{LOCAL_DATA_DIR}/youtube_data.db"


@dag(dag_id="save_to_database")
def save_to_database():

    @task()
    def get_channel_id(**kwargs):
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

    @task()
    def create_database_and_tables():
        """Airflow task to create the SQLite database and tables."""
        try:
            db_exists = os.path.exists(DATABASE_NAME)
            conn = create_connection(DATABASE_NAME)

            if not db_exists:
                print(f"Database '{DATABASE_NAME}' does not exist. Creating...")
                if conn:
                    create_tables(conn)
                    conn.close()
                else:
                    raise Exception("Failed to create database connection.")

        except Exception as e:
            print(f"Error creating database or tables: {e}")
            raise

    @task()
    def process_channel_files(channel_id):
        """Airflow task to load YouTube data from JSON files to SQLite."""
        conn = create_connection(DATABASE_NAME)
        if conn:
            process_json_files(
                conn, f"{LOCAL_DATA_DIR}/{channel_id}", "channel"
            )  # Updated directory
            conn.close()
        else:
            raise Exception("Failed to create database connection.")

    @task()
    def process_video_files(channel_id):
        """Airflow task to load YouTube data from JSON files to SQLite."""
        conn = create_connection(DATABASE_NAME)
        if conn:
            process_json_files(
                conn, f"{LOCAL_DATA_DIR}/{channel_id}", "video"
            )  # Updated directory
            conn.close()
        else:
            raise Exception("Failed to create database connection.")

    @task()
    def process_comments_files(channel_id):
        """Airflow task to load YouTube data from JSON files to SQLite."""
        conn = create_connection(DATABASE_NAME)
        if conn:
            process_json_files(
                conn, f"{LOCAL_DATA_DIR}/{channel_id}", "comments"
            )  # Updated directory
            conn.close()
        else:
            raise Exception("Failed to create database connection.")

    # Dependencies
    channel_id = get_channel_id()
    create_db_and_table_task = create_database_and_tables()

    channel_task = process_channel_files(channel_id)
    video_task = process_video_files(channel_id)
    comment_task = process_comments_files(channel_id)

    channel_id.set_upstream(create_db_and_table_task)
    channel_task.set_upstream(channel_id)
    video_task.set_upstream(channel_task)
    comment_task.set_upstream(video_task)


save_to_database()
