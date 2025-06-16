import os
import json
from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from src.yt_search import main as search_channel
from src.yt_channels import main as extract_channel_data
from src.yt_playlistitems import main as extract_playlist_data
from src.yt_video import main as extract_video_details
from src.yt_comment import main as extract_comments

from src.yt_utils import logger, get_channel_name_config, LOCAL_DATA_DIR

DAG_ID = "youtube_data_pipeline"


@dag()
def youtube_data_pipeline():
    # This DAG orchestrates the extraction of YouTube channel data.

    @task()
    def extract_youtube_channel(channel_name=None):
        if channel_name == "None":
            try:
                channel_name = get_channel_name_config()
            except Exception:
                raise ValueError("Channel name not provided in the config.yml file.")

        channel_search_info = search_channel(channel_name)

        if not channel_search_info:
            raise ValueError(
                f"No channel information found for the specified {channel_name}."
            )
        return channel_search_info

    # Task to extract channel information using the channel ID.
    @task()
    def extract_channel_info(extract_youtube_channel: dict[str]):
        channel_data = extract_youtube_channel
        if channel_data:
            channel_id = channel_data.get("channelId")
            if not channel_id:
                raise ValueError("Channel ID not found in the extracted data.")

            return extract_channel_data(channel_id)

    # Task to extract playlist items from the channel's main playlist.
    @task()
    def extract_playlist_items(extract_channel_info: dict[str]):
        playlist_data = extract_channel_info
        if playlist_data:
            playlist_id = playlist_data.get("playlistId")

            return extract_playlist_data(playlist_id)

    # Task to extract and save video details for all videos in the playlist.
    @task()
    def extract_and_save_video_info(extract_playlist_items: list):
        video_ids = [video["videoId"] for video in extract_playlist_items]
        if not video_ids:
            raise ValueError("No playlist items found for the specified playlist.")

        return extract_video_details(video_ids)

    # Task to extract and save comments for all videos in the playlist.
    @task()
    def extract_and_save_comments_info(extract_playlist_items: list):
        video_ids = [video["videoId"] for video in extract_playlist_items]
        if not video_ids:
            raise ValueError("No video IDs found in the playlist items.")

        logger.info(f"Extracting comments for video IDs: {video_ids}")
        extract_comments(video_ids)

    # Task to save the initial channel information to a JSON file.
    @task()
    def save_channel_info(ch_info, ch_data):
        channel_id = ch_info.get("channelId")
        ch_data["channelTitle"] = ch_info.get("channelTitle")
        path = f"{LOCAL_DATA_DIR}/{channel_id}"

        if not os.path.exists(path):
            logger.info(f"Creating directory {path}")
            # Create the channel directory if it does not exist
            os.mkdir(path)

        json_response = json.dumps(ch_data, indent=2)
        open(f"{path}/channel_{channel_id}.json", "w").write(str(json_response))
        logger.info(f"Initial Channel Info written to {path}/{channel_id}.json")

    # Define the task dependencies
    # The workflow defines the sequence of data extraction and saving tasks.
    # Used jinja here to able to get channel_name from Aiflow UI
    channel_info = extract_youtube_channel(
        channel_name="{{ dag_run.conf.get('channel_name', None)}}"
    )

    trigger_save_to_cloud_dag = TriggerDagRunOperator(
        task_id="trigger_save_to_cloud",
        # Trigger the 'save_and_delete' DAG after data extraction is complete.
        trigger_dag_id="save_to_cloud",  # ID of the DAG to trigger
        reset_dag_run=True,
        conf=channel_info,
    )

    trigger_save_to_database_dag = TriggerDagRunOperator(
        task_id="trigger_save_to_database_dag",
        # Trigger the 'save_and_delete' DAG after data extraction is complete.
        trigger_dag_id="save_to_database",  # ID of the DAG to trigger
        reset_dag_run=True,
        conf=channel_info,
    )

    channel_data = extract_channel_info(channel_info)
    channel_data >> [save_channel_info(channel_info, channel_data)]

    playlist_items = extract_playlist_items(channel_data)

    [
        extract_and_save_video_info(playlist_items),
        extract_and_save_comments_info(playlist_items),
    ] >> trigger_save_to_database_dag  # trigger_save_to_cloud


youtube_data_pipeline()
