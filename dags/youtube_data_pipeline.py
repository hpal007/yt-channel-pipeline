from airflow.sdk import Context, dag, task

from src.yt_search import main as search_channel #Changed import
from src.yt_channels import main as extract_channel_data #Changed import
from src.yt_playlistitems import main as extract_playlist_data #Changed import
from src.yt_video import main as extract_video_details #Changed import
from src.yt_comment import main as extract_comments #Changed import
# from dotenv import load_dotenv
from src.yt_utils import logger

DAG_ID = "youtube_data_pipeline"
channel = "MANISH KUMAR"  # Replace with the desired channel name
# This DAG extracts YouTube channel data, playlist items, video details, and comments

@dag()
def youtube_data_pipeline():

    @task
    def extract_youtube_channel():
        logger.info(f"Searching for channel: {channel}")
        channel_search_info = search_channel(channel)
        if not channel_search_info:
            raise ValueError("No channel information found for the specified channel name.")
        return channel_search_info
    
    @task
    def extract_channel_info(extract_youtube_channel: dict[str]):
        channel_data = extract_youtube_channel
        if channel_data:
            channel_id = channel_data.get("channelId")
            if not channel_id:
                raise ValueError("Channel ID not found in the extracted data.")
            
            return extract_channel_data(channel_id)

    @task
    def extract_playlist_items(extract_channel_info: dict[str]):
        playlist_data = extract_channel_info
        if playlist_data:
            playlist_id = playlist_data.get("playlistId")

            return extract_playlist_data(playlist_id)

    @task
    def extract_video_info(extract_playlist_items: list):
        video_ids = [video["videoId"] for video in extract_playlist_items]
        if not video_ids:
            raise ValueError("No playlist items found for the specified playlist.")
        
        return extract_video_details(video_ids)

    @task
    def extract_comments_info(extract_playlist_items: list):
        video_ids = [video["videoId"] for video in extract_playlist_items]
        if not video_ids:
            raise ValueError("No video IDs found in the playlist items.")
        logger.info(f"Extracting comments for video IDs: {video_ids}")
        extract_comments(video_ids)
    
    # Define the task dependencies
    channel_info = extract_youtube_channel()
    channel_data = extract_channel_info(channel_info)
    playlist_items = extract_playlist_items(channel_data)
    extract_video_info(playlist_items)
    extract_comments_info(playlist_items)

youtube_data_pipeline()
# This will create an instance of the DAG

