import logging
import os
import googleapiclient.discovery
import yaml

# Logger configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# YouTube API key should be set in the environment variable YOUTUBE_API_KEY
# API Configuration

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
api_service_name = "youtube"
api_version = "v3"
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
if not YOUTUBE_API_KEY:
    raise ValueError(
        "YOUTUBE_API_KEY environment variable is not set. Please set it in your .env file or environment variables."
    )

youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=YOUTUBE_API_KEY
)

drop_location = "/opt/airflow/local_data"


def get_channel_name_config():
    logger.info(f"Checking channel name in config file.")
    config_path = os.path.join(os.path.dirname(__file__), "channel.yml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    return config["channel_name"]  # Get the channel name from the config file


# Function to get channel information
#  Process YouTube comments
def process_comments(response_items):
    comments = []
    for comment in response_items:
        author = comment["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
        comment_text = comment["snippet"]["topLevelComment"]["snippet"]["textOriginal"]
        publish_time = comment["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
        likes_count = comment["snippet"]["topLevelComment"]["snippet"]["likeCount"]
        replies_count = comment["snippet"].get("totalReplyCount", 0)

        comment_info = {
            "author": author,
            "comment": comment_text,
            "published_at": publish_time,
            "likes_count": likes_count,
            "replies_count": replies_count,
        }

        comments.append(comment_info)
    logger.info(f"Finished processing {len(comments)} comments.")
    return comments


# Process YouTube videos
def process_videos(response_items):
    videos = []
    for video in response_items:
        video_title = video["snippet"]["title"]
        video_description = video["snippet"]["description"]
        video_id = video["contentDetails"]["videoId"]
        video_published_at = video["contentDetails"]["videoPublishedAt"]

        video_info = {
            "title": video_title,
            "description": video_description,
            "videoId": video_id,
            "videoPublishedAt": video_published_at,
        }

        videos.append(video_info)
    logger.info(f"Finished processing {len(videos)} videos.")
    return videos
