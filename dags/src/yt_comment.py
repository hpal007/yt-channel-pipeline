import os
import json

from src.yt_utils import logger, process_comments, youtube


def main(video_ids):
    for video_id in video_ids:
        logger.info(f"Fetching comments for video ID: {video_id}")
        get_comments_per_video(video_id)


def get_comments_per_video(video_id):
    comments_list = []
    next_page_token = None

    while True:
        request = youtube.commentThreads().list(
            part="snippet", videoId=video_id, maxResults=100, pageToken=next_page_token
        )
        response = request.execute()

        if not response.get("items"):
            print("No comments found for this video.")
            break

        comments_list.extend(process_comments(response["items"]))

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            print("No more pages of comments.")
            break

    logger.info(f"Total comments fetched: {len(comments_list)}")

    if not os.path.exists("/tmp/comments"):
        # Create the directory if it does not exist
        logger.info("Creating directory /tmp/comments")
        os.mkdir("/tmp/comments")

    json_response = json.dumps(comments_list, indent=2)
    open(f"/tmp/comments/{video_id}.json", "w").write(str(json_response))
    print(f"Response written to /tmp/comments/{video_id}.json")


if __name__ == "__main__":
    main(["N0p05Tq7oq0"])
