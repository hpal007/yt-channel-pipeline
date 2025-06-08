import os
import json

from src.yt_utils import logger, youtube


def main(video_ids):
    logger.info(f"Extracting video details for video IDs: {video_ids}")
    videos_data = []
    batch_size = 25  # YouTube API allows a maximum of 50 video IDs per request

    for i in range(0, len(video_ids), batch_size):
        batch = video_ids[i : i + batch_size]
        logger.info(f"Processing batch {batch} of size {len(batch)}")
        try:
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=",".join(
                    batch
                ),  # Pass multiple video IDs as a comma-separated string
            )
            response = request.execute()
            items = response.get("items", [])

            for item in items:
                videos_data.append(
                    {
                        "videoId": item["id"],
                        "channelId": item["snippet"]["channelId"],
                        "categoryId": item["snippet"]["categoryId"],
                        "defaultAudioLanguage": item["snippet"].get(
                            "defaultAudioLanguage", ""
                        ),
                        "tags": item["snippet"].get("tags", []),
                        "duration": item["contentDetails"]["duration"],
                        "definition": item["contentDetails"]["definition"],
                        "caption": item["contentDetails"]["caption"],
                        "licensedContent": item["contentDetails"]["licensedContent"],
                        "viewCount": item["statistics"]["viewCount"],
                        "likeCount": item["statistics"]["likeCount"],
                        "commentCount": item["statistics"]["commentCount"],
                    }
                )
        except Exception as e:
            logger.error(f"An error occurred during batch processing: {e}")
            continue

    logger.info(f"Total videos fetched: {len(videos_data)}")
    channel_id = videos_data[0]["channelId"] if videos_data else "unknown"
    json_response = json.dumps(videos_data, indent=2)

    if not os.path.exists("/tmp/video"):
        # Create the directory if it does not exist
        logger.info("Creating directory /tmp/video")
        os.mkdir("/tmp/video")

    # Write the response to a file
    open(f"/tmp/video/{channel_id}.json", "w").write(str(json_response))
    logger.info(f"Response written to /tmp/video/{channel_id}.json")


if __name__ == "__main__":
    main()
