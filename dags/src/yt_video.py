import os
import json

from src.yt_utils import logger, youtube, LOCAL_DATA_DIR


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
                        "title": item["snippet"]["title"],
                        "channelId": item["snippet"]["channelId"],
                        "categoryId": item["snippet"]["categoryId"],
                        "defaultAudioLanguage": item["snippet"].get(
                            "defaultAudioLanguage", ""
                        ),
                        "videoPublishedAt": item["snippet"]["publishedAt"],
                        "description": item["snippet"]["description"],
                        "defaultLanguage": item["snippet"].get("defaultLanguage", ""),
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

    if not os.path.exists(f"{LOCAL_DATA_DIR}/{channel_id}"):
        # Create the directory if it does not exist
        logger.info(f"Creating directory {LOCAL_DATA_DIR}")
        os.mkdir(f"{LOCAL_DATA_DIR}/{channel_id}")

    # Write the response to a file
    open(f"{LOCAL_DATA_DIR}/{channel_id}/video_info.json", "w").write(
        str(json_response)
    )
    logger.info(f"Response written to {LOCAL_DATA_DIR}/{channel_id}.json")


if __name__ == "__main__":
    main()
