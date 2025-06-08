import os

from src.yt_utils import logger, youtube


def main(channel_id):
    logger.info(f"Extracting channel info for channel ID: {channel_id}")

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics", id=channel_id
    )
    response = request.execute()
    items = response.get("items", [])
    channel_info = {}

    for item in items:
        channel_info.update(
            {
                "channelId": item["id"],
                "title": item["snippet"].get("title"),
                "description": item["snippet"].get("description"),
                "customUrl": item["snippet"].get("customUrl"),
                "country": item["snippet"].get("country"),
                "viewCount": item["statistics"].get("viewCount"),
                "subscriberCount": item["statistics"].get("subscriberCount"),
                "videoCount": item["statistics"].get("videoCount"),
                "playlistId": item["contentDetails"]
                .get("relatedPlaylists", {})
                .get("uploads"),
            }
        )
    logger.info(f"Channel Info: {channel_info}")
    logger.info(f"Completed fetching channel info for channel ID: {channel_id}")
    return channel_info

    # json_response = json.dumps(response, indent=2)
    # open("test_data/channels.json", "w").write(str(json_response))
    # print("Response written to channels.json")


if __name__ == "__main__":
    main("UCz6PEeVLG1TL6jMRTvSLm4g")
