from src.yt_utils import logger, youtube


def main(channel_to_search):

    logger.info(f"Searching for channel: {channel_to_search}")

    request = youtube.search().list(
        part="snippet", type="channel", maxResults=1, q=channel_to_search
    )
    # TODO: use maxResults=50 to get more results, as each request will cost 100 credits.

    response = request.execute()
    items = response.get("items", [])

    channel_search_info = {}
    for item in items:
        if (
            item["snippet"]["channelTitle"] == channel_to_search
            and item["id"]["kind"] == "youtube#channel"
        ):
            logger.info(f"Found {channel_to_search} channel!")

            channel_search_info.update(
                {
                    "channelId": item["id"]["channelId"],
                    "title": item["snippet"]["title"],
                    "channelTitle": item["snippet"]["channelTitle"],
                    "description": item["snippet"]["description"],
                    "publishedAt": item["snippet"]["publishedAt"],
                }
            )
    logger.info(f"Channel Info: {channel_search_info}")

    return channel_search_info
    # json_response = json.dumps(response, indent=2)
    # open("test_data/channel.json", "w").write(str(json_response))
    # print("Response written to channel.json")


if __name__ == "__main__":
    main("Krish Naik")
