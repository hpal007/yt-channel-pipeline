from src.yt_utils import logger, youtube, process_videos


def main(playlist_id):
    logger.info(f"Extracting playlist items for playlist ID: {playlist_id}")

    videos_list = []
    next_page_token = None

    while True:

        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,  # get it from yt-api-channels.py
            maxResults=50,
            pageToken=next_page_token,
        )
        response = request.execute()
        if not response.get("items"):
            logger.info("No items found in this playlist.")
            break

        videos_list.extend(process_videos(response["items"]))

        next_page_token = response.get("nextPageToken")

        if not next_page_token:
            logger.info("No more pages of playlist items.")
            break

    logger.info(f"Total videos fetched: {len(videos_list)}")
    return videos_list

    # json_response = json.dumps(response, indent=2)
    # open("playlistitems.json", "w").write(
    #     str(json_response)
    # )
    # print("Response written to playlistitems.json")


if __name__ == "__main__":
    main("UUz6PEeVLG1TL6jMRTvSLm4g")
