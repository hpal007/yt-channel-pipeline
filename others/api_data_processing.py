from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag
def api_data_processing():

    create_comments_table = SQLExecuteQueryOperator(
        task_id="create_comments_table",
        conn_id="postgres_default",
        sql=""" 
            CREATE TABLE IF NOT EXISTS comments (
                channel_id VARCHAR(50),
                videoId VARCHAR(50),
                comment VARCHAR(2000),
                author_name VARCHAR(50),
                likes_count INTEGER,
                replies_count INTEGER,
                published_at VARCHAR(50)
            )
            """,
    )

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        from src.yt_utils import youtube

        request = youtube.commentThreads().list(
            part="snippet",
            videoId="GzZZQr5BJf8",
            maxResults=50,
            textFormat="plainText",
        )
        response = request.execute()

        if response.get("items"):
            print("API is available and returned data.")
            return PokeReturnValue(is_done=True, xcom_value=response["items"])
        return PokeReturnValue(is_done=False, xcom_value=None)

    @task
    def extract_comments(api_data):
        # api_data = ti.xcom_pull(task_ids='is_api_available')
        print("Extracting comments data...")
        extracted_data = []
        for item in api_data:
            channel_id = item["snippet"]["topLevelComment"]["snippet"][
                "authorChannelId"
            ]["value"]
            video_id = item["snippet"]["videoId"]
            comment = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            author_name = item["snippet"]["topLevelComment"]["snippet"][
                "authorDisplayName"
            ]
            likes_count = item["snippet"]["topLevelComment"]["snippet"]["likeCount"]
            replies_count = item["snippet"].get("totalReplyCount", 0)
            published_at = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]

            # Here you would typically insert the data into a database
            print(
                f"Channel ID: {channel_id}, Video ID: {video_id}, Comment: {comment}, Author: {author_name}, Likes: {likes_count}, Replies: {replies_count}, Published At: {published_at}"
            )
            extracted_data.append(
                {
                    "channel_id": channel_id,
                    "videoId": video_id,
                    "comment": comment,
                    "author_name": author_name,
                    "likes_count": likes_count,
                    "replies_count": replies_count,
                    "published_at": published_at,
                }
            )
            # For demonstration, we will return a sample dictionary
        return extracted_data

    @task
    def process_comments(comments_info):
        import json
        # This function can be used to process the comments data further if needed
        print("Processing comments data...")
        with open("/tmp/comments_data.json", "w") as f:
            json.dump(comments_info, f)
        print("Comments data written to /tmp/comments_data.json")

    @task
    def store_comments_to_db():
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        import json

        with open("/tmp/comments_data.json", "r") as f:
            comments_data = json.load(f)
        for comment in comments_data:
            cursor.execute(
                """
                INSERT INTO comments (channel_id, videoId, comment, author_name, likes_count, replies_count, published_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    comment["channel_id"],
                    comment["videoId"],
                    comment["comment"],
                    comment["author_name"],
                    comment["likes_count"],
                    comment["replies_count"],
                    comment["published_at"],
                ),
            )
        conn.commit()
        cursor.close()
        conn.close()

    (
        process_comments(extract_comments(create_comments_table >> is_api_available()))
        >> store_comments_to_db()
    )


api_data_processing()
