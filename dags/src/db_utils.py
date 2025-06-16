from src.yt_utils import LOCAL_DATA_DIR, logger
import sqlite3
import json
import os

# --- Configuration (Store in Airflow Variables) ---
DATABASE_NAME = "youtube_data.db"


def create_connection(db_file):
    """Create a database connection to the SQLite database specified by db_file."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        logger.info(f"Connected to SQLite database: {db_file}")
    except sqlite3.Error as e:
        print(e)
    return conn


def create_tables(conn):
    """Create the necessary tables in the SQLite database."""
    try:
        logger.info("Creating channels tables...")
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS channels (
                title TEXT,
                channel_country TEXT,
                channel_id TEXT PRIMARY KEY,
                custom_url TEXT,
                view_cont INTEGER,
                playlist_id TEXT,
                video_count INTEGER,
                description TEXT,
                published_at TEXT,
                channel_title TEXT,
                subscriber_count INTEGER
            )
        """
        )
        logger.info("Creating videos tables...")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS videos (
                video_id TEXT PRIMARY KEY,
                channel_id TEXT,
                video_title TEXT,
                video_description TEXT,
                video_published_at TEXT,
                video_view_count INTEGER,
                video_like_count INTEGER,
                video_comment_count INTEGER,
                video_duration TEXT,  -- ISO 8601 duration format (e.g., PT1H2M30S)
                video_definition TEXT, -- hd or sd
                video_caption BOOLEAN,
                video_licensed_content BOOLEAN,
                video_tags TEXT,  -- Comma-separated or JSON array
                video_category_id INTEGER,
                video_default_audio_language TEXT,
                video_default_language TEXT,

                FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
            )
        """
        )

        logger.info("Creating comments tables...")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS comments (
                comment_id TEXT PRIMARY KEY,
                video_id TEXT,
                channel_id TEXT,
                author_channel_id TEXT,
                comment_text TEXT,
                comment_published_at TEXT,
                comment_like_count INTEGER,
                comment_reply_count INTEGER,

                FOREIGN KEY (video_id) REFERENCES videos(video_id)
            )
        """
        )

        conn.commit()
        print("Tables created successfully.")

    except sqlite3.Error as e:
        print(f"Error creating tables: {e}")


def insert_channel_data(conn, data):
    """Inserts channel data into the 'channels' table."""
    sql = """
        INSERT OR IGNORE INTO channels (title, channel_country, channel_id, custom_url, view_cont, playlist_id, video_count, description, published_at, channel_title, subscriber_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cursor = conn.cursor()
        cursor.execute(
            sql,
            (
                data.get("title", ""),
                data.get("country"),
                data.get("channelId"),
                data.get("customUrl"),
                data.get("viewCount"),
                data.get("playlistId"),
                data.get("videoCount"),
                data.get("description"),
                data.get("publishedAt"),
                data.get("channelTitle"),
                data.get("subscriberCount"),
            ),
        )
        conn.commit()
        return cursor.lastrowid
    except Exception as e:
        print(f"Error inserting channel data: {e}")
        raise


def insert_video_data(conn, data):
    """Inserts video data into the 'videos' table."""
    sql = """
       INSERT OR IGNORE INTO videos (
            video_id, 
            channel_id, 
            video_title, 
            video_description, 
            video_published_at, 
            video_view_count, 
            video_like_count, 
            video_comment_count, 
            video_duration,
            video_definition, 
            video_caption, 
            video_licensed_content, 
            video_tags, 
            video_category_id, 
            video_default_audio_language,
            video_default_language)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cursor = conn.cursor()
        tags_string = ",".join(data.get("tags", [])) if data.get("tags") else ""
        cursor.execute(
            sql,
            (
                data.get("videoId"),
                data.get("channelId"),
                data.get("title"),
                data.get("description"),
                data.get("videoPublishedAt"),
                data.get("viewCount"),
                data.get("likeCount"),
                data.get("commentCount"),
                data.get("duration"),
                data.get("definition"),
                data.get("caption"),
                data.get("licensedContent"),
                tags_string,
                data.get("categoryId"),
                data.get("defaultAudioLanguage"),
                data.get("defaultLanguage"),
            ),
        )
        conn.commit()
        return cursor.lastrowid
    except sqlite3.Error as e:
        logger.error(f"Error inserting video data: {e}")
        raise


def insert_comment_data(conn, data):
    """Inserts comment data into the 'comments' table."""
    sql = """
        INSERT OR IGNORE INTO comments (comment_id, video_id, channel_id, author_channel_id, comment_text, comment_published_at, comment_like_count, comment_reply_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cursor = conn.cursor()
        cursor.execute(
            sql,
            (
                data.get("comment_id"),
                data.get("video_id"),
                data.get("channel_id"),
                data.get("author"),
                data.get("comment"),
                data.get("published_at"),
                data.get("likes_count"),
                data.get("replies_count"),
            ),
        )
        conn.commit()
        return cursor.lastrowid
    except sqlite3.Error as e:
        logger.error(f"Error inserting comment data: {e}")
        raise


def process_json_files(conn, directory, file_type):
    """Processes JSON files of a specific type and inserts the data into the database."""
    try:
        for filename in os.listdir(directory):
            if filename.endswith(".json") and file_type in filename:
                filepath = os.path.join(directory, filename)
                logger.info(f"Processing {file_type} file: {filename}")
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):  # Check if the json data is an array
                        for item in data:
                            if file_type == "channel":
                                insert_channel_data(conn, item)
                            elif file_type == "video":
                                insert_video_data(conn, item)
                            elif file_type == "comments":
                                insert_comment_data(conn, item)
                    else:
                        if file_type == "channel":
                            insert_channel_data(conn, data)
                        elif file_type == "video":
                            insert_video_data(conn, data)
                        elif file_type == "comments":
                            insert_comment_data(conn, data)
        logger.info(f"Completed processing {file_type} files.")
    except Exception as e:
        logger.error(f"Error processing JSON files: {e}")
        raise
