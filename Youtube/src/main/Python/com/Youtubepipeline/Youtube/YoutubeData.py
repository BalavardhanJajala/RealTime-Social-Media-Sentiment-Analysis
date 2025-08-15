import time
import requests
import psycopg2
from datetime import datetime
from threading import Event

# === DB Config ===
DB_HOST = "localhost"
DB_NAME = "youtube"
DB_USER = "postgres"
DB_PASS = "150030441@klU"

# === YouTube API Config ===
API_KEY = "AIzaSyCwXjwmK-p7eWrher4jnBUHtDVfR54yhq4"
MAX_RESULTS = 100

# === Global pagination variable ===
next_page_token = None


# --- DB: Get latest videoId ---
def fetch_video_id_from_database(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT videoId FROM videos ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else None


# --- DB: Insert comment if new ---
def insert_comment_into_database(conn, comment_id, comment_text, comment_created_datetime, target_video_id):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM comments WHERE comment_id = %s", (comment_id,))
        if cur.fetchone():
            return  # Skip if exists

        timestamp = datetime.strptime(comment_created_datetime, "%Y-%m-%d %H:%M:%S")
        cur.execute("""
            INSERT INTO comments (comment_id, comment_text, comment_created_datetime, target_video_id)
            VALUES (%s, %s, %s, %s)
        """, (comment_id, comment_text, timestamp, target_video_id))
        conn.commit()


# --- Main logic to fetch & store ---
def fetch_and_insert_youtube_comments(conn):
    global next_page_token

    video_id = fetch_video_id_from_database(conn)
    if not video_id:
        print("No videoId found in videos table.")
        return

    api_url = (
        f"https://www.googleapis.com/youtube/v3/commentThreads"
        f"?key={API_KEY}&textFormat=plainText&part=snippet"
        f"&videoId={video_id}&maxResults={MAX_RESULTS}"
    )
    if next_page_token:
        api_url += f"&pageToken={next_page_token}"

    response = requests.get(api_url)
    if response.status_code != 200:
        print(f"Error Response Code: {response.status_code}")
        print(response.text)
        return

    json_response = response.json()
    items = json_response.get("items", [])

    for item in items:
        snippet = item.get("snippet", {})
        top_comment_snippet = snippet.get("topLevelComment", {}).get("snippet", {})

        comment_id = item.get("id")
        comment_text = top_comment_snippet.get("textDisplay", "")
        published_at = top_comment_snippet.get("publishedAt", "")
        target_video_id = top_comment_snippet.get("videoId", "")

        # Convert "2023-10-15T12:34:56Z" → "YYYY-MM-DD HH:MM:SS"
        try:
            dt_obj = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
            comment_created_datetime_formatted = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            print(f"Date parse error: {e}")
            continue

        insert_comment_into_database(
            conn,
            comment_id,
            comment_text,
            comment_created_datetime_formatted,
            target_video_id
        )

    next_page_token = json_response.get("nextPageToken")
    print("Insertion done")


# --- Scheduler: runs every 5 minutes ---
def scheduler(interval_sec=300):
    stop_event = Event()
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )

        while not stop_event.is_set():
            fetch_and_insert_youtube_comments(conn)
            stop_event.wait(interval_sec)

    except Exception as e:
        print(f"DB/Logic error: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    scheduler()