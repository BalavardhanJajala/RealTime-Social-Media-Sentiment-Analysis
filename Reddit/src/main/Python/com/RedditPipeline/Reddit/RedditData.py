import time
import base64
import requests
import psycopg2
from datetime import datetime, timezone
from threading import Event

# Reddit OAuth credentials
CLIENT_ID = "qlQUzr9kQi2k12vy0C4RBA"
CLIENT_SECRET = "ohKP0Ao2IPhZcqfc_E2TfBsPkXlbyg"
USER_AGENT = "MyRedditApp/1.0 (by PuzzledBrother9059; jvbalavardhanyadav@gmail.com)"

# Database connection details
DB_HOST = "localhost"
DB_NAME = "reddit"
DB_USER = "postgres"
DB_PASS = "150030441@klU"

# Get OAuth token
def get_reddit_access_token():
    url = "https://www.reddit.com/api/v1/access_token"
    auth = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    headers = {
        "User-Agent": USER_AGENT,
        "Authorization": f"Basic {auth}"
    }
    data = {"grant_type": "client_credentials"}

    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        print(f"Could not get access token. Status: {response.status_code}")
        return None

# Fetch subreddit list from DB
def fetch_subreddits(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT subreddit_name FROM subreddits")
        return [row[0] for row in cur.fetchall()]

# Insert comment if not exists
def insert_comment_if_new(conn, comment_id, comment_text, subreddit, comment_created_time, submission_id):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM reddit_comments WHERE comment_id = %s", (comment_id,))
        if not cur.fetchone():
            cur.execute("""
                INSERT INTO reddit_comments 
                (comment_id, comment_text, subreddit, comment_created_time, submission_id, db_insertion_time)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (comment_id) DO NOTHING
            """, (comment_id, comment_text, subreddit, comment_created_time, submission_id))
            conn.commit()

# Main job
def fetch_and_store_comments():
    token = get_reddit_access_token()
    if not token:
        return

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )

        subreddits = fetch_subreddits(conn)

        for subreddit_name in subreddits:
            url = f"https://oauth.reddit.com/r/{subreddit_name}/comments.json"
            headers = {
                "Authorization": f"Bearer {token}",
                "User-Agent": USER_AGENT
            }
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print(f"HTTP error for {subreddit_name}: {response.status_code}")
                continue

            data = response.json()
            if "error" in data:
                print(f"Reddit API error for {subreddit_name}: {data['error']}")
                continue

            if "data" in data and "children" in data["data"]:
                for child in data["data"]["children"]:
                    comment_data = child["data"]
                    comment_id = comment_data.get("id")
                    submission_id = comment_data.get("link_id")
                    comment_text = comment_data.get("body", "")
                    created_utc = comment_data.get("created_utc", 0)
                    comment_created_time = datetime.fromtimestamp(created_utc, tz=timezone.utc)

                    insert_comment_if_new(
                        conn,
                        comment_id,
                        comment_text,
                        subreddit_name,
                        comment_created_time,
                        submission_id
                    )

        conn.close()

    except Exception as e:
        print(f"Error: {e}")

# Scheduler: run every 5 minutes
def scheduler(interval_sec=300):
    stop_event = Event()
    while not stop_event.is_set():
        fetch_and_store_comments()
        stop_event.wait(interval_sec)

if __name__ == "__main__":
    scheduler()