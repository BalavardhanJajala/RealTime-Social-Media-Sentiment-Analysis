# Process API-error reddit records
import psycopg2
import requests
import os
from dotenv import load_dotenv

load_dotenv('db_cred.env')

# Db parameters
db_config = {
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": "localhost",
    "database": "reddit",
}

# Function to check the hate speech in the comments
def hs_check_comment(comment):
    CONF_THRESHOLD = 0.9
    data = {
        "token": os.environ.get("API_TOKEN_2"),
        "text": comment
    }

    try:
        response = requests.post("https://api.moderatehatespeech.com/api/v1/moderate/", json=data).json()
        if response["class"] == "flag" and float(response["confidence"]) > CONF_THRESHOLD:
            return "hateful"
        return "not hateful"
    except requests.exceptions.RequestException as e:
        # Updating the hate-value = api-error, which will be processed separately for getting the right api values later
        print("API request error:", e)
        return "api-error"

# Connection to DB
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Fetch comments where hatevalue is 'api-error'
    query = "SELECT comment_text, comment_id FROM reddit_comments WHERE hatevalue='api-error' LIMIT 1000"
    cursor.execute(query)
    comments_to_process = cursor.fetchall()

    for comment_row in comments_to_process:
        comment = comment_row[0]

        # Hateful comment evaluation
        is_hateful = hs_check_comment(comment)

        # Update the database immediately
        update_query = "UPDATE reddit_comments SET hatevalue = %s WHERE comment_id = %s"
        cursor.execute(update_query, (is_hateful, comment_row[1]))
        conn.commit()

    print("Processing done")

except psycopg2.Error as db_error:
    print("Database connection error:", db_error)

finally:
    # Close the database connection
    if conn is not None:
        cursor.close()
        conn.close()
