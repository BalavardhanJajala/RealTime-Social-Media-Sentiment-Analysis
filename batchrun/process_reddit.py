# To process the REDDIT records in batches to find out Sentiment score, Hate speech detection & langugage detection
import psycopg2
import requests
from textblob import TextBlob
from langdetect import detect
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
        # Updating the hate-value = api-error, which will be processed separately for getting right api values later
        print("API request error:", e)
        return "api-error"

# Connection to DB
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Fetch 1000 comments at a time where language/value/hatevalue is empty
    query = "SELECT comment_text, comment_id FROM reddit_comments WHERE language IS NULL AND value IS NULL AND hatevalue IS NULL LIMIT 1000"
    cursor.execute(query)
    comments_to_process = cursor.fetchall()

    for comment_row in comments_to_process:
        comment = comment_row[0]

        # Language detection
        try:
            detected_language = detect(comment)
        except Exception as lang_detection_error:
            # If language is not detected, keeping language as unknown
            print("Language detection error:", lang_detection_error)
            detected_language = "unknown"

        # Sentiment analysis
        if detected_language == "en":
            blob = TextBlob(comment)
            sentiment = blob.sentiment.polarity

            if sentiment > 0:
                sentiment_label = "positive"
            elif sentiment == 0:
                sentiment_label = "neutral"
            else:
                sentiment_label = "negative"

            # Hateful comment evaluation
            is_hateful = hs_check_comment(comment)
        else:
            sentiment_label = "not applicable"
            is_hateful = "not applicable"

        # print("Comment: ", comment)
        # print("Detected Language: ", detected_language)
        # print("Sentiment: ", sentiment_label)
        # print("Hateful: ", is_hateful)
        # print("\n")

        # Update the database immediately
        update_query = "UPDATE reddit_comments SET language = %s, value = %s, hatevalue = %s WHERE comment_id = %s"
        cursor.execute(update_query, (detected_language, sentiment_label, is_hateful, comment_row[1]))
        conn.commit()

    print("Processing done")

except psycopg2.Error as db_error:
    print("Database connection error:", db_error)

finally:
    # Close db connection
    if conn is not None:
        cursor.close()
        conn.close()
