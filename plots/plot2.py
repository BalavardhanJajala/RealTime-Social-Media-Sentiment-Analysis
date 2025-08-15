# 3 sentiment bar graphs for Politics, Reddit & Youtube

import psycopg2
import os
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from matplotlib.ticker import FuncFormatter


load_dotenv('db_cred.env')
# Database connection parameters for YouTube, Reddit & Politics
youtube_db_params = {
     "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": "localhost",
    "database": "youtube",
}

reddit_db_params = {
     "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": "localhost",
    "database": "reddit",
}

politics_db_params = {
     "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": "localhost",
    "database": "politics",
}

# Queries for the db
reddit_positive_query = "SELECT count(*) FROM reddit_comments WHERE value='positive';"
reddit_negative_query = "SELECT count(*) FROM reddit_comments WHERE value='negative';"
reddit_neutral_query = "SELECT count(*) FROM reddit_comments WHERE value='neutral';"

politics_positive_query = "SELECT count(*) FROM reddit_comments WHERE value='positive';"
politics_negative_query = "SELECT count(*) FROM reddit_comments WHERE value='negative';"
politics_neutral_query = "SELECT count(*) FROM reddit_comments WHERE value='neutral';"

youtube_positive_query = "SELECT count(*) FROM comments WHERE value='positive';"
youtube_negative_query = "SELECT count(*) FROM comments WHERE value='negative';"
youtube_neutral_query = "SELECT count(*) FROM comments WHERE value='neutral';"

# Function to connect to the database and run a query
def run_query(query, db_params):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    return result

# Function to fetch data from the database and return sentiment counts
def fetch_sentiment_counts(db_params, positive_query, negative_query, neutral_query):
    positive_count = run_query(positive_query, db_params)
    negative_count = run_query(negative_query, db_params)
    neutral_count = run_query(neutral_query, db_params)
    return positive_count, negative_count, neutral_count

# Fetch sentiment counts for Reddit + politics + youtube
reddit_counts = fetch_sentiment_counts(reddit_db_params, reddit_positive_query, reddit_negative_query, reddit_neutral_query)
politics_counts = fetch_sentiment_counts(politics_db_params, politics_positive_query, politics_negative_query, politics_neutral_query)
youtube_counts = fetch_sentiment_counts(youtube_db_params, youtube_positive_query, youtube_negative_query, youtube_neutral_query)

# Plotting the data as separate bar graphs
sentiments = ['Positive', 'Negative', 'Neutral']



# Function to format y-axis values
def format_y_values(value, pos):
    if value >= 1e6:
        return f'{value / 1e6:.0f}M'
    elif value >= 1e3:
        return f'{value / 1e3:.0f}K'
    else:
        return f'{value:.0f}'
    

# Plot for reddit, politics, youtube
plt.bar(sentiments, reddit_counts, color=['green', 'red', 'blue'])
plt.xlabel('Sentiment')
plt.ylabel('Count')
plt.title('Sentiment Analysis of Reddit Comments')
plt.gca().yaxis.set_major_formatter(FuncFormatter(format_y_values))

plt.savefig('reddit_sentiment_analysis_plot2.png')
plt.clf()   

plt.bar(sentiments, politics_counts, color=['green', 'red', 'blue'])
plt.xlabel('Sentiment')
plt.ylabel('Count')
plt.title('Sentiment Analysis of Politics Comments')
plt.gca().yaxis.set_major_formatter(FuncFormatter(format_y_values))

plt.savefig('politics_sentiment_analysis_plot2.png')
plt.clf()   

plt.bar(sentiments, youtube_counts, color=['green', 'red', 'blue'])
plt.xlabel('Sentiment')
plt.ylabel('Count')
plt.title('Sentiment Analysis of Youtube Comments')
plt.gca().yaxis.set_major_formatter(FuncFormatter(format_y_values))

plt.savefig('youtube_sentiment_analysis_plot2.png')
