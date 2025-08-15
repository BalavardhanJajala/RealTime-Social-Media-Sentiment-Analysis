# 1 Plot showing sentiment in each subreddit as seperate bars
import psycopg2
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv

load_dotenv('db_cred.env')

# Db parameters
db_params = {
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": "localhost",
}

# Db query to get distinct subreddits from reddit_comments
subreddit_query = "SELECT DISTINCT subreddit FROM reddit_comments;"

# Db query for sentiment analysis in each subreddit
sentiment_query = """
    SELECT
        subreddit,
        COALESCE(SUM(CASE WHEN value='positive' THEN 1 ELSE 0 END), 0) AS positive_count,
        COALESCE(SUM(CASE WHEN value='negative' THEN 1 ELSE 0 END), 0) AS negative_count,
        COALESCE(SUM(CASE WHEN value='neutral' THEN 1 ELSE 0 END), 0) AS neutral_count
    FROM
        reddit_comments
    WHERE
        subreddit IS NOT NULL
    GROUP BY
        subreddit;
"""

# Function to connect to the database and run a query
def run_query(query, params=None):
    conn = psycopg2.connect(**db_params, database="reddit")
    cur = conn.cursor()
    cur.execute(query, params)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# Fetch subreddits and sentiment counts
subreddit_sentiments = run_query(sentiment_query)

# Plotting the data as a single bar graph
subreddits = [result[0] for result in subreddit_sentiments]
positive_counts = [result[1] for result in subreddit_sentiments]
negative_counts = [result[2] for result in subreddit_sentiments]
neutral_counts = [result[3] for result in subreddit_sentiments]

bar_width = 0.2
index = range(len(subreddits))

plt.bar(index, positive_counts, width=bar_width, label='Positive', color='green')
plt.bar([i + bar_width for i in index], negative_counts, width=bar_width, label='Negative', color='red')
plt.bar([i + 2 * bar_width for i in index], neutral_counts, width=bar_width, label='Neutral', color='blue')

plt.xlabel('Subreddit')
plt.ylabel('Count')
plt.title('Sentiment Analysis of Comments in Subreddits')
plt.xticks([i + bar_width for i in index], subreddits, rotation=45, ha='right')
plt.legend()
plt.tight_layout()
plt.savefig('subreddit_sentiment_analysis_plot.png')
