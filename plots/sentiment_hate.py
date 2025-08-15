import os
import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# env variables
load_dotenv('db_cred.env')

# Db params
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

# Connect to db  & run the queries
def run_query(query, db_params):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results

# Fetch data 
youtube_query = "SELECT value, hatevalue FROM comments WHERE hatevalue NOT IN ('api-error', 'not applicable');"
youtube_data = run_query(youtube_query, youtube_db_params)
youtube_df = pd.DataFrame(youtube_data, columns=['value', 'hatevalue'])

reddit_query = "SELECT value, hatevalue FROM reddit_comments WHERE hatevalue NOT IN ('api-error', 'not applicable');"
reddit_data = run_query(reddit_query, reddit_db_params)
reddit_df = pd.DataFrame(reddit_data, columns=['value', 'hatevalue'])

# Plotting 
sns.histplot(youtube_df, x="value", hue="hatevalue", stat="percent", multiple="dodge", shrink=.8)
plt.title('Sentiment - Hate Speech presence Summary in Youtube')
plt.savefig('youtube_sentiment_hate.png')
plt.clf()

sns.histplot(reddit_df, x="value", hue="hatevalue", stat="percent", multiple="dodge", shrink=.8)
plt.title('Sentiment - Hate Speech presence Summary in Reddit')
plt.savefig('reddit_sentiment_hate.png')
plt.clf()
