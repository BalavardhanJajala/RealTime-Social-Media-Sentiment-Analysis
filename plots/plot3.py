# 3 Plots for hate speech in all 3 sources Politics, Yt, reddit using modern hate speech API data
import os
import psycopg2
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

# db queries

reddit_hateful_query = "SELECT count(*) FROM reddit_comments WHERE hatevalue='hateful';"
reddit_not_hateful_query = "SELECT count(*) FROM reddit_comments WHERE hatevalue='not hateful';"

politics_hateful_query = "SELECT count(*) FROM reddit_comments WHERE hatevalue='hateful';"
politics_not_hateful_query = "SELECT count(*) FROM reddit_comments WHERE hatevalue='not hateful';"

youtube_hateful_query = "SELECT count(*) FROM comments WHERE hatevalue='hateful';"
youtube_not_hateful_query = "SELECT count(*) FROM comments WHERE hatevalue='not hateful';"

# Function to connect to the database and run a query
def run_query(query, db_params):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    return result

# Function to fetch data from the database and return hate speech counts
def fetch_hate_speech_counts(db_params, hateful_query, not_hateful_query):
    hateful_count = run_query(hateful_query, db_params)
    not_hateful_count = run_query(not_hateful_query, db_params)
    return hateful_count, not_hateful_count

# Fetch hate speech counts for Reddit, Youtube, Politics
reddit_hateful_counts = fetch_hate_speech_counts(reddit_db_params, reddit_hateful_query, reddit_not_hateful_query)

politics_hateful_counts = fetch_hate_speech_counts(politics_db_params, politics_hateful_query, politics_not_hateful_query)
youtube_hateful_counts = fetch_hate_speech_counts(youtube_db_params, youtube_hateful_query, youtube_not_hateful_query)

# Plotting the data as separate bar graphs for hate speech
hate_speech_values = ['Hateful', 'Not Hateful']


# Function to format y-axis values
def format_y_values(value, pos):
    if value >= 1e6:
        return f'{value / 1e6:.0f}M'
    elif value >= 1e3:
        return f'{value / 1e3:.0f}K'
    else:
        return f'{value:.0f}'


# Plot for Reddit, politics n youtube
plt.bar(hate_speech_values, reddit_hateful_counts, color=['red', 'blue'])
plt.xlabel('Hate Speech')
plt.ylabel('Count')
plt.title('Hate Speech Analysis in Reddit Comments')
plt.gca().yaxis.set_major_formatter(FuncFormatter(format_y_values))
plt.savefig('reddit_hate_speech_analysis_plot3.png')
plt.clf()  

plt.bar(hate_speech_values, politics_hateful_counts, color=['red', 'blue'])
plt.xlabel('Hate Speech')
plt.ylabel('Count')
plt.title('Hate Speech Analysis in Politics Comments')
plt.gca().yaxis.set_major_formatter(FuncFormatter(format_y_values))
plt.savefig('politics_hate_speech_analysis_plot3.png')
plt.clf()   

plt.bar(hate_speech_values, youtube_hateful_counts, color=['red', 'blue'])
plt.xlabel('Hate Speech')
plt.ylabel('Count')
plt.title('Hate Speech Analysis in Youtube Comments')
plt.gca().yaxis.set_major_formatter(FuncFormatter(format_y_values))
plt.savefig('youtube_hate_speech_analysis_plot3.png')
