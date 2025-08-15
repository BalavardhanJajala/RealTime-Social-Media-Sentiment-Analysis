# 1 Plot for showing each subreddit as a bar and its count
# Each subreddit count represented as bars
import psycopg2
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv

load_dotenv('db_cred.env')
# Db connection parameters for Reddit
reddit_db_params = {
     "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": "localhost",
    "database": "reddit",
}

# List of subreddits
subreddits = [
    'worldnews',
    'news',
    'conspiracy',
    'TrueReddit',
    'offbeat',
    'soccer',
    'science',
    'movies',
    'breakingbad',
    'gameofthrones',
    'gaming',
    'announcements'
]

# Query to get the count of comments for each subreddit
subreddit_count_query = """
    SELECT subreddit, COUNT(*) as comment_count
    FROM reddit_comments
    WHERE subreddit IN %s
    GROUP BY subreddit
    ORDER BY subreddit;
"""

# Function to connect to the database and run a query
def run_query(query, db_params, params=None):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    cur.execute(query, params)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# Fetch subreddit counts
subreddit_counts = run_query(subreddit_count_query, reddit_db_params, (tuple(subreddits),))
subreddit_names, comment_counts = zip(*subreddit_counts)

# Plotting the data as a bar graph
plt.bar(subreddit_names, comment_counts, color='blue')
plt.xlabel('Subreddit')
plt.ylabel('Comment Count')
plt.title('Comment Counts for Each Subreddit')
plt.xticks(rotation=45, ha='right')  
plt.tight_layout()  
plt.savefig('Each_Subreddit_count_plot4.png')
