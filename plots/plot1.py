# 2 plots - Line plot for Comments count on each day for all 3 data sources & Bar graph for total comments so far for 3 sources
import psycopg2
import os
import matplotlib.pyplot as plt
from datetime import datetime
from matplotlib.dates import DateFormatter
from dotenv import load_dotenv

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

# Queries for all the plots of our projects are below
# Plot 1 (Comments count per day starting from Nov 1) Queries -- Start ----

# Query for Youtube
youtube_sql_query_plot1 = """
    SELECT
        DATE_TRUNC('day', db_insert_time) AS insert_date,
        COUNT(comment_id) AS comment_count
    FROM
        comments
    WHERE
        db_insert_time >= '2023-11-01'::timestamp AT TIME ZONE 'UTC'
    GROUP BY
        insert_date
    ORDER BY
        insert_date;
"""

# Query For Reddit & Politics
reddit_sql_query_plot1 = """
    SELECT
        DATE_TRUNC('day', comment_created_time) AS insert_date,
        COUNT(comment_id) AS comment_count
    FROM
        reddit_comments
    WHERE
        comment_created_time >= '2023-11-01'::timestamp AT TIME ZONE 'UTC'
    GROUP BY
        insert_date
    ORDER BY
        insert_date;
"""
# Plot 1 Queries -- End----
 
# Function to connect to the database and fetch data
def fetch_data(query, db_params):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# Fetch data from YouTube database
youtube_data = fetch_data(youtube_sql_query_plot1, youtube_db_params)
reddit_data = fetch_data(reddit_sql_query_plot1, reddit_db_params)

politics_data = fetch_data(reddit_sql_query_plot1, politics_db_params)

# Extracting dates and counts for plotting 
youtube_dates = [row[0] for row in youtube_data]
youtube_counts = [row[1] for row in youtube_data]

reddit_dates = [row[0] for row in reddit_data]
reddit_counts = [row[1] for row in reddit_data]

politics_dates = [row[0] for row in politics_data]
politics_counts = [row[1] for row in politics_data]

# Plotting the data
fig, ax = plt.subplots()

ax.plot_date(youtube_dates, youtube_counts, '-', label='YouTube')
ax.plot_date(reddit_dates, reddit_counts, '-', label='Reddit')
ax.plot_date(politics_dates, politics_counts, '-', label='Politics')
ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
plt.xticks(rotation=45)

plt.xlabel('Date')
plt.ylabel('Comment Count')
plt.title('Reddit/Youtube/Politics Comment Count Over Time')
plt.legend()
plt.savefig('All_comment_count_plot1.png')
plt.clf()  # Clear the plot for the next one

# Bar graph representing all the total count of Politcs subreddit & Reddit & Youtube
# Calculate total count of comments for each data source
total_youtube_comments = sum(youtube_counts)
total_reddit_comments = sum(reddit_counts)
total_politics_comments = sum(politics_counts)

# Plotting the total count of comments in a bar graph
fig, ax = plt.subplots()

# Creating bars for each data source
data_sources = ['YouTube', 'Reddit', 'Politics']
comment_counts = [total_youtube_comments, total_reddit_comments, total_politics_comments]

ax.bar(data_sources, comment_counts, color=['blue', 'orange', 'green'])

plt.xlabel('Data Source')
plt.ylabel('Total Comment Count')
plt.title('Total Comment Count for YouTube, Reddit, and Politics')
plt.savefig('Total_comment_count_bar_graph.png')
plt.show()
