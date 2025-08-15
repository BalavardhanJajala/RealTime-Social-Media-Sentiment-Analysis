# 2 plots for politics submissions and comments
# 1 plot - politics comments per every hour in a day from Nov 1 to 14
# 2 plot - politics submissions per day from nov 1 to 14

import psycopg2
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os 
from datetime import timedelta, datetime

load_dotenv('db_cred.env')

# db params
db_params = {
    'dbname': 'politics',
    'user': os.environ.get("DB_USER"),
    'password': os.environ.get("DB_PASSWORD"),
    'host': 'localhost',
    'port': '5432',
}

# query for comments count per hour
comment_query = """
   WITH hour_series AS (
    SELECT generate_series(
        '2023-11-01'::timestamp AT TIME ZONE 'UTC',
        '2023-11-14'::timestamp AT TIME ZONE 'UTC' + INTERVAL '23 hours',
        interval '1 hour'
    )::timestamp AS comment_hour
)
SELECT
    hs.comment_hour,
    COALESCE(COUNT(rc.comment_created_time), 0) AS comment_count
FROM
    hour_series hs
LEFT JOIN
    reddit_comments rc ON date_trunc('hour', rc.comment_created_time AT TIME ZONE 'UTC') = hs.comment_hour
    AND rc.comment_created_time >= '2023-11-01'::timestamp AT TIME ZONE 'UTC'
    AND rc.comment_created_time < '2023-11-14'::timestamp AT TIME ZONE 'UTC' + INTERVAL '1 day'
    AND rc.subreddit = 'politics'
GROUP BY
    hs.comment_hour
ORDER BY
    hs.comment_hour;

"""


# query for submission count
submission_query = """
   WITH date_series AS (
    SELECT generate_series(
        '2023-11-01'::timestamp AT TIME ZONE 'UTC',
        '2023-11-14'::timestamp AT TIME ZONE 'UTC',
        interval '1 day'
    )::date AS day
)
SELECT
    ds.day AS submission_day,
    COUNT(DISTINCT rc.submission_id) AS unique_submission_count
FROM
    date_series ds
LEFT JOIN
    reddit_comments rc ON DATE_TRUNC('day', rc.comment_created_time AT TIME ZONE 'UTC') = ds.day
    AND rc.comment_created_time >= '2023-11-01'::timestamp AT TIME ZONE 'UTC'
    AND rc.comment_created_time < '2023-11-15'::timestamp AT TIME ZONE 'UTC'
GROUP BY
    ds.day
ORDER BY
    ds.day;

"""


# db connection
conn = psycopg2.connect(**db_params)

# comments query
with conn.cursor() as cursor:
    cursor.execute(comment_query)
    comment_results = cursor.fetchall()

# submissions Query n results
with conn.cursor() as cursor:
    cursor.execute(submission_query)
    submission_results = cursor.fetchall()

conn.close()

# Extract x and y values  
comment_x_values = [result[0] for result in comment_results]
comment_y_values = [result[1] for result in comment_results]

submission_date_range = [result[0] for result in submission_results]
submission_y_values = [result[1] for result in submission_results]

# extracting the dates from the comments results for x-label
unique_comment_dates = list(set([date.date() for date in comment_x_values]))

# Nov 1 to Nov 14 dates
start_date = datetime(2023, 11, 1)
end_date = datetime(2023, 11, 14)
date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

# Plotting the comment graph
plt.figure(figsize=(12, 6))
plt.plot(comment_x_values, comment_y_values, linestyle='-', color='b')
plt.xlabel('Date')
plt.ylabel('Number of Comments')
plt.title('Number of Comments per Hour in r/politics from Nov 1 to 14')

plt.xticks(comment_x_values[::24], [date.strftime('%Y-%m-%d') for date in comment_x_values[::24]], rotation=45, ha='right')

plt.tight_layout()
plt.grid()
plt.savefig('comments_daily.png')

# Plotting the submission graph
plt.figure(figsize=(12, 6))
plt.plot(submission_date_range, submission_y_values, linestyle='-', color='b')
plt.xlabel('Date')
plt.ylabel('Number of submissions')
plt.title('Number of Submissions per Day in r/politics from Nov 1 to 14')

plt.xticks(date_range, [date.strftime('%Y-%m-%d') for date in date_range], rotation=45, ha='right')

plt.tight_layout()
plt.grid()
plt.savefig('sub_daily.png')
