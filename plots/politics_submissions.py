# 1 plot for displaying count of submissions in politics on each day from nov 1 to 14
import psycopg2
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os 

load_dotenv('db_cred.env')

# Db params
db_params = {
    'dbname': 'politics',
    'user': os.environ.get("DB_USER"),
    'password': os.environ.get("DB_PASSWORD"),
    'host': 'localhost',
    'port': '5432',
}

# db query to get the submission counts
query = """
    SELECT
        DATE_TRUNC('day', comment_created_time) AS submission_day,
        COUNT(DISTINCT submission_id) AS unique_submission_count
    FROM
        reddit_comments
    WHERE
        comment_created_time >= '2023-11-01'::timestamp
        AND comment_created_time < '2023-11-15'::timestamp
    GROUP BY
        submission_day
    ORDER BY
        submission_day;
"""

# Connect to db
conn = psycopg2.connect(**db_params)

# Query n fetch results
with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()

conn.close()

x_values = [result[0] for result in results]
y_values = [result[1] for result in results]

# plotting the graph
plt.figure(figsize=(12, 6))
plt.bar(x_values, y_values, color='skyblue')
plt.xlabel('Date')
plt.ylabel('Number of Unique Submissions')
plt.title('Number of Unique Submissions per Day in r/politics (Nov 1 - Nov 14)')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig('sub_daily.png')
