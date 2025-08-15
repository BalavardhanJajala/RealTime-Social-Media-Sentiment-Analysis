from flask import Flask, request, jsonify, render_template
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv('db_cred.env')


app = Flask(__name__)

# Db details for our data sources
youtube_db_params = {
    'dbname': 'youtube',
    'user': os.environ.get("DB_USER"),
    'password': os.environ.get("DB_PASSWORD"),
    'host': 'localhost',
    'port': '5432'
}

reddit_db_params = {
    'dbname': 'reddit',
     'user': os.environ.get("DB_USER"),
    'password': os.environ.get("DB_PASSWORD"),
    'host': 'localhost',
    'port': '5432'
}

# Common methods to conenct and execute the queries

def connect_to_database(db_params):
    try:
        connection = psycopg2.connect(**db_params)
        return connection
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None
#Common functiong we wrote to return multiple value results
def execute_query(connection, query, params):
    try:
        cursor = connection.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()
        return results
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
    finally:
        if connection:
            connection.close()
# Common method to execute query that returns a single value
def execute_count_query(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()[0]
        return result
    except Exception as e:
        print(f"Error executing count query: {e}")
        return None
    finally:
        if cursor:
            cursor.close()

######################------------- MY MAIN FUNCTIONALITY FROM HERE----------------------##############

##---------We have developed 5 APIs -----------###
## First API - To return the count of comments per day for each data source between selected date range
## Second API - To return the sentiment percentages in each subreddit or videoID - based on selection of data source and date range
## Third API - To return Negative and hateful matching percentages for Reddit and YouTube in selected date range
#Fourth API - Just like second one, showing hate speech in each and every video ID and subreddit
#Fifth API - For basic data like : how many comments have been collected so far and processed so far

# Function to get YouTube comments count based on start date and end date

def get_youtube_comments_count(start_date, end_date):
    query = """
       SELECT
    DATE_TRUNC('day', db_insert_time) AS insert_date,
    COUNT(comment_id) AS comment_count
FROM
    comments
WHERE
    DATE_TRUNC('day', db_insert_time) >= %s::date
    AND DATE_TRUNC('day', db_insert_time) <= %s::date
GROUP BY
    insert_date
ORDER BY
    insert_date;

    """
    params = (start_date, end_date)
    return execute_query(connect_to_database(youtube_db_params), query, params)

# Function to get Reddit comments count based on start date and end date
def get_reddit_comments_count(start_date, end_date):
    query = """
       SELECT
    DATE_TRUNC('day', db_insertion_time) AS insert_date,
    COUNT(comment_id) AS comment_count
FROM
    reddit_comments
WHERE
    DATE_TRUNC('day', db_insertion_time) >= %s::date
    AND DATE_TRUNC('day', db_insertion_time) <= %s::date
GROUP BY
    insert_date
ORDER BY
    insert_date;

    """
    params = (start_date, end_date)
    return execute_query(connect_to_database(reddit_db_params), query, params)


# Landing Page
@app.route('/')
def index():
    return render_template('dashboard.html')

# Basic Data API
@app.route('/basic/')
def get_basic_data():
    try:
        # Execute queries for Reddit database
        reddit_connection = connect_to_database(reddit_db_params)
        reddit_total = execute_count_query(reddit_connection, "SELECT COUNT(*) FROM reddit_comments;")
        reddit_processed = execute_count_query(reddit_connection, "SELECT COUNT(*) FROM reddit_comments WHERE language IS NOT NULL;")

        # Execute queries for YouTube database
        youtube_connection = connect_to_database(youtube_db_params)
        youtube_total = execute_count_query(youtube_connection, "SELECT COUNT(*) FROM comments;")
        youtube_processed = execute_count_query(youtube_connection, "SELECT COUNT(*) FROM comments WHERE language IS NOT NULL;")

        # Return the results as JSON
        result = {
            'reddit_total': reddit_total,
            'reddit_processed': reddit_processed,
            'youtube_total': youtube_total,
            'youtube_processed': youtube_processed
        }

        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)})

# First API
@app.route('/data/')
def get_comments_data():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date is None or end_date is None:
        return jsonify({'error': 'Start date and end date params needed!!'})

    youtube_results = get_youtube_comments_count(start_date, end_date)
    reddit_results = get_reddit_comments_count(start_date, end_date)

    if youtube_results is not None and reddit_results is not None:
        data = {'youtube': youtube_results, 'reddit': reddit_results}
        return jsonify(data)
    else:
        return jsonify({'error': 'Unable to retrieve comments data'})


# Second API

@app.route('/sentiment/')
def get_sentiment_data():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    source = request.args.get('source')

    if start_date is None or end_date is None or source not in ['youtube', 'reddit']:
        return jsonify({'error': 'Invalid parameters'})

    if source == 'youtube':
        connection = connect_to_database(youtube_db_params)
        query = """
          SELECT
    v.songname AS target_song_name,
    (COUNT(c.comment_id) FILTER (WHERE c.value = 'positive') * 100.0 /
    NULLIF(COUNT(c.comment_id) FILTER (WHERE c.value IN ('positive', 'neutral', 'negative')), 0)) AS positive_percentage,
    (COUNT(c.comment_id) FILTER (WHERE c.value = 'neutral') * 100.0 /
    NULLIF(COUNT(c.comment_id) FILTER (WHERE c.value IN ('positive', 'neutral', 'negative')), 0)) AS neutral_percentage,
    (COUNT(c.comment_id) FILTER (WHERE c.value = 'negative') * 100.0 /
    NULLIF(COUNT(c.comment_id) FILTER (WHERE c.value IN ('positive', 'neutral', 'negative')), 0)) AS negative_percentage
FROM
    comments c
JOIN
    videos v ON c.target_video_id = v.videoid
WHERE
    DATE_TRUNC('day', c.db_insert_time) >= %s::date
    AND DATE_TRUNC('day', c.db_insert_time) <= %s::date
GROUP BY
    v.songname
ORDER BY
    v.songname;

        """
    elif source == 'reddit':
        connection = connect_to_database(reddit_db_params)
        query = """
            SELECT
                subreddit,
                (COUNT(comment_id) FILTER (WHERE value = 'positive') * 100.0 /
                NULLIF(COUNT(comment_id) FILTER (WHERE value IN ('positive', 'neutral', 'negative')), 0)) AS positive_percentage,
                (COUNT(comment_id) FILTER (WHERE value = 'neutral') * 100.0 /
                NULLIF(COUNT(comment_id) FILTER (WHERE value IN ('positive', 'neutral', 'negative')), 0)) AS neutral_percentage,
                (COUNT(comment_id) FILTER (WHERE value = 'negative') * 100.0 /
                NULLIF(COUNT(comment_id) FILTER (WHERE value IN ('positive', 'neutral', 'negative')), 0)) AS negative_percentage
            FROM
                reddit_comments
            WHERE
                DATE_TRUNC('day', db_insertion_time) >= %s::date
    AND DATE_TRUNC('day', db_insertion_time) <= %s::date
            GROUP BY
                subreddit
            ORDER BY
                subreddit;
        """
    
    params = (start_date, end_date)
    results = execute_query(connection, query, params)
    
    if results is not None:
        return jsonify({source: results})
    else:
        return jsonify({'error': 'Unable to retrieve sentiment data'})





# Fourth API - Similar to Sentiment in 2nd API, how is Hate Speech in each and every video Id and subreddit


@app.route('/hatespeech/')
def get_hatespeech_data():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    source = request.args.get('source')

    if start_date is None or end_date is None or source not in ['youtube', 'reddit']:
        return jsonify({'error': 'Invalid parameters'})

    if source == 'youtube':
        connection = connect_to_database(youtube_db_params)
        query = """
     SELECT
    v.songname AS song_name,
    (COUNT(c.comment_id) FILTER (WHERE c.hatevalue = 'hateful') * 100.0 /
        NULLIF(COUNT(c.comment_id) FILTER (WHERE c.hatevalue IN ('hateful', 'not hateful')), 0)) AS hateful_percentage,
    (COUNT(c.comment_id) FILTER (WHERE c.hatevalue = 'not hateful') * 100.0 /
        NULLIF(COUNT(c.comment_id) FILTER (WHERE c.hatevalue IN ('hateful', 'not hateful')), 0)) AS not_hateful_percentage
FROM
    comments c
JOIN
    videos v ON c.target_video_id = v.videoid
WHERE
    DATE_TRUNC('day', c.db_insert_time) >= %s::date
    AND DATE_TRUNC('day', c.db_insert_time) <= %s::date
GROUP BY
    v.songname
ORDER BY
    v.songname;


        """
    elif source == 'reddit':
        connection = connect_to_database(reddit_db_params)
        query = """
            SELECT
    subreddit,
    (COUNT(comment_id) FILTER (WHERE hatevalue = 'hateful') * 100.0 /
        NULLIF(COUNT(comment_id) FILTER (WHERE hatevalue IN ('hateful', 'not hateful')), 0)) AS hateful_percentage,
    (COUNT(comment_id) FILTER (WHERE hatevalue = 'not hateful') * 100.0 /
        NULLIF(COUNT(comment_id) FILTER (WHERE hatevalue IN ('hateful', 'not hateful')), 0)) AS not_hateful_percentage
FROM
    reddit_comments
WHERE
    DATE_TRUNC('day', db_insertion_time) >= %s::date
    AND DATE_TRUNC('day', db_insertion_time) <= %s::date
GROUP BY
    subreddit
ORDER BY
    subreddit;

        """
    
    params = (start_date, end_date)
    results = execute_query(connection, query, params)
    
    if results is not None:
        return jsonify({source: results})
    else:
        return jsonify({'error': 'Unable to retrieve Hatespeech data'})


# Third API - How Much Sentiment = Negative and Hate Speech presence is matching overall in a source
@app.route('/hatepercentage/')
def get_hatepercentage_data():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    source = request.args.get('source')

    if start_date is None or end_date is None or source not in ['youtube', 'reddit']:
        return jsonify({'error': 'Invalid parameters'})

    if source == 'youtube':
        connection = connect_to_database(youtube_db_params)
        query = """
            SELECT
                (COUNT(comment_id) FILTER (WHERE value = 'negative' AND hatevalue = 'hateful') * 100.0 /
                NULLIF(COUNT(comment_id) FILTER (WHERE hatevalue IN ('hateful', 'not hateful')), 0)) AS matched_percentage,
                (100.0 - (COUNT(comment_id) FILTER (WHERE value = 'negative' AND hatevalue = 'hateful') * 100.0 /
                    NULLIF(COUNT(comment_id) FILTER (WHERE hatevalue IN ('hateful', 'not hateful')), 0))) AS not_matched_percentage
            FROM
                comments
            WHERE
                DATE_TRUNC('day', db_insert_time) >= %s::date
    AND DATE_TRUNC('day', db_insert_time) <= %s::date
                AND hatevalue IN ('hateful', 'not hateful');
        """
    elif source == 'reddit':
        connection = connect_to_database(reddit_db_params)
        query = """
            SELECT
                (COUNT(comment_id) FILTER (WHERE value = 'negative' AND hatevalue = 'hateful') * 100.0 /
                NULLIF(COUNT(comment_id) FILTER (WHERE hatevalue IN ('hateful', 'not hateful')), 0)) AS matched_percentage,
                (100.0 - (COUNT(comment_id) FILTER (WHERE value = 'negative' AND hatevalue = 'hateful') * 100.0 /
                    NULLIF(COUNT(comment_id) FILTER (WHERE hatevalue IN ('hateful', 'not hateful')), 0))) AS not_matched_percentage
            FROM
                reddit_comments
            WHERE
                 DATE_TRUNC('day', db_insertion_time) >= %s::date
    AND DATE_TRUNC('day', db_insertion_time) <= %s::date
                AND hatevalue IN ('hateful', 'not hateful');
        """
    
    params = (start_date, end_date)
    results = execute_query(connection, query, params)
    
    if results is not None:
        return jsonify({source: results})
    else:
        return jsonify({'error': 'Unable to retrieve hate percentage data'})


# My application running on Port 80 and accessible from anywhere once started running
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=80)
