import requests
import sqlite3
import pandas as pd

# Create a session object
session = requests.Session()

class AppConfig:
    """Configuration class for API details and endpoints."""
    
    API_HOST = "imdb188.p.rapidapi.com"
    API_KEY = "bc2c5f4993msh344e364ea756a1fp153763jsnbb51caaac9ea"
    HEADERS = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": API_HOST
    }
    # API Endpoints
    GET_POPULAR_CELEBRITIES_URL = f'https://{API_HOST}/api/v1/getPopularCelebrities'
    GET_WEEK_TOP_10_URL = f'https://{API_HOST}/api/v1/getWeekTop10'
    SEARCH_IMDB_URL = f'https://{API_HOST}/api/v1/searchIMDB'

    # List of endpoints to fetch data from
    URL_ENDPOINTS = [GET_POPULAR_CELEBRITIES_URL, GET_WEEK_TOP_10_URL, SEARCH_IMDB_URL]

def create_database_connection(db_file):
    """Create and return a SQLite database connection.

    :param db_file: Path to the SQLite database file.
    :type db_file: str
    :returns: SQLite connection object or None if connection failed.
    :rtype: sqlite3.Connection or None
    """
    connection = None
    try:
        connection = sqlite3.connect(db_file)
    except Exception as e:
        print(e)
    return connection

def fetch_data_from_api(endpoint):
    """Fetch data from the specified API endpoint.

    :param endpoint: The API endpoint to fetch data from.
    :type endpoint: str
    :returns: JSON response from the API or None if the request failed.
    :rtype: dict or None
    """
    response = session.get(endpoint, headers=AppConfig.HEADERS)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None

def gather_movie_data():
    """Fetch data from multiple movie-related API endpoints.

    :returns: List of data fetched from each endpoint.
    :rtype: list
    """
    data_collection = []
    for url in AppConfig.URL_ENDPOINTS:
        data = fetch_data_from_api(url)
        if data:
            data_collection.append(data)
    return data_collection

def insert_popular_celebrities(connection, celebrities_df):
    """Insert popular celebrities data into the SQLite database.

    :param connection: SQLite connection object.
    :type connection: sqlite3.Connection
    :param celebrities_df: DataFrame containing celebrities' data.
    :type celebrities_df: pandas.DataFrame
    """
    cursor = connection.cursor()
    # Create table
    cursor.execute('''CREATE TABLE IF NOT EXISTS popular_celebrities (
        id TEXT PRIMARY KEY,
        name TEXT,
        birth_date TEXT,
        height REAL,
        current_rank INTEGER
    )''')

    # Insert data
    for _, row in celebrities_df.iterrows():
        cursor.execute('''INSERT OR REPLACE INTO popular_celebrities (id, name, birth_date, height, current_rank)
            VALUES (?, ?, ?, ?, ?)''', (
            row['id'], 
            row['nameText.text'], 
            row['birthDateComponents.displayableProperty.value.plainText'], 
            row.get('height.measurement.value', None), 
            row.get('meterRanking.currentRank', None)
        ))
    connection.commit()

def insert_weekly_top_10(connection, weekly_top_10_df):
    """Insert weekly top 10 movie data into the SQLite database.

    :param connection: SQLite connection object.
    :type connection: sqlite3.Connection
    :param weekly_top_10_df: DataFrame containing weekly top 10 data.
    :type weekly_top_10_df: pandas.DataFrame
    """
    cursor = connection.cursor()
    # Drop the table if it already exists to avoid conflicts with the schema
    cursor.execute('DROP TABLE IF EXISTS weekly_top_10')

    # Create the table with the correct schema
    cursor.execute('''CREATE TABLE IF NOT EXISTS weekly_top_10 (
        id TEXT PRIMARY KEY,
        title TEXT,
        release_year INTEGER,
        rating REAL,
        vote_count INTEGER,
        rank INTEGER,
        provider TEXT
    )''')

    # Insert the data into the table
    for _, row in weekly_top_10_df.iterrows():
        categorized_watch_options_list = row.get('watchOptionsByCategory.categorizedWatchOptionsList', [])
        
        # Check if the list is not empty before accessing its elements
        if categorized_watch_options_list and 'watchOptions' in categorized_watch_options_list[0]:
            provider = categorized_watch_options_list[0]['watchOptions'][0].get('provider.name.value', 'Unknown')
        else:
            provider = 'Unknown'  # Default value if no provider found

        cursor.execute('''INSERT OR REPLACE INTO weekly_top_10 (id, title, release_year, rating, vote_count, rank, provider)
            VALUES (?, ?, ?, ?, ?, ?, ?)''', (
            row['id'],
            row['titleText.text'],
            row.get('releaseYear.year', None),
            row.get('ratingsSummary.aggregateRating', None),
            row.get('ratingsSummary.voteCount', None),
            row.get('chartMeterRanking.currentRank', None),
            provider
        ))

    connection.commit()

def insert_movie_data(connection, movie_data):
    """Insert movie data into the SQLite database.

    :param connection: SQLite connection object.
    :type connection: sqlite3.Connection
    :param movie_data: Movie data in JSON format.
    :type movie_data: dict
    """
    # Normalize data to handle the nested structure
    movie_data_df = pd.json_normalize(movie_data['data'])

    # Create table for the movie dataset
    cursor = connection.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS movies (
        id TEXT PRIMARY KEY,
        qid TEXT,
        title TEXT,
        year INTEGER,
        stars TEXT,
        q TEXT,
        image_url TEXT
    )''')

    # Insert the data into the table
    for _, row in movie_data_df.iterrows():
        cursor.execute('''INSERT OR REPLACE INTO movies (id, qid, title, year, stars, q, image_url)
            VALUES (?, ?, ?, ?, ?, ?, ?)''', (
            row['id'], 
            row['qid'], 
            row['title'], 
            row.get('year', None), 
            row.get('stars', 'Unknown'), 
            row['q'], 
            row.get('image', None)
        ))

    # Commit the changes to the database
    connection.commit()

def display_celebrities_kpis(connection):
    """Display key performance indicators for popular celebrities.

    :param connection: SQLite connection object.
    :type connection: sqlite3.Connection
    """
    # Read data from the 'popular_celebrities' table
    celebrities_df = pd.read_sql('SELECT * FROM popular_celebrities', connection)

    # Calculate KPIs: average height and count of celebrities
    kpi_data = celebrities_df.agg(
        avg_height=('height', 'mean'),
        celeb_count=('id', 'count')
    )

    print("\nCelebrity KPIs (Average Height and Celebrity Count):\n")
    print(kpi_data)

def display_weekly_top_kpis(connection):
    """Display key performance indicators for weekly top 10 movies.

    :param connection: SQLite connection object.
    :type connection: sqlite3.Connection
    """
    # Read data from the 'weekly_top_10' table
    weekly_top_df = pd.read_sql('SELECT * FROM weekly_top_10', connection)

    # Calculate KPIs: average rating and count of movies
    kpi_data = weekly_top_df.agg(
        avg_rating=('rating', 'mean'),
        movie_count=('id', 'count')
    )

    print("\nWeekly Top 10 KPIs (Average Rating and Movie Count):\n")
    print(kpi_data)

def display_movies_kpis(connection):
    """Display key performance indicators for movies.

    :param connection: SQLite connection object.
    :type connection: sqlite3.Connection
    """
    # Read data from the 'movies' table
    movies_df = pd.read_sql('SELECT * FROM movies', connection)

    # Calculate KPIs: average year of release and count of movies per category (q)
    kpi_data = movies_df.groupby('q').agg(
        avg_year=('year', 'mean'),
        movie_count=('id', 'count')
    )

    print("\nMovie KPIs (Average Year of Release and Movie Count per Category):\n")
    print(kpi_data)

def display_latest_movie_report(connection):
    """Display a report of the latest 10 movies.

    :param connection: SQLite connection object.
    :type connection: sqlite3.Connection
    """
    # SQL Report Query: Fetch movies with stars and year details
    report_query = '''
        SELECT title, year, stars, q
        FROM movies
        ORDER BY year DESC
        LIMIT 10
    '''
    
    report_df = pd.read_sql(report_query, connection)

    print("\nMovie Report (Latest 10 Movies):\n")
    print(report_df)

def update_database_with_api_data():
    """Fetch and insert data from API into SQLite database.

    This function gathers data from multiple API endpoints and stores it in
    the SQLite database. It also displays KPIs and reports for the stored data.
    """
    # Fetch the data from the API
    popular_celebrities, weekly_top_10, movie_data = gather_movie_data()  

    # Normalize data to handle nested structures
    popular_celebrities_df = pd.json_normalize(popular_celebrities['data']['list'])
    weekly_top_10_df = pd.json_normalize(weekly_top_10['data'])
    
    # Create SQLite connection
    conn = create_database_connection('imdb_data.db')

    # Insert data into the first table (popular_celebrities)
    insert_popular_celebrities(conn, popular_celebrities_df)

    # Insert data into the second table (weekly_top_10)
    insert_weekly_top_10(conn, weekly_top_10_df)

    # Insert data into the third table (movies)
    insert_movie_data(conn, movie_data)

    # Display KPIs and Reports
    display_celebrities_kpis(conn)
    display_weekly_top_kpis(conn)
    display_movies_kpis(conn)
    display_latest_movie_report(conn)

    # Close the connection
    conn.close()

if __name__ == '__main__':
    update_database_with_api_data()
