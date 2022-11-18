from collections import OrderedDict
from typing import Dict, Iterable

STAGING_TABLES: OrderedDict[str, Iterable[str]] = OrderedDict(
    staging_events=(
        "artist_name varchar(256)",
        "auth varchar(256)",
        "first_name varchar(256)",
        "gender varchar(256)",
        "item_session int4",
        "last_name varchar(256)",
        "length numeric(18,0)",
        "level varchar(256)",
        "location varchar(256)",
        "method varchar(256)",
        "page varchar(256)",
        "registration numeric(18,0)",
        "session_id int4",
        "song varchar(256)",
        "status int4",
        "ts int8",
        "user_agent varchar(256)",
        "user_id int4",
    ),
    staging_songs=(
        "num_songs int4",
        "artist_id varchar(256)",
        "artist_name varchar(256)",
        "artist_latitude numeric(18,0)",
        "artist_longitude numeric(18,0)",
        "artist_location varchar(256)",
        "song_id varchar(256)",
        "title varchar(256)",
        "duration numeric(18,0)",
        "year int4",
    ),
)
STAR_TABLES: OrderedDict[str, Iterable[str]] = OrderedDict(
    dim_songs=(
        "song_id varchar(256) NOT NULL",
        "title varchar(256)",
        "artist_id varchar(256)",
        "year int4",
        "duration numeric(18,0)",
    ),
    dim_artists=(
        "artist_id varchar(256) NOT NULL",
        "name varchar(256)",
        "location varchar(256)",
        "latitude numeric(18,0)",
        "longitude numeric(18,0)",
    ),
    dim_users=(
        "user_id int4 NOT NULL",
        "first_name varchar(256)",
        "last_name varchar(256)",
        "gender varchar(256)",
        "level varchar(256)",
    ),
    dim_time=(
        "start_time timestamp NOT NULL",
        "hour int4",
        "day int4",
        "week int4",
        "month varchar(256)",
        "year int4",
        "weekday varchar(256)",
    ),
    fact_songplays=(
        "play_id varchar(32) NOT NULL",
        "start_time timestamp NOT NULL",
        "user_id int4 NOT NULL",
        "level varchar(256)",
        "song_id varchar(256)",
        "artist_id varchar(256)",
        "session_id int4",
        "location varchar(256)",
        "user_agent varchar(256)",
    ),
)
STAR_TABLES_CONSTRAINTS: Dict[str, Iterable[str]] = {
    "dim_songs": ("CONSTRAINT songs_pkey PRIMARY KEY (song_id)",),
    "dim_users": ("CONSTRAINT users_pkey PRIMARY KEY (user_id)",),
    "dim_artists": ("CONSTRAINT artists_pkey PRIMARY KEY (artist_id)",),
    "dim_time": ("CONSTRAINT time_pkey PRIMARY KEY (start_time)",),
    "fact_songplays": (
        "CONSTRAINT songplays_pkey PRIMARY KEY (play_id)",
        (
            "CONSTRAINT FK_songplays_time FOREIGN KEY(start_time) REFERENCES "
            "dim_time(start_time)"
        ),
        (
            "CONSTRAINT FK_songplays_users FOREIGN KEY(user_id) REFERENCES "
            "dim_users(user_id)"
        ),
        (
            "CONSTRAINT FK_songplays_songs FOREIGN KEY(song_id) REFERENCES "
            "dim_songs(song_id)"
        ),
        (
            "CONSTRAINT FK_songplays_artists FOREIGN KEY(artist_id) REFERENCES "
            "dim_artists(artist_id)"
        ),
    ),
}
STAR_TABLES_DISTSTYLES: Dict[str, str] = {
    "dim_songs": "DISTSTYLE ALL",
    "dim_artists": "DISTSTYLE ALL",
    "dim_users": "DISTSTYLE ALL",
    "dim_time": "DISTSTYLE ALL",
    "fact_songplays": "DISTSTYLE EVEN",
}
STAR_TABLES_INSERTS: Dict[str, str] = OrderedDict(
    dim_time="""
        INSERT INTO
            dim_time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT
            e.start_time,
            EXTRACT (HOUR FROM e.start_time) AS hour,
            EXTRACT (DAY FROM e.start_time) AS day,
            EXTRACT (WEEK FROM e.start_time) AS week,
            EXTRACT (MONTH FROM e.start_time) AS month,
            EXTRACT (YEAR FROM e.start_time) AS year,
            EXTRACT (DOW FROM e.start_time) AS weekday
        FROM
            (
            SELECT
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM
                staging_events
            WHERE
                page='NextSong'
            ) e
        WHERE
            start_time IS NOT NULL
    """,
    dim_artists="""
        INSERT INTO
            dim_artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM
            staging_songs
        WHERE
            artist_id IS NOT NULL
    """,
    dim_songs="""
        INSERT INTO
            dim_songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT
            song_id, title, artist_id, year, duration
        FROM
            staging_songs
        WHERE
            song_id IS NOT NULL
    """,
    dim_users="""
        INSERT INTO
            dim_users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT
            user_id, first_name, last_name, gender, level
        FROM
            staging_events
        WHERE
            user_id IS NOT NULL
    """,
    fact_songplays="""
        INSERT INTO
            fact_songplays
            (play_id,
            start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent)
        SELECT DISTINCT
            md5(e.session_id || e.start_time) AS play_id,
            e.start_time,
            e.user_id,
            e.level,
            s.song_id,
            s.artist_id,
            e.session_id,
            e.location,
            e.user_agent
        FROM
            (
            SELECT
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM
                staging_events
            WHERE
                page='NextSong'
            ) e
        JOIN
            staging_songs s
        ON
            e.song = s.title AND e.artist_name = s.artist_name AND e.length = s.duration
        WHERE
            e.ts IS NOT NULL AND e.user_id IS NOT NULL
            AND s.song_id IS NOT NULL AND e.artist_name IS NOT NULL
    """,
)


def get_drop_table_query(table_name: str) -> str:
    """Generate a DROP TABLE query given a table name.

    Args:
        table_name: table name.
    """
    return f"DROP TABLE IF EXISTS {table_name} CASCADE"


def get_create_table_query(table_name: str, table_args: Iterable[str]) -> str:
    """Generate a CREATE TABLE query given a table name and a list of arguments.

    Args:
        table_name: table name.
        table_args: An iterable of strings including column names, data types and any
            other modifiers.
    """
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(table_args)})"
