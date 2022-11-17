from collections import OrderedDict
from typing import Dict, Iterable

STAGING_TABLES: OrderedDict[str, Iterable[str]] = OrderedDict(
    staging_events=(
        "artist varchar(256)",
        "auth varchar(256)",
        "firstname varchar(256)",
        "gender varchar(256)",
        "iteminsession int4",
        "lastname varchar(256)",
        "length numeric(18,0)",
        "level varchar(256)",
        "location varchar(256)",
        "method varchar(256)",
        "page varchar(256)",
        "registration numeric(18,0)",
        "sessionid int4",
        "song varchar(256)",
        "status int4",
        "ts int8",
        "useragent varchar(256)",
        "userid int4",
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
        "songid varchar(256) NOT NULL",
        "title varchar(256)",
        "artistid varchar(256)",
        "year int4",
        "duration numeric(18,0)",
    ),
    dim_artists=(
        "artistid varchar(256) NOT NULL",
        "name varchar(256)",
        "location varchar(256)",
        "lattitude numeric(18,0)",
        "longitude numeric(18,0)",
    ),
    dim_users=(
        "userid int4 NOT NULL",
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
        "playid varchar(32) NOT NULL",
        "start_time timestamp NOT NULL",
        "userid int4 NOT NULL",
        "level varchar(256)",
        "songid varchar(256)",
        "artistid varchar(256)",
        "sessionid int4",
        "location varchar(256)",
        "user_agent varchar(256)",
    ),
)
STAR_TABLES_CONSTRAINTS: Dict[str, Iterable[str]] = {
    "dim_songs": ("CONSTRAINT songs_pkey PRIMARY KEY (songid)",),
    "dim_users": ("CONSTRAINT users_pkey PRIMARY KEY (userid)",),
    "dim_artists": ("CONSTRAINT artists_pkey PRIMARY KEY (artistid)",),
    "dim_time": ("CONSTRAINT time_pkey PRIMARY KEY (start_time)",),
    "fact_songplays": (
        "CONSTRAINT songplays_pkey PRIMARY KEY (playid)",
        (
            "CONSTRAINT FK_songplays_time FOREIGN KEY(start_time) REFERENCES "
            "dim_time(start_time)"
        ),
        (
            "CONSTRAINT FK_songplays_users FOREIGN KEY(userid) REFERENCES "
            "dim_users(userid)"
        ),
        (
            "CONSTRAINT FK_songplays_songs FOREIGN KEY(songid) REFERENCES "
            "dim_songs(songid)"
        ),
        (
            "CONSTRAINT FK_songplays_artists FOREIGN KEY(artistid) REFERENCES "
            "dim_artists(artistid)"
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
            dim_artists (artistid, name, location, latitude, longitude)
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
            dim_songs (songid, title, artist_id, year, duration)
        SELECT DISTINCT
            song_id, title, artist_id, year, duration
        FROM
            staging_songs
        WHERE
            song_id IS NOT NULL
    """,
    dim_users="""
        INSERT INTO
            dim_users (userid, first_name, last_name, gender, level)
        SELECT DISTINCT
            userid, firstname, lastname, gender, level
        FROM
            staging_events
        WHERE
            userid IS NOT NULL
    """,
    fact_songplays="""
        INSERT INTO
            fact_songplays
            (playid,
            start_time,
            userid,
            level,
            songid,
            artistid,
            sessionid,
            location,
            user_agent)
        SELECT DISTINCT
            md5(events.sessionid || events.start_time) AS playid,
            e.start_time,
            e.userId,
            e.level,
            s.song_id,
            e.artist_id,
            e.sessionId,
            e.location,
            e.userAgent
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
            ON e.song = s.title
            AND e.artist = s.artist_name
            AND e.length = s.duration
        WHERE
            e.ts IS NOT NULL AND e.userId IS NOT NULL
            AND s.song_id IS NOT NULL AND e.artist_id IS NOT NULL
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
