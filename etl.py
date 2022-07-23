import glob
import os
from datetime import datetime
from typing import Callable

import pandas as pd
from psycopg2 import connect
from psycopg2.extensions import connection, cursor

from sql_queries import (artist_table_insert, song_select, song_table_insert,
                         songplay_table_insert, time_table_insert,
                         user_table_insert)


def process_song_file(cur, filepath):
    """
    - Processes a song file in the songs dataset
    - upserts data into tables [songs, artists]

    :param cur: cursor of the database connection
    :param filepath: file path of the songs file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = (
        df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
    )
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Processes a log file in the events log dataset
    - Upserts data into tables [users, time, songplays]

    :param cur: cursor of the database connection
    :param filepath: file path of the logs file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")

    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.isocalendar().day)
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame({label: data for label, data in zip(column_labels, time_data)})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            datetime.fromtimestamp(row.ts / 1000),
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur: cursor, conn: connection, folder_path: str, func: Callable) -> None:
    """
    - Recursively iterates over folder structure data
    - Processes all json format files

    :param cur: cursor of the database connection
    :param conn: database connection
    :param folder_path: path of the root folder of a dataset
    :param func: callable processing function to use for the dataset
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(folder_path):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, folder_path))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    """
    - Establishes connection with the sparkify database and gets cursor to it.
    - Processes songs dataset
    - Processes logs dataset
    - Finally, closes the connection.
    """
    conn = connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, folder_path="data/song_data", func=process_song_file)
    process_data(cur, conn, folder_path="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
