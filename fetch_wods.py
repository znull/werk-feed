from feedgen.feed import FeedGenerator
from zoneinfo import ZoneInfo
import datetime
import duckdb
import json
import os
import requests
import uuid

def populate_db(conn, data):
    now = datetime.datetime.now()
    for i, wodset in enumerate(data["wodsets"]):
        try:
            wodset_id = conn.execute("""
                INSERT INTO wodsets (track_name, date, scraped_at)
                VALUES (?, ?, ?)
                RETURNING id
            """, [wodset["track_name"], wodset["date"], now]).fetchone()[0]
        except duckdb.ConstraintException as e:
            print(f"Error inserting: {e}")
            continue

        for j, entry in enumerate(wodset["entries"]):
            workout = entry["workout"]
            conn.execute("""
                INSERT INTO workouts (
                    wodset_id, wod_section, wod_title, workout_name,
                    workout_description, wod_results_count, wod_results_url
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                wodset_id, entry["wod_section"], entry["wod_title"],
                workout["workout_name"], workout["workout_description"],
                workout["wod_results_count"], workout["wod_results_url"]
            ])

def example_queries(conn):
    print("\n--- Example Queries ---\n")

    print("1. All workout dates and names:")
    result = conn.execute("""
        SELECT w.date, wo.workout_name
        FROM wodsets w
        JOIN workouts wo ON w.id = wo.wodset_id
        ORDER BY w.date, wo.id ASC
    """).fetchall()
    for row in result:
        print(f"{row[0]}: {row[1]}")

    print("\n2. Workouts with description:")
    result = conn.execute("""
        SELECT w.date, wo.workout_name, wo.workout_description
        FROM wodsets w
        JOIN workouts wo ON w.id = wo.wodset_id
        ORDER BY w.date, wo.id ASC
    """).fetchall()
    for row in result:
        print(f"# {row[0]}: {row[1]}\n\n{row[2]}\n")

def fetch_wod_json(url):
    try:
        headers = {
            'Accept': 'application/vnd.btwb.v1.webwidgets+json',
            'Authorization': os.environ['BTWB_TOKEN'],
            #'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:142.0) Gecko/20100101 Firefox/142.0' \
            #'Accept-Language: en-US,en;q=0.8,de-DE;q=0.5,zh-CN;q=0.3' \
            #'Accept-Encoding: gzip, deflate, br, zstd' \
            #'Origin: https://crossfitwerk.de' \
            #'Referer: https://crossfitwerk.de/' \
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise exception for HTTP errors
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")

def scrape(db):
    url = 'https://webwidgets.prod.btwb.com/webwidgets/wods?track_ids=573806&activity_length=0&leaderboard_length=0&days=40'
    data = fetch_wod_json(url)

    with duckdb.connect() as conn:
        conn.execute(f"IMPORT DATABASE '{db}'")
        populate_db(conn, data)
        #example_queries(conn)
        conn.execute(f"EXPORT DATABASE '{db}'")

def generate_feed(db):
    query = """
    SELECT
        ws.id,
        ws.date,
        w.workout_name,
        w.workout_description
    FROM workouts w
    JOIN wodsets ws ON w.wodset_id = ws.id
    ORDER BY ws.date DESC, w.id ASC
    """

    with duckdb.connect() as conn:
        conn.execute(f"IMPORT DATABASE '{db}'")
        results = conn.execute(query).fetchall()

        workouts_by_date = {}
        for workout_id, date, name, description in results:
            if date not in workouts_by_date:
                workouts_by_date[date] = []

            workouts_by_date[date].append({
                'id': workout_id,
                'name': name,
                'description': description,
            })

    feed = FeedGenerator()
    feed.title("Crossfit Werk WODs")
    feed.subtitle("Crossfit Werk WODs")
    #feed.link( href='http://larskiesow.de/test.atom', rel='self' )
    feed.link(href='https://crossfitwerk.de/workout-of-the-day', rel='alternate')
    feed.language('en')
    feed.logo('https://images.squarespace-cdn.com/content/v1/638096caaf6dba73fe17c5c8/a599d2e8-074d-4aa0-a6db-f99537367f72/253590-2015_12_17_09_38_50.png?format=1500w')
    feed.id('https://github.com/znull/werk/wods.atom')
    #feed.author( {'name':'John Doe','email':'john@example.de'} )

    now = datetime.datetime.now(ZoneInfo('Europe/Berlin'))
    for date, workouts in workouts_by_date.items():
        date_str = date.strftime("%Y-%m-%d")

        content = ""
        for workout in workouts:
            content += f"<h2>{workout['name']}</h2>\n"
            content += f"<p>{workout['description']}</p>\n\n"

        entry = feed.add_entry()
        entry.id(str(workout['id']))
        entry.title(f"Workout for {date_str}")
        entry.description(content)
        entry.updated(now)  # TODO: track content
        #link = f"https://workoutoftheday.com/workouts/{date_str}/"  # Replace with your actual URL structure

    return feed

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='wod feed generator')
    parser.add_argument('action', type=str, choices=['scrape', 'feed'],
                        help='Action to perform: "scrape" to collect data or "feed" to generate atom feed')
    parser.add_argument('--db', type=str, default='db', help='Path to DuckDB database export')
    parser.add_argument('--output', type=str, default='workouts.atom',
                        help='Path to output feed file')

    args = parser.parse_args()

    if args.action == 'feed':
        feed = generate_feed(args.db)
        with open(args.output, 'wb') as fh:
            fh.write(feed.atom_str(pretty=True))
    elif args.action == 'scrape':
        scrape(args.db)
    else:
        parser.print_help()
