from feedgen.feed import FeedGenerator
from zoneinfo import ZoneInfo
from datetime import datetime, time, timedelta
import duckdb
import json
import os
import re
import requests
import sys
import urllib.parse
from uuid import uuid5, NAMESPACE_OID

def populate_db(conn, data):
    now = datetime.now()
    for wodset in data["wodsets"]:
        for i, entry in enumerate(wodset["entries"]):
            workout = entry["workout"]
            conn.execute("""
                INSERT OR IGNORE INTO workouts (
                    date, seq, wod_section, updated_at, wod_title,
                    workout_name, wod_results_count, wod_results_url,
                    workout_description
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                wodset['date'], i, entry["wod_section"], now,
                entry["wod_title"], workout["workout_name"],
                workout["wod_results_count"], workout["wod_results_url"],
                workout["workout_description"]
            ])

def fetch_wod_json(url):
    headers = {
        'Accept': 'application/vnd.btwb.v1.webwidgets+json',
        'Authorization': os.environ['BTWB_TOKEN'],
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

# date=Sun Sep 14 2025 23:01:22 GMT+0200 (Mitteleuropäische Sommerzeit)
def next_sunday():
    today = datetime.now(ZoneInfo("Europe/Berlin"))
    dt = today + timedelta(days=(7 - (today.weekday()+1)%7))

    # Get timezone offset in +HHMM format
    offset = dt.strftime('%z')
    offset = offset[:3] + offset[3:]  # e.g., +0200

    tzname = dt.tzname()  # returns 'CEST' for Europe/Berlin in summer

    return dt.strftime(f'%a %b %d %Y %H:%M:%S GMT{offset} ({tzname})')

def scrape(db):
    base = 'https://webwidgets.prod.btwb.com/webwidgets/wods'
    params = dict(
        track_ids = 573806,
        activity_length = 0,
        leaderboard_length = 0,
        days = 32,
        date = next_sunday(),
    )
    custom_quote = lambda s, safe, encoding=None, errors=None: urllib.parse.quote(s, safe + '()', encoding, errors)
    query = urllib.parse.urlencode(params, quote_via=custom_quote)
    url = f"{base}?{query}"
    data = fetch_wod_json(url)

    with duckdb.connect() as conn:
        conn.execute(f"IMPORT DATABASE '{db}'")
        populate_db(conn, data)
        conn.execute(f"EXPORT DATABASE '{db}'")

def generate_feed(db):
    query = """
    SELECT
        date,
        updated_at,
        wod_title,
        workout_name,
        workout_description
    FROM workouts
    ORDER BY date, seq
    """

    with duckdb.connect() as conn:
        conn.execute(f"IMPORT DATABASE '{db}'")
        results = conn.execute(query).fetchall()

        wodsets = []
        for date, updated_at, title, name, description in results:
            wod = {
                'date': date,
                'title': title,
                'name': name,
                'description': description,
            }
            if wodsets and wodsets[-1][0]['date'] == date:
                wodsets[-1].append(wod)
            else:
                wodsets.append([wod])

    feed = FeedGenerator()
    feed.title("Crossfit Werk WODs")
    feed.subtitle('scraped from https://crossfitwerk.de/workout-of-the-day')
    feed_url = 'https://znull.github.io/werk-feed/workouts.atom'
    feed.id(feed_url)
    feed.link(href=feed_url, rel='self')
    feed.link(href='https://crossfitwerk.de/workout-of-the-day', rel='alternate')
    feed.language('en')
    feed.logo('https://images.squarespace-cdn.com/content/v1/638096caaf6dba73fe17c5c8/a599d2e8-074d-4aa0-a6db-f99537367f72/253590-2015_12_17_09_38_50.png?format=1500w')

    for ws in wodsets:
        content = ""
        for workout in ws:
            content += f"<h3>{workout['title'] or workout['name']}</h3>\n"
            content += f"<p>{workout['description']}</p>\n\n"
        content = re.sub(r'(&#13;|&#10;|\r|\n)', '<br/>\n', content)
        content = re.sub(r'\n*(<br/>\n*){2,}', '\n<br/><br/>\n', content)

        entry = feed.add_entry()
        date = workout['date']
        entry.guid(str(uuid5(NAMESPACE_OID, str(date))))
        entry.title(f"Workout for {date.strftime("%a %b %-d, %Y")}")
        entry.content(content, type='xhtml')
        #entry.updated(scraped_at.replace(tzinfo=ZoneInfo('Europe/Berlin')))
        entry.updated(datetime.combine(date, time(), tzinfo=ZoneInfo('Europe/Berlin')))

    return feed

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='wod feed generator')
    parser.add_argument('action', type=str, choices=['scrape', 'feed'],
                        help='Action to perform: "scrape" to collect data or "feed" to generate atom feed')
    parser.add_argument('--db', type=str, default='db', help='Path to DuckDB database export')

    args = parser.parse_args()

    if args.action == 'feed':
        feed = generate_feed(args.db)
        sys.stdout.buffer.write(feed.atom_str(pretty=True))
    elif args.action == 'scrape':
        scrape(args.db)
    else:
        parser.print_help()
