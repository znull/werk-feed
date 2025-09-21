from feedgen.feed import FeedGenerator, FeedEntry
from zoneinfo import ZoneInfo
from datetime import datetime, time, timedelta
import duckdb
import hashlib
import json
import os
import re
import requests
import sys
import urllib.parse
import lxml.etree
from urllib.parse import urlparse, urlunparse, quote, urlencode
from uuid import uuid5, NAMESPACE_OID

def populate_workouts(conn, data):
    for wodset in data["wodsets"]:
        for i, entry in enumerate(wodset["entries"]):
            workout = entry["workout"]
            conn.execute("""
                INSERT OR REPLACE INTO workouts (
                    date, seq, wod_section, wod_title,
                    workout_name, wod_results_count, wod_results_url,
                    workout_description
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                wodset['date'], i, entry["wod_section"],
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

# BTWB API wants a specific date format:
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
    custom_quote = lambda s, safe, encoding=None, errors=None: quote(s, safe + '()', encoding, errors)
    query = urlencode(params, quote_via=custom_quote)
    url = f"{base}?{query}"
    data = fetch_wod_json(url)

    with duckdb.connect() as conn:
        conn.execute(f"IMPORT DATABASE '{db}'")
        populate_workouts(conn, data)
        update_entries(conn)
        conn.execute(f"EXPORT DATABASE '{db}'")

    # normalize db dump csv
    with duckdb.connect() as conn:
        conn.execute(f"IMPORT DATABASE '{db}'")
        conn.execute(f"EXPORT DATABASE '{db}'")

def update_entries(conn):
    offset = 1
    for entry in feed_entries(conn):
        # init entry if not already present
        now = datetime.now()
        conn.execute("""
            INSERT OR IGNORE INTO atom_entries (date, created_at, csum)
            VALUES (?, ?, ?)
        """, [ entry.wod_date, now, entry_csum(entry) ])

        # update if changed
        query = 'SELECT 1 FROM atom_entries WHERE date = ? AND csum = ?'
        csum = entry_csum(entry)
        if conn.execute(query, [entry.wod_date, csum]).fetchone():
            pass # csum unchanged
        else:
            query = '''
                UPDATE atom_entries
                SET csum = ?, updated_at = ?
                WHERE date = ?
            '''
            conn.execute(query, [csum, now + timedelta(seconds=offset), entry.wod_date])
            offset += 1

def dump_feed(db, fh):
    with duckdb.connect() as conn:
        conn.execute(f"IMPORT DATABASE '{db}'")
        feed = generate_feed(conn)
    fh.write(feed.atom_str(pretty=True))

def generate_feed(conn):
    feed = FeedGenerator()
    feed.title("Crossfit Werk WODs")
    feed.subtitle('scraped from https://crossfitwerk.de/workout-of-the-day')
    feed_url = 'https://znull.github.io/werk-feed/workouts.atom'
    feed.id(feed_url)
    feed.link(href=feed_url, rel='self')
    feed.link(href='https://crossfitwerk.de/workout-of-the-day', rel='alternate')
    feed.language('en')
    feed.logo('https://images.squarespace-cdn.com/content/v1/638096caaf6dba73fe17c5c8/a599d2e8-074d-4aa0-a6db-f99537367f72/253590-2015_12_17_09_38_50.png?format=1500w')

    for entry in feed_entries(conn):
        feed.add_entry(entry)

    return feed

class WodInfo(object):
    def __init__(self, wod, created_at, updated_at):
        self.wods = [ wod ]
        self.date = wod['date']
        self.created_at = created_at
        self.updated_at = updated_at

    def add(self, wod):
        self.wods.append(wod)

def feed_entries(conn):
    query = """
    SELECT
        w.date,
        wod_title,
        workout_name,
        workout_description,
        wod_results_url,
        created_at,
        updated_at
    FROM workouts w LEFT JOIN atom_entries ae
    ON w.date = ae.date
    ORDER BY w.date, seq
    """
    results = conn.execute(query).fetchall()
    wodinfo = []
    for date, title, name, description, results_url, created_at, updated_at in results:
        wod = {
            'date': date,
            'title': title,
            'name': name,
            'description': description,
            'results_url': results_url,
        }
        if wodinfo and wodinfo[-1].date == date:
            wodinfo[-1].add(wod)
        else:
            wodinfo.append(WodInfo(wod, created_at, updated_at))

    for wi in wodinfo:
        content = ""
        for workout in wi.wods:
            content += f"<h3>{workout['title'] or workout['name']}</h3>\n"
            content += f"<p>{workout['description']}</p>\n\n"
        content = re.sub(r'(&#13;|&#10;|\r|\n)', '<br/>\n', content)
        content = re.sub(r'\n*(<br/>\n*){2,}', '\n<br/><br/>\n', content)

        entry = FeedEntry()
        date = workout['date']
        entry.guid(str(uuid5(NAMESPACE_OID, str(date))))
        entry.title(f"Workout for {date.strftime("%a %b %-d, %Y")}")
        entry.link({'href': workout['results_url'], 'rel': 'related', 'title': 'BTWB'})
        entry.content(content, type='CDATA')
        entry.published(wi.created_at.replace(tzinfo=ZoneInfo('Europe/Berlin')))
        updated_at = updated_at or wi.created_at
        entry.updated(updated_at.replace(tzinfo=ZoneInfo('Europe/Berlin')))
        #print(lxml.etree.tostring(entry.atom_entry()), file=sys.stderr)
        entry.wod_date = date
        yield entry

def entry_csum(entry):
    hasher = hashlib.md5()
    hasher.update(entry.__dict__['_FeedEntry__atom_title'].encode('utf-8'))
    hasher.update(entry.__dict__['_FeedEntry__atom_content']['content'].encode('utf-8'))
    hasher.update(entry.__dict__['_FeedEntry__atom_link'][0]['href'].encode('utf-8'))
    return hasher.hexdigest()

def strip_query(url):
    return urlunparse(urlparse(url)._replace(query=""))

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='wod feed generator')
    parser.add_argument('action', type=str, choices=['scrape', 'feed'],
                        help='Action to perform: "scrape" to collect data or "feed" to generate atom feed')
    parser.add_argument('--db', type=str, default='db', help='Path to DuckDB database export')

    args = parser.parse_args()

    if args.action == 'feed':
        dump_feed(args.db, sys.stdout.buffer)
    elif args.action == 'scrape':
        scrape(args.db)
    else:
        parser.print_help()
