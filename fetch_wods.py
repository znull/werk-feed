import json
import duckdb
import os

# Load the JSON data
def load_json_data(json_str):
    return json.loads(json_str)

# Initialize the database
def init_db(db_name="workout_database.db"):
    # Create or connect to the database
    conn = duckdb.connect(db_name)
    
    # Create tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wodsets (
            id INTEGER PRIMARY KEY,
            track_name VARCHAR,
            date_string VARCHAR,
            date DATE
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS workout_entries (
            id INTEGER PRIMARY KEY,
            wodset_id INTEGER,
            wod_section VARCHAR,
            wod_title VARCHAR,
            wod_instructions VARCHAR,
            FOREIGN KEY (wodset_id) REFERENCES wodsets(id)
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS workouts (
            id INTEGER PRIMARY KEY,
            entry_id INTEGER,
            workout_name VARCHAR,
            workout_description VARCHAR,
            wod_results_count INTEGER,
            wod_results_url VARCHAR,
            wod_leaderboard_show BOOLEAN,
            wod_recent_results_show BOOLEAN,
            FOREIGN KEY (entry_id) REFERENCES workout_entries(id)
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wod_links (
            id INTEGER PRIMARY KEY,
            entry_id INTEGER,
            link VARCHAR,
            FOREIGN KEY (entry_id) REFERENCES workout_entries(id)
        )
    """)
    
    return conn

# Insert data into the database
def populate_db(conn, data):
    # Insert wodsets
    for i, wodset in enumerate(data["wodsets"]):
        conn.execute("""
            INSERT INTO wodsets (id, track_name, date_string, date)
            VALUES (?, ?, ?, ?)
        """, [i+1, wodset["track_name"], wodset["date_string"], wodset["date"]])
        
        # Insert entries for each wodset
        for j, entry in enumerate(wodset["entries"]):
            entry_id = (i * 100) + j + 1
            conn.execute("""
                INSERT INTO workout_entries (id, wodset_id, wod_section, wod_title, wod_instructions)
                VALUES (?, ?, ?, ?, ?)
            """, [entry_id, i+1, entry["wod_section"], entry["wod_title"], entry["wod_instructions"]])
            
            # Insert wod_links
            for k, link in enumerate(entry["wod_links"]):
                link_id = (entry_id * 100) + k + 1
                conn.execute("""
                    INSERT INTO wod_links (id, entry_id, link)
                    VALUES (?, ?, ?)
                """, [link_id, entry_id, link])
            
            # Insert workout details
            workout = entry["workout"]
            conn.execute("""
                INSERT INTO workouts (
                    id, entry_id, workout_name, workout_description, 
                    wod_results_count, wod_results_url, 
                    wod_leaderboard_show, wod_recent_results_show
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                entry_id, entry_id, workout["workout_name"], workout["workout_description"],
                workout["wod_results_count"], workout["wod_results_url"],
                workout["wod_leaderboard_show"], workout["wod_recent_results_show"]
            ])

# Query examples
def example_queries(conn):
    print("\n--- Example Queries ---\n")
    
    print("1. All workout dates and names:")
    result = conn.execute("""
        SELECT w.date, w.date_string, wo.workout_name 
        FROM wodsets w
        JOIN workout_entries we ON w.id = we.wodset_id
        JOIN workouts wo ON we.id = wo.entry_id
        ORDER BY w.date DESC
    """).fetchall()
    for row in result:
        print(f"{row[0]} ({row[1]}): {row[2]}")
    
    print("\n2. Workouts with Hero WODs (usually have descriptions with stories):")
    result = conn.execute("""
        SELECT w.date, wo.workout_name, LEFT(wo.workout_description, 100) || '...' as description_preview
        FROM workouts wo
        JOIN workout_entries we ON wo.entry_id = we.id
        JOIN wodsets w ON we.wodset_id = w.id
        WHERE wo.workout_description LIKE '%survived by%' 
           OR wo.workout_description LIKE '%died%'
        ORDER BY w.date DESC
    """).fetchall()
    for row in result:
        print(f"{row[0]}: {row[1]}\n  Preview: {row[2]}")
    
    print("\n3. Most popular workouts (by result count):")
    result = conn.execute("""
        SELECT w.date, wo.workout_name, wo.wod_results_count
        FROM workouts wo
        JOIN workout_entries we ON wo.entry_id = we.id
        JOIN wodsets w ON we.wodset_id = w.id
        ORDER BY wo.wod_results_count DESC
        LIMIT 5
    """).fetchall()
    for row in result:
        print(f"{row[0]}: {row[1]} - {row[2]} results")

def main():
    # Parse the JSON data
    data = load_json_data(json_str)

    # Database name
    db_name = "workout_database.db"

    # Remove existing database file if it exists
    if os.path.exists(db_name):
        os.remove(db_name)

    # Initialize the database
    conn = init_db(db_name)

    # Populate the database with data
    populate_db(conn, data)

    # Run example queries
    example_queries(conn)

    # Close the connection
    conn.close()

    print(f"\nDatabase '{db_name}' has been successfully created and populated!")
    print(f"You can connect to it using: conn = duckdb.connect('{db_name}')")

if __name__ == "__main__":
    main()
