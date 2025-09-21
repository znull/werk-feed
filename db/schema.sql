CREATE TABLE atom_entries(date DATE, csum VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP, uuid UUID, PRIMARY KEY(date));;
CREATE TABLE workouts(date DATE, seq INTEGER, wod_section VARCHAR, wod_title VARCHAR, workout_name VARCHAR, wod_results_count INTEGER, wod_results_url VARCHAR, workout_description VARCHAR, PRIMARY KEY(date, seq));;

