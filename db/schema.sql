CREATE SEQUENCE seq_workout_id INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 NO CYCLE;;
CREATE TABLE wodsets(id UUID DEFAULT(gen_random_uuid()) PRIMARY KEY, track_name VARCHAR, date DATE UNIQUE, scraped_at TIMESTAMP);;
CREATE TABLE workouts(id INTEGER DEFAULT(nextval('seq_workout_id')) PRIMARY KEY, wodset_id UUID, wod_section VARCHAR, wod_title VARCHAR, workout_name VARCHAR, workout_description VARCHAR, wod_results_count INTEGER, wod_results_url VARCHAR, FOREIGN KEY (wodset_id) REFERENCES wodsets(id));;

