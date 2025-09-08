CREATE SEQUENCE seq_workout_id INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 58 NO CYCLE;;
CREATE TABLE wodsets(date DATE PRIMARY KEY, uuid UUID DEFAULT(gen_random_uuid()) UNIQUE);;
CREATE TABLE workouts(wod_section VARCHAR, wod_title VARCHAR, workout_name VARCHAR, workout_description VARCHAR, wod_results_count INTEGER, wod_results_url VARCHAR, date DATE, updated_at TIMESTAMP, seq INTEGER, FOREIGN KEY (date) REFERENCES wodsets(date));;

