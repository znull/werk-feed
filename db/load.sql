COPY workouts FROM 'db/workouts.csv' (FORMAT 'csv', force_not_null ('date', 'seq'), quote '"', delimiter ',', header 1);
