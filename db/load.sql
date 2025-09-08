COPY wodsets FROM 'db/wodsets.csv' (FORMAT 'csv', force_not_null 'date', quote '"', delimiter ',', header 1);
COPY workouts FROM 'db/workouts.csv' (FORMAT 'csv', quote '"', delimiter ',', header 1);
