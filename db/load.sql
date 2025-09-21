COPY atom_entries FROM 'db/atom_entries.csv' (FORMAT 'csv', force_not_null 'date', quote '"', delimiter ',', header 1);
COPY workouts FROM 'db/workouts.csv' (FORMAT 'csv', force_not_null ('date', 'seq'), quote '"', delimiter ',', header 1);
