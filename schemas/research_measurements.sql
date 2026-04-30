CREATE TABLE measurements (
    sr_no INTEGER PRIMARY KEY AUTOINCREMENT,
    location TEXT NOT NULL,
    date TEXT,
    month TEXT,
    year INTEGER,
    season TEXT,
    parameter TEXT NOT NULL,
    actual_value TEXT,
    mean REAL,
    std_dev REAL,
    unit TEXT,
    source TEXT,
    notes TEXT
);
