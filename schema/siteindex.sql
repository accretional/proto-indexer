CREATE TABLE databases (
    db_name      TEXT,
    db_path      TEXT,
    db_schema    TEXT,
    url_resolver TEXT,
    num_records  INTEGER,
    file_size    INTEGER,
    index_time   TEXT
);

CREATE TABLE database_metadata (
    uuid        TEXT PRIMARY KEY,
    db_path     TEXT,
    num_records INTEGER,
    file_size   INTEGER,
    index_time  TEXT
);
