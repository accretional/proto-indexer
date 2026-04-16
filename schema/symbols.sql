CREATE TABLE symbols (
    id             INTEGER PRIMARY KEY,
    repo           TEXT NOT NULL,
    repo_url       TEXT NOT NULL,
    proto_package  TEXT NOT NULL,
    kind           TEXT NOT NULL, -- message | service | method | enum
    name           TEXT NOT NULL,
    fqn            TEXT NOT NULL,
    file_path      TEXT NOT NULL,
    line           INTEGER,
    descriptor     BLOB NOT NULL,
    input_fqn      TEXT, -- methods only
    output_fqn     TEXT  -- methods only
);
CREATE INDEX symbols_fqn     ON symbols(fqn);
CREATE INDEX symbols_name    ON symbols(name);
CREATE INDEX symbols_kind    ON symbols(kind);
CREATE INDEX symbols_package ON symbols(proto_package);
