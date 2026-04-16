CREATE TABLE packages (
    id             INTEGER PRIMARY KEY,
    repo           TEXT NOT NULL,
    repo_url       TEXT NOT NULL,
    proto_package  TEXT NOT NULL,
    file_count     INTEGER NOT NULL,
    descriptor_set BLOB NOT NULL
);
CREATE INDEX packages_name ON packages(proto_package);
