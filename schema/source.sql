CREATE TABLE files (
    id       INTEGER PRIMARY KEY,
    repo     TEXT NOT NULL,
    repo_url TEXT NOT NULL,
    path     TEXT NOT NULL,
    language TEXT,
    size     INTEGER NOT NULL,
    sha256   TEXT NOT NULL,
    content  TEXT
);
CREATE INDEX files_repo_path ON files(repo, path);

CREATE TABLE IF NOT EXISTS files_vectors (
    file_id   INTEGER PRIMARY KEY REFERENCES files(id),
    provider  TEXT    NOT NULL,
    model     TEXT    NOT NULL,
    vector    BLOB    NOT NULL
);
