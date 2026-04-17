CREATE TABLE files (
    id       INTEGER PRIMARY KEY,
    repo     TEXT NOT NULL,
    repo_url TEXT NOT NULL,
    path     TEXT NOT NULL,
    language TEXT,
    size     INTEGER NOT NULL,
    sha256   TEXT NOT NULL,
    content  TEXT NOT NULL
);
CREATE INDEX files_repo_path ON files(repo, path);

CREATE VIRTUAL TABLE files_fts USING fts5(
    path,
    content,
    content='files',
    content_rowid='id'
);

CREATE TRIGGER files_ai AFTER INSERT ON files BEGIN
    INSERT INTO files_fts(rowid, path, content) VALUES (new.id, new.path, new.content);
END;

CREATE TABLE IF NOT EXISTS files_vectors (
    file_id   INTEGER PRIMARY KEY REFERENCES files(id),
    provider  TEXT    NOT NULL,
    model     TEXT    NOT NULL,
    vector    BLOB    NOT NULL
);
