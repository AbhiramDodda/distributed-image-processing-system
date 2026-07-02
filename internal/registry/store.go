package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/abhiramd/petabyte-platform/internal/sandbox"
	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS algorithms (
    name       TEXT NOT NULL,
    version    TEXT NOT NULL,
    owner      TEXT NOT NULL,
    image_ref  TEXT NOT NULL,
    digest     TEXT NOT NULL,
    manifest   TEXT NOT NULL DEFAULT '{}',
    created_at DATETIME NOT NULL,
    PRIMARY KEY (name, version)
);
CREATE INDEX IF NOT EXISTS idx_algo_name  ON algorithms(name);
CREATE INDEX IF NOT EXISTS idx_algo_owner ON algorithms(owner);
`

// Algorithm is a registered, buildable algorithm version. It ties a tenant
// (Owner) to an immutable OCI image so a job referencing name+version always
// runs the exact code that was validated at registration time.
type Algorithm struct {
	Name string `json:"name"`
	Version string `json:"version"`
	Owner string `json:"owner"`
	ImageRef string `json:"image_ref"`
	Digest string `json:"digest"`
	Manifest sandbox.Manifest `json:"manifest"`
	CreatedAt time.Time `json:"created_at"`
}

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open registry db: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init registry schema: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Register inserts an algorithm version. The (name, version) pair is immutable:
// re-registering the same version is rejected so a job's code can never change
// under it. Callers bump the version (or rely on the content digest) to publish
// new code.
func (s *Store) Register(ctx context.Context, a Algorithm) error {
	manifest, err := json.Marshal(a.Manifest)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if a.CreatedAt.IsZero() {
		a.CreatedAt = time.Now()
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO algorithms (name, version, owner, image_ref, digest, manifest, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		a.Name, a.Version, a.Owner, a.ImageRef, a.Digest, string(manifest), a.CreatedAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("register %s@%s: %w", a.Name, a.Version, err)
	}
	return nil
}

func (s *Store) Get(ctx context.Context, name, version string) (*Algorithm, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT name, version, owner, image_ref, digest, manifest, created_at
		FROM algorithms WHERE name = ? AND version = ?`, name, version)
	a, err := scanAlgorithm(row)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("algorithm %s@%s not found", name, version)
	}
	if err != nil {
		return nil, err
	}
	return a, nil
}

// List returns every registered version, newest first. Small by design — the
// registry holds algorithm metadata, not data.
func (s *Store) List(ctx context.Context) ([]Algorithm, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT name, version, owner, image_ref, digest, manifest, created_at
		FROM algorithms ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list algorithms: %w", err)
	}
	defer rows.Close()

	var out []Algorithm
	for rows.Next() {
		a, err := scanAlgorithm(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

// ListVersions returns all versions of one algorithm, newest first.
func (s *Store) ListVersions(ctx context.Context, name string) ([]Algorithm, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT name, version, owner, image_ref, digest, manifest, created_at
		FROM algorithms WHERE name = ? ORDER BY created_at DESC`, name)
	if err != nil {
		return nil, fmt.Errorf("list versions of %s: %w", name, err)
	}
	defer rows.Close()

	var out []Algorithm
	for rows.Next() {
		a, err := scanAlgorithm(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

func scanAlgorithm(s scanner) (*Algorithm, error) {
	var a Algorithm
	var manifestJSON string
	if err := s.Scan(&a.Name, &a.Version, &a.Owner, &a.ImageRef, &a.Digest, &manifestJSON, &a.CreatedAt); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(manifestJSON), &a.Manifest); err != nil {
		return nil, fmt.Errorf("decode stored manifest: %w", err)
	}
	return &a, nil
}
