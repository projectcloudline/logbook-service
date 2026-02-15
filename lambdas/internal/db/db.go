// Package db provides a database interface and pgxpool implementation for Lambda functions.
package db

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxvec "github.com/pgvector/pgvector-go/pgx"
)

// DB defines the database operations used by Lambda handlers.
type DB interface {
	Query(ctx context.Context, sql string, args ...any) ([]map[string]any, error)
	Insert(ctx context.Context, sql string, args ...any) (string, error)
	Exec(ctx context.Context, sql string, args ...any) error
	Pool() *pgxpool.Pool
}

// CredentialsFunc returns database credentials as a JSON-encoded map with keys:
// host, port, dbname, username, password.
type CredentialsFunc func(ctx context.Context) (map[string]string, error)

// PgxDB implements DB using pgxpool.
type PgxDB struct {
	credsFn CredentialsFunc
	pool    *pgxpool.Pool
	once    sync.Once
	initErr error
}

// New creates a new PgxDB with lazy pool initialization.
func New(credsFn CredentialsFunc) *PgxDB {
	return &PgxDB{credsFn: credsFn}
}

func (d *PgxDB) init(ctx context.Context) error {
	d.once.Do(func() {
		creds, err := d.credsFn(ctx)
		if err != nil {
			d.initErr = fmt.Errorf("get db credentials: %w", err)
			return
		}

		host := creds["host"]
		port := creds["port"]
		if port == "" {
			port = "5432"
		}
		dbname := creds["dbname"]
		if dbname == "" {
			dbname = creds["database"]
		}
		if dbname == "" {
			dbname = "postgres"
		}
		user := creds["username"]
		pass := creds["password"]

		connStr := fmt.Sprintf(
			"postgres://%s:%s@%s:%s/%s?search_path=logbook,public&pool_max_conns=2&connect_timeout=10",
			user, pass, host, port, dbname,
		)

		config, err := pgxpool.ParseConfig(connStr)
		if err != nil {
			d.initErr = fmt.Errorf("parse pool config: %w", err)
			return
		}

		config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			return pgxvec.RegisterTypes(ctx, conn)
		}

		pool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			d.initErr = fmt.Errorf("create pool: %w", err)
			return
		}
		d.pool = pool
	})
	return d.initErr
}

// Pool returns the underlying pgxpool.Pool, initializing it if needed.
func (d *PgxDB) Pool() *pgxpool.Pool {
	return d.pool
}

// Query executes a SQL query and returns results as a slice of maps.
// This mirrors Python's RealDictCursor behavior.
func (d *PgxDB) Query(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
	if err := d.init(ctx); err != nil {
		return nil, err
	}

	rows, err := d.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	fieldDescs := rows.FieldDescriptions()
	var results []map[string]any

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		row := make(map[string]any, len(fieldDescs))
		for i, fd := range fieldDescs {
			row[fd.Name] = values[i]
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	return results, nil
}

// Insert executes a SQL INSERT with RETURNING id and returns the id as a string.
func (d *PgxDB) Insert(ctx context.Context, sql string, args ...any) (string, error) {
	if err := d.init(ctx); err != nil {
		return "", err
	}

	var id any
	err := d.pool.QueryRow(ctx, sql, args...).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("insert: %w", err)
	}

	return fmt.Sprintf("%v", id), nil
}

// Exec executes a SQL statement that does not return rows.
func (d *PgxDB) Exec(ctx context.Context, sql string, args ...any) error {
	if err := d.init(ctx); err != nil {
		return err
	}

	_, err := d.pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	return nil
}

// SerializeValue converts database values to JSON-friendly types.
// Handles UUIDs, time.Time, Decimal, etc.
func SerializeValue(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []byte:
		return string(val)
	case json.Number:
		if i, err := val.Int64(); err == nil {
			return i
		}
		if f, err := val.Float64(); err == nil {
			return f
		}
		return val.String()
	default:
		return v
	}
}
