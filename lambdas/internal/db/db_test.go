package db

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

func TestNew(t *testing.T) {
	d := New(func(ctx context.Context) (map[string]string, error) {
		return map[string]string{
			"host":     "localhost",
			"port":     "5432",
			"dbname":   "testdb",
			"username": "user",
			"password": "pass",
		}, nil
	})

	if d == nil {
		t.Fatal("expected non-nil PgxDB")
	}
	if d.pool != nil {
		t.Fatal("expected nil pool before init")
	}
}

func TestNew_CredsError(t *testing.T) {
	d := New(func(ctx context.Context) (map[string]string, error) {
		return nil, fmt.Errorf("secret not found")
	})

	_, err := d.Query(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected error from Query with bad creds")
	}
	if err.Error() != "get db credentials: secret not found" {
		t.Errorf("unexpected error: %v", err)
	}

	// Insert should also fail
	_, err = d.Insert(context.Background(), "INSERT INTO x VALUES (1) RETURNING id")
	if err == nil {
		t.Fatal("expected error from Insert with bad creds")
	}

	// Exec should also fail
	err = d.Exec(context.Background(), "DELETE FROM x")
	if err == nil {
		t.Fatal("expected error from Exec with bad creds")
	}
}

func TestSerializeValue(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want any
	}{
		{"nil", nil, nil},
		{"string", "hello", "hello"},
		{"int", 42, 42},
		{"bytes", []byte("data"), "data"},
		{"json_number_int", json.Number("42"), int64(42)},
		{"json_number_float", json.Number("3.14"), float64(3.14)},
		{"json_number_string", json.Number("not_a_number"), "not_a_number"},
		{"bool", true, true},
		{"float64", 3.14, 3.14},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SerializeValue(tt.in)
			if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", tt.want) {
				t.Errorf("SerializeValue(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestPool(t *testing.T) {
	d := New(func(ctx context.Context) (map[string]string, error) {
		return map[string]string{
			"host":     "localhost",
			"port":     "5432",
			"dbname":   "testdb",
			"username": "user",
			"password": "pass",
		}, nil
	})

	// Pool should be nil before any operation
	pool := d.Pool()
	if pool != nil {
		t.Error("expected nil pool before init")
	}
}

func TestNew_DefaultPort(t *testing.T) {
	d := New(func(ctx context.Context) (map[string]string, error) {
		return map[string]string{
			"host":     "localhost",
			"database": "mydb",
			"username": "user",
			"password": "pass",
		}, nil
	})

	// Will fail to connect but exercises the credential parsing with default port and "database" key
	_, err := d.Query(context.Background(), "SELECT 1")
	if err == nil {
		t.Log("query succeeded unexpectedly (maybe a local pg is running)")
	}
}

func TestNew_DefaultDbname(t *testing.T) {
	d := New(func(ctx context.Context) (map[string]string, error) {
		return map[string]string{
			"host":     "localhost",
			"port":     "5432",
			"username": "user",
			"password": "pass",
		}, nil
	})

	// Will fail to connect but exercises the code path for default dbname="postgres"
	_, err := d.Insert(context.Background(), "INSERT INTO x VALUES (1) RETURNING id")
	if err == nil {
		t.Log("insert succeeded unexpectedly")
	}
}

func TestNew_ExecInitError(t *testing.T) {
	d := New(func(ctx context.Context) (map[string]string, error) {
		return nil, fmt.Errorf("creds unavailable")
	})

	err := d.Exec(context.Background(), "DELETE FROM x")
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "get db credentials: creds unavailable" {
		t.Errorf("unexpected error: %v", err)
	}
}
