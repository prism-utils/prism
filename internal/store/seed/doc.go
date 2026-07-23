// Package seed writes idempotent zero-row Parquet placeholders so DuckDB
// read_parquet globs always match at least one file for freshly provisioned tenants.
package seed
