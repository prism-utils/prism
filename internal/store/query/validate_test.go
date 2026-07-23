package query

import "testing"

func TestValidateReadOnlySQL_withDMLO400(t *testing.T) {
	bad := []string{
		"WITH x AS (SELECT 1) INSERT INTO metrics SELECT * FROM x",
		"WITH x AS (SELECT 1) DELETE FROM metrics",
		"WITH x AS (SELECT 1) COPY metrics TO '/tmp/out.parquet'",
		"WITH x AS (SELECT 1) UPDATE metrics SET value=0",
		"WITH x AS (SELECT 1) CREATE TABLE t (n INT)",
	}
	for _, sql := range bad {
		if err := validateReadOnlySQL(sql); err == nil {
			t.Fatalf("want error for %q", sql)
		}
	}
}

func TestValidateReadOnlySQL_withSelectOK(t *testing.T) {
	ok := []string{
		"WITH x AS (SELECT 1 AS n) SELECT * FROM x",
		"SELECT COUNT(*) FROM metrics",
	}
	for _, sql := range ok {
		if err := validateReadOnlySQL(sql); err != nil {
			t.Fatalf("validate %q: %v", sql, err)
		}
	}
}

func TestValidateReadOnlySQL_resetRejected(t *testing.T) {
	t.Parallel()
	cases := []string{
		"RESET enable_external_access",
		"WITH x AS (SELECT 1) RESET enable_external_access",
	}
	for _, sql := range cases {
		if err := validateReadOnlySQL(sql); err == nil {
			t.Fatalf("want error for %q", sql)
		}
	}
}
