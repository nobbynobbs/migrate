package multistmt_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/golang-migrate/migrate/v4/database/multistmt"
)

func TestPGParse(t *testing.T) {
	testCases := []struct {
		name        string
		multiStmt   string
		delimiter   string
		expected    []string
		expectedErr error
	}{
		{name: "single statement, no delimiter", multiStmt: "select 1 from tbl", delimiter: ";",
			expected: []string{"select 1 from tbl"}, expectedErr: nil},
		{name: "single statement, one delimiter", multiStmt: "SELECT 1 FROM tbl;", delimiter: ";",
			expected: []string{"SELECT 1 FROM tbl"}, expectedErr: nil},
		{name: "two statements, no trailing delimiter", multiStmt: "SELECT 1 FROM tbl1; SELECT 2 FROM tbl2", delimiter: ";",
			expected: []string{"SELECT 1 FROM tbl1", "SELECT 2 FROM tbl2"}, expectedErr: nil},
		{name: "two statements, with trailing delimiter", multiStmt: "SELECT 1 FROM tbl1; SELECT 2 FROM tbl2;", delimiter: ";",
			expected: []string{"SELECT 1 FROM tbl1", "SELECT 2 FROM tbl2"}, expectedErr: nil},
		{
			name: "create function, dollar-quoted strings, comments",
			multiStmt: `CREATE FUNCTION check_password(uname TEXT, pass TEXT)
RETURNS BOOLEAN AS $$
DECLARE passed BOOLEAN;
BEGIN
        SELECT  (pwd = $2) INTO passed
        FROM    pwds
        WHERE   username = $1;

        RETURN passed;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'pg_temp'.
    SET search_path = admin, pg_temp;`, delimiter: ";",
			expected: []string{`CREATE FUNCTION check_password(uname TEXT, pass TEXT)
RETURNS BOOLEAN AS $$
DECLARE passed BOOLEAN;
BEGIN
        SELECT  (pwd = $2) INTO passed
        FROM    pwds
        WHERE   username = $1;

        RETURN passed;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'pg_temp'.
    SET search_path = admin, pg_temp;`,
			}, expectedErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmts := make([]string, 0, len(tc.expected))
			err := multistmt.PGParse(strings.NewReader(tc.multiStmt), []byte(tc.delimiter), maxMigrationSize, func(b []byte) bool {
				stmts = append(stmts, string(b))
				return true
			})
			assert.Equal(t, tc.expectedErr, err)
			assert.Equal(t, tc.expected, stmts)
		})
	}
}

func TestPGParseDiscontinue(t *testing.T) {
	multiStmt := "SELECT 1 FROM tbl1; select 2 from tbl2"
	delimiter := ";"
	expected := []string{"SELECT 1 FROM tbl1"}

	stmts := make([]string, 0, len(expected))
	err := multistmt.PGParse(strings.NewReader(multiStmt), []byte(delimiter), maxMigrationSize, func(b []byte) bool {
		stmts = append(stmts, string(b))
		return false
	})
	assert.Nil(t, err)
	assert.Equal(t, expected, stmts)
}
