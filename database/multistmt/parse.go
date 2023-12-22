// Package multistmt provides methods for parsing multi-statement database migrations
package multistmt

import (
	"bufio"
	"bytes"
	"io"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"
)

// StartBufSize is the default starting size of the buffer used to scan and parse multi-statement migrations
var StartBufSize = 4096

// Handler handles a single migration parsed from a multi-statement migration.
// It's given the single migration to handle and returns whether or not further statements
// from the multi-statement migration should be parsed and handled.
type Handler func(migration []byte) bool

func splitWithDelimiter(delimiter []byte) func(d []byte, atEOF bool) (int, []byte, error) {
	return func(d []byte, atEOF bool) (int, []byte, error) {
		// SplitFunc inspired by bufio.ScanLines() implementation
		if atEOF {
			if len(d) == 0 {
				return 0, nil, nil
			}
			return len(d), d, nil
		}
		if i := bytes.Index(d, delimiter); i >= 0 {
			return i + len(delimiter), d[:i+len(delimiter)], nil
		}
		return 0, nil, nil
	}
}

// Parse parses the given multi-statement migration
func Parse(reader io.Reader, delimiter []byte, maxMigrationSize int, h Handler) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, StartBufSize), maxMigrationSize)
	scanner.Split(splitWithDelimiter(delimiter))
	for scanner.Scan() {
		cont := h(scanner.Bytes())
		if !cont {
			break
		}
	}
	return scanner.Err()
}

// PGParse parses the given multi-statement migration
func PGParse(reader io.Reader, _ []byte, _ int, h Handler) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	stmts, err := parser.Parse(string(data))
	if err != nil {
		return err
	}

	for _, stmt := range stmts {
		if !h([]byte(stmt.SQL)) {
			break
		}
	}

	return nil
}
