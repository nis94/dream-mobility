package chsink

import (
	"strings"
	"testing"
)

func TestSplitStatements_StripsCommentsAndBlanks(t *testing.T) {
	in := `-- leading comment
-- another comment

CREATE DATABASE flight;

-- inline comment line
CREATE TABLE foo (id String) ENGINE = MergeTree() ORDER BY id;

;
CREATE MATERIALIZED VIEW mv TO foo AS SELECT id FROM bar;
`
	stmts := splitStatements(in)
	if len(stmts) != 3 {
		t.Fatalf("got %d statements, want 3: %v", len(stmts), stmts)
	}
	for i, s := range stmts {
		if strings.HasPrefix(s, "--") {
			t.Errorf("stmt %d still has leading comment: %q", i, s)
		}
		if s == "" {
			t.Errorf("stmt %d is empty", i)
		}
	}
	if !strings.Contains(stmts[0], "CREATE DATABASE") {
		t.Errorf("stmt 0 = %q, want CREATE DATABASE", stmts[0])
	}
	if !strings.Contains(stmts[1], "CREATE TABLE") {
		t.Errorf("stmt 1 = %q, want CREATE TABLE", stmts[1])
	}
	if !strings.Contains(stmts[2], "CREATE MATERIALIZED VIEW") {
		t.Errorf("stmt 2 = %q, want CREATE MATERIALIZED VIEW", stmts[2])
	}
}

func TestSplitStatements_EmptyInput(t *testing.T) {
	if got := splitStatements(""); len(got) != 0 {
		t.Errorf("empty input produced %d statements", len(got))
	}
	if got := splitStatements("   \n -- only a comment \n"); len(got) != 0 {
		t.Errorf("comments-only input produced %d statements: %v", len(got), got)
	}
}

func TestTruncate(t *testing.T) {
	cases := []struct {
		in   string
		n    int
		want string
	}{
		{"short", 10, "short"},
		{"exactly_ten", 11, "exactly_ten"},
		{"this is a long string", 10, "this is a …"},
	}
	for _, tc := range cases {
		if got := truncate(tc.in, tc.n); got != tc.want {
			t.Errorf("truncate(%q, %d) = %q, want %q", tc.in, tc.n, got, tc.want)
		}
	}
}
