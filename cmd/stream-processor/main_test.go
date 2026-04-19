package main

import "testing"

func TestRedactDSN(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "password stripped, username retained",
			in:   "postgres://user:pass@host:5432/db?sslmode=disable",
			want: "postgres://user@host:5432/db?sslmode=disable",
		},
		{
			name: "no password present",
			in:   "postgres://user@host:5432/db",
			want: "postgres://user@host:5432/db",
		},
		{
			name: "no userinfo",
			in:   "postgres://host:5432/db",
			want: "postgres://host:5432/db",
		},
		{
			name: "empty input",
			in:   "",
			want: "",
		},
		{
			name: "unparseable → sentinel",
			in:   "://broken",
			want: "<unparseable>",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := redactDSN(tc.in)
			if got != tc.want {
				t.Errorf("redactDSN(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
