package app

import (
	"reflect"
	"testing"
)

func TestParseCSVList(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{
			name: "empty",
			in:   "",
			want: nil,
		},
		{
			name: "trim_and_split",
			in:   " 127.0.0.1:2019 , https://ops.example.com/admin ",
			want: []string{"127.0.0.1:2019", "https://ops.example.com/admin"},
		},
		{
			name: "drop_empty_entries",
			in:   ",,127.0.0.1:2019,,",
			want: []string{"127.0.0.1:2019"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseCSVList(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("parseCSVList(%q) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}
