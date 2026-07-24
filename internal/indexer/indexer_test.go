package indexer

import "testing"

func TestAckErrMessage(t *testing.T) {
	cases := []struct {
		ackErr string
		want   string
	}{
		{"", "ack error"},
		{"insufficient funds", "ack error: insufficient funds"},
	}
	for _, tc := range cases {
		if got := ackErrMessage(tc.ackErr); got != tc.want {
			t.Errorf("ackErrMessage(%q) = %q, want %q", tc.ackErr, got, tc.want)
		}
	}
}
