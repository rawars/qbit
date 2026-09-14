package qbit

import "testing"

func TestQueueKeysUseOneClusterSlot(t *testing.T) {
	keys, err := newQueueKeys("emails")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := keys.ready(), "qbit:{emails}:groups:ready"; got != want {
		t.Fatalf("ready key = %q, want %q", got, want)
	}
	if got, want := keys.groupWait("tenant-42"), "qbit:{emails}:group:tenant-42:wait"; got != want {
		t.Fatalf("group key = %q, want %q", got, want)
	}
	if got, want := keys.metricsBucket(12345), "qbit:{emails}:metrics:bucket:12345"; got != want {
		t.Fatalf("metrics bucket key = %q, want %q", got, want)
	}
}

func TestQueueNameValidation(t *testing.T) {
	for _, name := range []string{"", "bad{name", "bad}name"} {
		if _, err := newQueueKeys(name); err == nil {
			t.Fatalf("newQueueKeys(%q) unexpectedly succeeded", name)
		}
	}
}
