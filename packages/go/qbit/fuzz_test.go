package qbit

import (
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

func FuzzIdentifiers(f *testing.F) {
	f.Add([]byte("emails"), uint8(0))
	f.Add([]byte("tenant-42"), uint8(1))
	f.Add([]byte("request-123"), uint8(2))
	f.Add([]byte("bad{name"), uint8(0))
	f.Add([]byte{0xff, 0xfe}, uint8(1))

	f.Fuzz(func(t *testing.T, raw []byte, identifierType uint8) {
		value := string(raw)
		maxLength := maxIdentifierLength
		separators := ":{}"
		kind := "group"
		if identifierType%3 == 0 {
			maxLength = maxQueueNameLength
			separators = "{}"
			kind = "queue name"
		} else if identifierType%3 == 2 {
			kind = "job ID"
		}

		err := validateIdentifier(kind, value, maxLength, separators)
		if err != nil {
			return
		}
		if value == "" || len(value) > maxLength || !utf8.ValidString(value) {
			t.Fatalf("accepted invalid %s %q", kind, value)
		}
		if strings.ContainsAny(value, separators) {
			t.Fatalf("accepted reserved separator in %s %q", kind, value)
		}
		for _, character := range value {
			if unicode.IsControl(character) {
				t.Fatalf("accepted control character in %s %q", kind, value)
			}
		}

		if kind == "queue name" {
			keys, keyErr := newQueueKeys(value)
			if keyErr != nil {
				t.Fatalf("validated queue name rejected by key builder: %v", keyErr)
			}
			tag := "{" + value + "}"
			for _, key := range []string{keys.ready(), keys.active(), keys.activeJobs(), keys.events(), keys.marker()} {
				if !strings.Contains(key, tag) {
					t.Fatalf("key %q lost cluster tag %q", key, tag)
				}
			}
		}
	})
}
