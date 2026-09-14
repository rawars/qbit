package qbit

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	maxQueueNameLength  = 128
	maxIdentifierLength = 256
)

type queueKeys struct{ base string }

func newQueueKeys(name string) (queueKeys, error) {
	if err := validateIdentifier("queue name", name, maxQueueNameLength, "{}"); err != nil {
		return queueKeys{}, err
	}
	return queueKeys{base: fmt.Sprintf("qbit:{%s}", name)}, nil
}

func validateIdentifier(kind, value string, maxLength int, separators string) error {
	if value == "" {
		return fmt.Errorf("qbit: %s cannot be empty", kind)
	}
	if len(value) > maxLength {
		return fmt.Errorf("qbit: %s exceeds %d bytes", kind, maxLength)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("qbit: %s must be valid UTF-8", kind)
	}
	if strings.ContainsAny(value, separators) {
		return fmt.Errorf("qbit: %s contains a reserved separator", kind)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fmt.Errorf("qbit: %s cannot contain control characters", kind)
		}
	}
	return nil
}

func (keys queueKeys) id() string            { return keys.base + ":id" }
func (keys queueKeys) events() string        { return keys.base + ":events" }
func (keys queueKeys) ready() string         { return keys.base + ":groups:ready" }
func (keys queueKeys) readySet() string      { return keys.base + ":groups:ready:set" }
func (keys queueKeys) active() string        { return keys.base + ":groups:active" }
func (keys queueKeys) activeJobs() string    { return keys.base + ":jobs:active" }
func (keys queueKeys) marker() string        { return keys.base + ":marker" }
func (keys queueKeys) metrics() string       { return keys.base + ":metrics" }
func (keys queueKeys) paused() string        { return keys.base + ":paused" }
func (keys queueKeys) workers() string       { return keys.base + ":workers" }
func (keys queueKeys) job(id string) string  { return keys.base + ":job:" + id }
func (keys queueKeys) lock(id string) string { return keys.job(id) + ":lock" }
func (keys queueKeys) worker(id string) string {
	return keys.base + ":worker:" + id
}
func (keys queueKeys) groupWait(group string) string {
	return keys.base + ":group:" + group + ":wait"
}
