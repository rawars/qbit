package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestVersionDoesNotConnectToRedis(t *testing.T) {
	var output bytes.Buffer
	if err := run(context.Background(), []string{"--version"}, &output, &bytes.Buffer{}); err != nil {
		t.Fatalf("run --version: %v", err)
	}
	if !strings.HasPrefix(output.String(), "qbitctl version=dev") {
		t.Fatalf("unexpected version output %q", output.String())
	}
}

func TestHelpDoesNotConnectToRedis(t *testing.T) {
	var output bytes.Buffer
	if err := run(context.Background(), []string{"help"}, &output, &bytes.Buffer{}); err != nil {
		t.Fatalf("run help: %v", err)
	}
	if !strings.Contains(output.String(), "pause QUEUE") {
		t.Fatalf("help does not document pause: %q", output.String())
	}
}

func TestSplitNonEmpty(t *testing.T) {
	values := splitNonEmpty(" redis-0:6379, ,redis-1:6379 ")
	if len(values) != 2 || values[0] != "redis-0:6379" || values[1] != "redis-1:6379" {
		t.Fatalf("unexpected values %#v", values)
	}
}
