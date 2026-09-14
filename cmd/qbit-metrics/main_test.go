package main

import (
	"slices"
	"testing"
)

func TestParseQueueFilter(t *testing.T) {
	got := parseQueueFilter(" whatsapp, emails ", "whatsapp", "")
	want := []string{"emails", "whatsapp"}
	if !slices.Equal(got, want) {
		t.Fatalf("parseQueueFilter = %v, want %v", got, want)
	}
}

func TestSplitCSVPreservesAddressOrder(t *testing.T) {
	got := splitCSV(" redis-1:6379, ,redis-2:6379 ")
	want := []string{"redis-1:6379", "redis-2:6379"}
	if !slices.Equal(got, want) {
		t.Fatalf("splitCSV = %v, want %v", got, want)
	}
}
