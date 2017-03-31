package store

import "testing"

func TestNew(t *testing.T) {
	store := New()
	println("new store", store)
}
