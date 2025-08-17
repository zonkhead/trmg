package main

import (
	"reflect"
	"testing"
)

func Test_computeHeaderOrder(t *testing.T) {
	t.Run("common only", func(t *testing.T) {
		cfg := mustConfig(t, `
common-output:
- a: foo
- b: bar
`)
		got := computeHeaderOrder(cfg)
		want := []string{"a", "b"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("specific only", func(t *testing.T) {
		cfg := mustConfig(t, `
specific-outputs:
- output:
  - x: foo
  - y: bar
`)
		got := computeHeaderOrder(cfg)
		want := []string{"x", "y"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("overlap", func(t *testing.T) {
		cfg := mustConfig(t, `
common-output:
- a: foo
specific-outputs:
- output:
  - a: foo
  - b: bar
`)
		got := computeHeaderOrder(cfg)
		want := []string{"a", "b"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}
