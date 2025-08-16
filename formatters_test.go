package main

import (
	"reflect"
	"testing"
)

func Test_computeHeaderOrder(t *testing.T) {
	t.Run("common only", func(t *testing.T) {
		cfg := &Config{
			CommonOutput: []OutputMap{{"a": "foo"}, {"b": "bar"}},
		}
		got := computeHeaderOrder(cfg)
		want := []string{"a", "b"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("specific only", func(t *testing.T) {
		cfg := &Config{
			SpecificOutputs: []SpecificOutputRule{{ 
				Output: []OutputMap{{"x": "foo"}, {"y": "bar"}},
			}},
		}
		got := computeHeaderOrder(cfg)
		want := []string{"x", "y"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("overlap", func(t *testing.T) {
		cfg := &Config{
			CommonOutput: []OutputMap{{"a": "foo"}},
			SpecificOutputs: []SpecificOutputRule{{ 
				Output: []OutputMap{{"a": "foo"}, {"b": "bar"}},
			}},
		}
		got := computeHeaderOrder(cfg)
		want := []string{"a", "b"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}
