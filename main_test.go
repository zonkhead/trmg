package main

import (
	"os"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

func mustConfig(t *testing.T, yamlString string) *Config {
	t.Helper()
	var cfg Config
	if err := yaml.Unmarshal([]byte(yamlString), &cfg); err != nil {
		t.Fatalf("Failed to unmarshal YAML: %v", err)
	}
	return &cfg
}

func Test_processInput(t *testing.T) {
	t.Run("only common mappings", func(t *testing.T) {
		record := map[string]any{"foo": "bar"}
		cfg := mustConfig(t, `
match-rule: all
common-output:
- baz: foo
`)
		got := processInput(record, *cfg)
		want := map[string]any{"baz": "bar"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("specific rule matches", func(t *testing.T) {
		record := map[string]any{"foo": "yes", "val": 123}
		cfg := mustConfig(t, `
match-rule: all
common-output:
- baz: foo
specific-outputs:
- field: foo
  eq: yes
  output:
  - extra: val
`)
		got := processInput(record, *cfg)
		want := map[string]any{"baz": "yes", "extra": 123}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("drop-no-match", func(t *testing.T) {
		record := map[string]any{"foo": "no"}
		cfg := mustConfig(t, `
match-rule: drop-no-match
common-output:
- baz: foo
specific-outputs:
- field: foo
  eq: yes
  output:
  - extra: val
`)
		got := processInput(record, *cfg)
		if got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})

	t.Run("all/no match returns record", func(t *testing.T) {
		record := map[string]any{"foo": "no", "bar": 1}
		cfg := mustConfig(t, `
common-output: []
specific-outputs:
- field: foo
  eq: yes
  output:
  - extra: val
match-rule: all
`)
		got := processInput(record, *cfg)
		if !reflect.DeepEqual(got, record) {
			t.Errorf("got %v, want %v", got, record)
		}
	})
}

func Test_applyMapping(t *testing.T) {
	t.Run("string path mapping", func(t *testing.T) {
		in := map[string]any{"foo": 42}
		out := map[string]any{}
		applyMapping("bar", in, out, "foo")
		if got, want := out["bar"], 42; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("regex value mapping", func(t *testing.T) {
		in := map[string]any{"text": "hello-123"}
		out := map[string]any{}
		outSpec := OutputMap{
			"src":   "text",
			"regex": "hello-(\\d+)",
			"value": "number=$1",
		}
		applyMapping("result", in, out, outSpec)
		if got, want := out["result"], "number=123"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("nested OutputMap mapping", func(t *testing.T) {
		in := map[string]any{"a": 1, "b": 2}
		out := map[string]any{}
		outSpec := OutputMap{
			"x": "a",
			"y": "b",
		}
		applyMapping("nested", in, out, outSpec)
		nested, ok := out["nested"].(OutputMap)
		if !ok {
			t.Fatalf("expected OutputMap, got %T", out["nested"])
		}
		if got, want := nested["x"], 1; got != want {
			t.Errorf("nested[\"x\"] = %v, want %v", got, want)
		}
		if got, want := nested["y"], 2; got != want {
			t.Errorf("nested[\"y\"] = %v, want %v", got, want)
		}
	})
}

func Test_getValueByPath(t *testing.T) {
	tests := []struct {
		name   string
		record map[string]any
		path   string
		want   any
	}{
		{
			name:   "flat key",
			record: map[string]any{"foo": 42},
			path:   "foo",
			want:   42,
		},
		{
			name:   "nested value",
			record: map[string]any{"foo": map[string]any{"bar": "baz"}},
			path:   "foo.bar",
			want:   "baz",
		},
		{
			name:   "nested object",
			record: map[string]any{"foo": map[string]any{"bar": "baz"}},
			path:   "foo",
			want:   map[string]any{"bar": "baz"},
		},
		{
			name:   "non-existent key",
			record: map[string]any{"foo": 42},
			path:   "bar",
			want:   nil,
		},
		{
			name:   "path through non-map",
			record: map[string]any{"foo": 42},
			path:   "foo.bar",
			want:   nil,
		},
		{
			name:   "empty path",
			record: map[string]any{"foo": 42},
			path:   "",
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getValueByPath(tt.record, tt.path)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getValueByPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func testReadJSONInput(t *testing.T, jsonString string, expectedCount int) []map[string]any {
	t.Helper()

	// Mock stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}
	_, err = w.Write([]byte(jsonString))
	if err != nil {
		t.Fatalf("writing to pipe failed: %v", err)
	}
	w.Close()

	origStdin := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = origStdin }()

	// Setup channel and config
	objs := make(chan map[string]any, 10)
	config := Config{InputFormat: "json", MatchRule: "all"}

	// Run the function
	readJSONInput(objs, config)

	// Collect results
	var results []map[string]any
	for obj := range objs {
		results = append(results, obj)
	}

	// Assert count
	if len(results) != expectedCount {
		t.Errorf("got %d records, want %d", len(results), expectedCount)
	}

	return results
}

func TestReadJSONInput_SingleObject(t *testing.T) {
	jsonInput := `{"id": 1, "name": "one"}`
	results := testReadJSONInput(t, jsonInput, 1)

	if len(results) == 1 {
		want := map[string]any{"id": float64(1), "name": "one"}
		if !reflect.DeepEqual(results[0], want) {
			t.Errorf("got %v, want %v", results[0], want)
		}
	}
}

func TestReadJSONInput_ArrayOfObjects(t *testing.T) {
	jsonInput := `[{"id": 1, "name": "one"}, {"id": 2, "name": "two"}]`
	results := testReadJSONInput(t, jsonInput, 2)

	if len(results) == 2 {
		want1 := map[string]any{"id": float64(1), "name": "one"}
		want2 := map[string]any{"id": float64(2), "name": "two"}
		if !reflect.DeepEqual(results[0], want1) {
			t.Errorf("record 1 got %v, want %v", results[0], want1)
		}
		if !reflect.DeepEqual(results[1], want2) {
			t.Errorf("record 2 got %v, want %v", results[1], want2)
		}
	}
}