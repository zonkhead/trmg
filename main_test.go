package main

import (
	"bytes"
	"flag"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strings"
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

	t.Run("clone original preserves unmapped fields", func(t *testing.T) {
		record := map[string]any{"foo": "yes", "bar": 1, "unmapped": true}
		cfg := mustConfig(t, `
clone-original: true
specific-outputs:
- field: foo
  eq: yes
  output:
  - mapped: bar
`)
		got := processInput(record, *cfg)
		want := map[string]any{"foo": "yes", "bar": 1, "unmapped": true, "mapped": 1}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
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

	t.Run("string literal mapping", func(t *testing.T) {
		in := map[string]any{"foo": 42}
		out := map[string]any{}
		applyMapping("bar", in, out, "YES")
		if got, want := out["bar"], "YES"; got != want {
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

func Test_lookupValueByPath(t *testing.T) {
	tests := []struct {
		name   string
		record map[string]any
		path   string
		want   any
		ok     bool
	}{
		{
			name:   "flat key exists",
			record: map[string]any{"foo": 42},
			path:   "foo",
			want:   42,
			ok:     true,
		},
		{
			name:   "missing key",
			record: map[string]any{"foo": 42},
			path:   "bar",
			want:   nil,
			ok:     false,
		},
		{
			name:   "present nil value",
			record: map[string]any{"foo": nil},
			path:   "foo",
			want:   nil,
			ok:     true,
		},
		{
			name:   "empty path",
			record: map[string]any{"foo": 42},
			path:   "",
			want:   nil,
			ok:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := lookupValueByPath(tt.record, tt.path)
			if !reflect.DeepEqual(got, tt.want) || ok != tt.ok {
				t.Errorf("lookupValueByPath() = (%v, %v), want (%v, %v)", got, ok, tt.want, tt.ok)
			}
		})
	}
}

func testReadJSONInput(t *testing.T, jsonString string, expectedCount int, expectedType InputType) []map[string]any {
	t.Helper()

	// Mock stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}
	// Handle empty input case
	if jsonString != "" {
		_, err = w.Write([]byte(jsonString))
		if err != nil {
			t.Fatalf("writing to pipe failed: %v", err)
		}
	}
	w.Close()

	origStdin := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = origStdin }()

	// Setup channels and config
	objs := make(chan map[string]any, 10)
	inputTypeChan := make(chan InputType, 1)
	config := Config{InputFormat: "json", MatchRule: "all"}

	// Run the function
	readJSONInput(objs, inputTypeChan, config)

	// Collect results
	var results []map[string]any
	for obj := range objs {
		results = append(results, obj)
	}

	// Assert count
	if len(results) != expectedCount {
		t.Errorf("got %d records, want %d", len(results), expectedCount)
	}

	// Check input type
	var gotType InputType
	select {
	case gotType = <-inputTypeChan:
		if gotType != expectedType {
			t.Errorf("got input type %v, want %v", gotType, expectedType)
		}
	default:
		// This case is valid for empty input, where the channel is closed.
		if jsonString != "" {
			t.Errorf("did not receive an input type")
		}
	}

	return results
}

func TestReadJSONInput_SingleObject(t *testing.T) {
	jsonInput := `{"id": 1, "name": "one"}`
	results := testReadJSONInput(t, jsonInput, 1, SingletonInput)

	if len(results) == 1 {
		want := map[string]any{"id": float64(1), "name": "one"}
		if !reflect.DeepEqual(results[0], want) {
			t.Errorf("got %v, want %v", results[0], want)
		}
	}
}

func TestReadJSONInput_ArrayOfObjects(t *testing.T) {
	jsonInput := `[{"id": 1, "name": "one"}, {"id": 2, "name": "two"}]`
	results := testReadJSONInput(t, jsonInput, 2, ArrayInput)

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

func testReadYAMLInput(t *testing.T, yamlString string, expectedCount int, expectedType InputType) []map[string]any {
	t.Helper()

	// Mock stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}
	if yamlString != "" {
		_, err = w.Write([]byte(yamlString))
		if err != nil {
			t.Fatalf("writing to pipe failed: %v", err)
		}
	}
	w.Close()

	origStdin := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = origStdin }()

	// Setup channels and config
	objs := make(chan map[string]any, 10)
	inputTypeChan := make(chan InputType, 1)
	config := Config{MatchRule: "all"} // A minimal config

	// Run the function in a goroutine
	go readYAMLInput(objs, inputTypeChan, config)

	// Collect all records from the objs channel until it's closed.
	var results []map[string]any
	for obj := range objs {
		results = append(results, obj)
	}

	// After objs is closed, the input type should have been sent.
	var gotType InputType
	var chanOpen bool
	select {
	case gotType, chanOpen = <-inputTypeChan:
		if !chanOpen {
			// Channel was closed (empty input). The received gotType is the zero value.
			if expectedType != SingletonInput { // The zero value for InputType is SingletonInput
				t.Errorf("expected type %v for empty input, but channel was closed (implying SingletonInput)", expectedType)
			}
		} else if gotType != expectedType {
			t.Errorf("got input type %v, want %v", gotType, expectedType)
		}
	default:
		t.Fatalf("readYAMLInput finished but did not send or close inputTypeChan")
	}

	// Assert count
	if len(results) != expectedCount {
		t.Errorf("got %d records, want %d", len(results), expectedCount)
	}

	return results
}

func TestReadYAMLInput(t *testing.T) {
	t.Run("singleton object", func(t *testing.T) {
		yamlInput := `name: Alice`
		results := testReadYAMLInput(t, yamlInput, 1, SingletonInput)
		if len(results) == 1 {
			want := map[string]any{"name": "Alice"}
			if !reflect.DeepEqual(results[0], want) {
				t.Errorf("got %v, want %v", results[0], want)
			}
		}
	})

	t.Run("array of objects", func(t *testing.T) {
		yamlInput := `
- name: Alice
- name: Bob`
		results := testReadYAMLInput(t, yamlInput, 2, ArrayInput)
		if len(results) == 2 {
			want1 := map[string]any{"name": "Alice"}
			want2 := map[string]any{"name": "Bob"}
			if !reflect.DeepEqual(results[0], want1) {
				t.Errorf("record 1 got %v, want %v", results[0], want1)
			}
			if !reflect.DeepEqual(results[1], want2) {
				t.Errorf("record 2 got %v, want %v", results[1], want2)
			}
		}
	})

	t.Run("stream of objects", func(t *testing.T) {
		yamlInput := `
name: Alice
---
name: Bob`
		results := testReadYAMLInput(t, yamlInput, 2, StreamInput)
		if len(results) == 2 {
			want1 := map[string]any{"name": "Alice"}
			want2 := map[string]any{"name": "Bob"}
			if !reflect.DeepEqual(results[0], want1) {
				t.Errorf("record 1 got %v, want %v", results[0], want1)
			}
			if !reflect.DeepEqual(results[1], want2) {
				t.Errorf("record 2 got %v, want %v", results[1], want2)
			}
		}
	})

	t.Run("empty input", func(t *testing.T) {
		// For empty input, the inputTypeChan is closed, resulting in a zero-value read (SingletonInput).
		testReadYAMLInput(t, "", 0, SingletonInput)
	})

	t.Run("single empty array", func(t *testing.T) {
		testReadYAMLInput(t, "[]", 0, ArrayInput)
	})

	t.Run("malformed document in stream", func(t *testing.T) {
		yamlInput := "name: Alice\n---\n[ invalid syntax\n---\nname: Bob"
		// Only Alice should be read. The syntax error breaks the stream parsing.
		// If it hangs here, we have an infinite loop!
		results := testReadYAMLInput(t, yamlInput, 1, StreamInput)
		if len(results) == 1 {
			want := map[string]any{"name": "Alice"}
			if !reflect.DeepEqual(results[0], want) {
				t.Errorf("got %v, want %v", results[0], want)
			}
		}
	})

	t.Run("stream containing an array", func(t *testing.T) {
		yamlInput := "name: Alice\n---\n- name: Bob\n- name: Charlie\n---\nname: Dave"
		// The array is skipped, so we should get Alice and Dave
		results := testReadYAMLInput(t, yamlInput, 2, StreamInput)
		if len(results) == 2 {
			want1 := map[string]any{"name": "Alice"}
			want2 := map[string]any{"name": "Dave"}
			if !reflect.DeepEqual(results[0], want1) {
				t.Errorf("record 1 got %v, want %v", results[0], want1)
			}
			if !reflect.DeepEqual(results[1], want2) {
				t.Errorf("record 2 got %v, want %v", results[1], want2)
			}
		}
	})
}

func testReadJSONLInput(t *testing.T, jsonlString string, expectedCount int, expectedType InputType) []map[string]any {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}
	if jsonlString != "" {
		_, err = w.Write([]byte(jsonlString))
		if err != nil {
			t.Fatalf("writing to pipe failed: %v", err)
		}
	}
	w.Close()

	origStdin := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = origStdin }()

	objs := make(chan map[string]any, 10)
	inputTypeChan := make(chan InputType, 1)
	config := Config{MatchRule: "all"}

	go readJSONLInput(objs, inputTypeChan, config)

	var results []map[string]any
	for obj := range objs {
		results = append(results, obj)
	}

	var gotType InputType
	select {
	case gotType = <-inputTypeChan:
		if gotType != expectedType {
			t.Errorf("got input type %v, want %v", gotType, expectedType)
		}
	default:
		t.Fatalf("readJSONLInput did not send input type")
	}

	if len(results) != expectedCount {
		t.Errorf("got %d records, want %d", len(results), expectedCount)
	}

	return results
}

func TestReadJSONLInput(t *testing.T) {
	jsonlInput := "{\"id\": 1, \"name\": \"one\"}\n{\"id\": 2, \"name\": \"two\"}\n"
	results := testReadJSONLInput(t, jsonlInput, 2, StreamInput)

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

func testReadCSVInput(t *testing.T, csvString string, expectedCount int, expectedType InputType) []map[string]any {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}
	if csvString != "" {
		_, err = w.Write([]byte(csvString))
		if err != nil {
			t.Fatalf("writing to pipe failed: %v", err)
		}
	}
	w.Close()

	origStdin := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = origStdin }()

	objs := make(chan map[string]any, 10)
	inputTypeChan := make(chan InputType, 1)
	config := Config{MatchRule: "all"}

	go readCSVInput(objs, inputTypeChan, config)

	var results []map[string]any
	for obj := range objs {
		results = append(results, obj)
	}

	var gotType InputType
	select {
	case gotType = <-inputTypeChan:
		if gotType != expectedType {
			t.Errorf("got input type %v, want %v", gotType, expectedType)
		}
	default:
		// For empty input, it might close without sending type or we handle it if tests demand
		// let's just make it soft fail or only check if count > 0
		if expectedCount > 0 {
			t.Fatalf("readCSVInput did not send input type")
		}
	}

	if len(results) != expectedCount {
		t.Errorf("got %d records, want %d", len(results), expectedCount)
	}

	return results
}

func TestReadCSVInput(t *testing.T) {
	csvInput := "id,name\n1,one\n2,two\n"
	results := testReadCSVInput(t, csvInput, 2, ArrayInput)

	if len(results) == 2 {
		want1 := map[string]any{"id": "1", "name": "one"}
		want2 := map[string]any{"id": "2", "name": "two"}
		if !reflect.DeepEqual(results[0], want1) {
			t.Errorf("record 1 got %v, want %v", results[0], want1)
		}
		if !reflect.DeepEqual(results[1], want2) {
			t.Errorf("record 2 got %v, want %v", results[1], want2)
		}
	}
}

func Test_getConfig(t *testing.T) {
	// Backup original args and command line
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	origCommandLine := flag.CommandLine
	defer func() { flag.CommandLine = origCommandLine }()

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	os.Args = []string{os.Args[0], "-i", "json", "-o", "jsonl", "-buffered"}

	config := getConfig()

	if config.InputFormat != "json" {
		t.Errorf("expected InputFormat to be json, got %s", config.InputFormat)
	}
	if config.OutputFormat != "jsonl" {
		t.Errorf("expected OutputFormat to be jsonl, got %s", config.OutputFormat)
	}
	if !config.Buffered {
		t.Errorf("expected Buffered to be true, got %t", config.Buffered)
	}

	// Call flag.Usage to cover it
	if flag.CommandLine.Usage != nil {
		origStderr := os.Stderr
		os.Stderr = os.NewFile(0, os.DevNull) // Suppress output
		flag.CommandLine.Usage()
		os.Stderr = origStderr
	}
}

func Test_getConfig_version(t *testing.T) {
	if os.Getenv("BE_CRASH_TEST_VERSION") == "1" {
		origArgs := os.Args
		defer func() { os.Args = origArgs }()
		origCommandLine := flag.CommandLine
		defer func() { flag.CommandLine = origCommandLine }()
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

		os.Args = []string{os.Args[0], "-version"}
		getConfig()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=Test_getConfig_version")
	cmd.Env = append(os.Environ(), "BE_CRASH_TEST_VERSION=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		// Expect exit 0, but exec.Command on Exit(0) returns nil err.
	}
	if !strings.Contains(stderr.String(), "Version: 0.1.9") {
		t.Errorf("expected output to contain version info, got %q", stderr.String())
	}
}

func Test_getConfig_invalid_input(t *testing.T) {
	if os.Getenv("BE_CRASH_TEST_INVALID_INPUT") == "1" {
		origArgs := os.Args
		defer func() { os.Args = origArgs }()
		origCommandLine := flag.CommandLine
		defer func() { flag.CommandLine = origCommandLine }()
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

		os.Args = []string{os.Args[0], "-i", "invalid"}
		getConfig()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=Test_getConfig_invalid_input")
	cmd.Env = append(os.Environ(), "BE_CRASH_TEST_INVALID_INPUT=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Run()
	if !strings.Contains(stderr.String(), "Invalid input format: invalid") {
		t.Errorf("expected stderr to contain invalid input message, got %q", stderr.String())
	}
}

func Test_getConfig_invalid_output(t *testing.T) {
	if os.Getenv("BE_CRASH_TEST_INVALID_OUTPUT") == "1" {
		origArgs := os.Args
		defer func() { os.Args = origArgs }()
		origCommandLine := flag.CommandLine
		defer func() { flag.CommandLine = origCommandLine }()
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

		os.Args = []string{os.Args[0], "-o", "invalid"}
		getConfig()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=Test_getConfig_invalid_output")
	cmd.Env = append(os.Environ(), "BE_CRASH_TEST_INVALID_OUTPUT=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Run()
	if !strings.Contains(stderr.String(), "Invalid output format: invalid") {
		t.Errorf("expected stderr to contain invalid output message, got %q", stderr.String())
	}
}

func Test_getConfig_with_file(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "trmg-test-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	configContent := `
match-rule: drop-no-match
clone-original: true
`
	if _, err := tmpFile.Write([]byte(configContent)); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	origCommandLine := flag.CommandLine
	defer func() { flag.CommandLine = origCommandLine }()
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	os.Args = []string{os.Args[0], "-c", tmpFile.Name()}
	config := getConfig()

	if config.MatchRule != "drop-no-match" {
		t.Errorf("expected MatchRule to be drop-no-match, got %s", config.MatchRule)
	}
	if !config.CloneOriginal {
		t.Errorf("expected CloneOriginal to be true")
	}
}

func Test_getConfig_missing_file(t *testing.T) {
	if os.Getenv("BE_CRASH_TEST_MISSING_FILE") == "1" {
		origArgs := os.Args
		defer func() { os.Args = origArgs }()
		origCommandLine := flag.CommandLine
		defer func() { flag.CommandLine = origCommandLine }()
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

		os.Args = []string{os.Args[0], "-c", "nonexistent-file.yaml"}
		getConfig()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=Test_getConfig_missing_file")
	cmd.Env = append(os.Environ(), "BE_CRASH_TEST_MISSING_FILE=1")
	err := cmd.Run()
	if err == nil {
		t.Errorf("expected process to fail for missing file, but it exited successfully")
	}
}

func Test_getConfig_invalid_yaml(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "trmg-test-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte("[1, 2, 3]")); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	if os.Getenv("BE_CRASH_TEST_INVALID_YAML") == "1" {
		origArgs := os.Args
		defer func() { os.Args = origArgs }()
		origCommandLine := flag.CommandLine
		defer func() { flag.CommandLine = origCommandLine }()
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

		os.Args = []string{os.Args[0], "-c", os.Getenv("TMPFILE_PATH")}
		getConfig()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=Test_getConfig_invalid_yaml")
	cmd.Env = append(os.Environ(), "BE_CRASH_TEST_INVALID_YAML=1", "TMPFILE_PATH="+tmpFile.Name())
	err = cmd.Run()
	if err == nil {
		t.Errorf("expected process to fail for invalid yaml, but it exited successfully")
	}
}

func Test_main(t *testing.T) {
	origStdin := os.Stdin
	origStdout := os.Stdout
	origArgs := os.Args
	origCommandLine := flag.CommandLine

	defer func() {
		os.Stdin = origStdin
		os.Stdout = origStdout
		os.Args = origArgs
		flag.CommandLine = origCommandLine
	}()

	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}

	os.Stdin = inR
	os.Stdout = outW

	inputData := `{"name": "Alice"}` + "\n" + `{"name": "Bob"}` + "\n"
	go func() {
		inW.Write([]byte(inputData))
		inW.Close()
	}()

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	os.Args = []string{os.Args[0], "-i", "jsonl", "-o", "jsonl"}

	main()
	outW.Close()

	var buf bytes.Buffer
	io.Copy(&buf, outR)
	outR.Close()

	got := buf.String()
	want := `{"name":"Alice"}` + "\n" + `{"name":"Bob"}` + "\n"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func Test_stderrln(t *testing.T) {
	origStderr := os.Stderr
	defer func() { os.Stderr = origStderr }()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}
	os.Stderr = w

	stderrln("test error")
	w.Close()

	var buf bytes.Buffer
	io.Copy(&buf, r)
	r.Close()

	got := buf.String()
	want := "test error\n"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func Test_strval(t *testing.T) {
	om := OutputMap{
		"valid":   "hello",
		"invalid": 123,
	}
	if got := strval(om, "valid"); got != "hello" {
		t.Errorf("got %q, want %q", got, "hello")
	}
	if got := strval(om, "invalid"); got != "" {
		t.Errorf("got %q, want %q", got, "")
	}
	if got := strval(om, "missing"); got != "" {
		t.Errorf("got %q, want %q", got, "")
	}
}

func Test_applyMapping_edgecases(t *testing.T) {
	t.Run("invalid regex", func(t *testing.T) {
		in := map[string]any{"text": "hello"}
		out := map[string]any{}
		outSpec := OutputMap{
			"src":   "text",
			"regex": "[invalid regex",
			"value": "foo",
		}
		applyMapping("result", in, out, outSpec)
		if _, exists := out["result"]; exists {
			t.Errorf("expected no mapping created for invalid regex")
		}
	})

	t.Run("src value not string", func(t *testing.T) {
		in := map[string]any{"text": 123} // integer instead of string
		out := map[string]any{}
		outSpec := OutputMap{
			"src":   "text",
			"regex": "hello",
			"value": "foo",
		}
		applyMapping("result", in, out, outSpec)
		if _, exists := out["result"]; exists {
			t.Errorf("expected no mapping created when src value is not a string")
		}
	})

	t.Run("regex no match", func(t *testing.T) {
		in := map[string]any{"text": "hello"}
		out := map[string]any{}
		outSpec := OutputMap{
			"src":   "text",
			"regex": "world",
			"value": "foo",
		}
		applyMapping("result", in, out, outSpec)
		if _, exists := out["result"]; exists {
			t.Errorf("expected no mapping created when regex does not match")
		}
	})

	t.Run("empty value mapping", func(t *testing.T) {
		in := map[string]any{"text": "hello"}
		out := map[string]any{}
		outSpec := OutputMap{
			"src":   "text",
			"regex": "hello",
			"value": "",
		}
		applyMapping("result", in, out, outSpec)
		if _, exists := out["result"]; exists {
			t.Errorf("expected no mapping created when value template is empty")
		}
	})
}


