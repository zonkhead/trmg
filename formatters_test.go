package main

import (
	"bufio"
	"bytes"
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

func TestJSONFormatter_SingletonVsArray(t *testing.T) {
	testRecord1 := map[string]any{"name": "Alice", "age": 30}
	testRecord2 := map[string]any{"name": "Bob", "age": 25}

	t.Run("singleton output", func(t *testing.T) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		formatter := NewJSONFormatter(writer, true) // isSingletonInput = true

		// Write header, record, footer
		if err := formatter.WriteHeader(); err != nil {
			t.Fatalf("WriteHeader failed: %v", err)
		}
		if err := formatter.WriteRecord(testRecord1); err != nil {
			t.Fatalf("WriteRecord failed: %v", err)
		}
		if err := formatter.WriteFooter(); err != nil {
			t.Fatalf("WriteFooter failed: %v", err)
		}
		writer.Flush()

		got := buf.String()
		want := `{"age":30,"name":"Alice"}`
		if got != want {
			t.Errorf("singleton output got %q, want %q", got, want)
		}
	})

	t.Run("array output", func(t *testing.T) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		formatter := NewJSONFormatter(writer, false) // isSingletonInput = false

		// Write header, multiple records, footer
		if err := formatter.WriteHeader(); err != nil {
			t.Fatalf("WriteHeader failed: %v", err)
		}
		if err := formatter.WriteRecord(testRecord1); err != nil {
			t.Fatalf("WriteRecord 1 failed: %v", err)
		}
		if err := formatter.WriteRecord(testRecord2); err != nil {
			t.Fatalf("WriteRecord 2 failed: %v", err)
		}
		if err := formatter.WriteFooter(); err != nil {
			t.Fatalf("WriteFooter failed: %v", err)
		}
		writer.Flush()

		got := buf.String()
		want := "[\n{\"age\":30,\"name\":\"Alice\"},\n{\"age\":25,\"name\":\"Bob\"}\n]"
		if got != want {
			t.Errorf("array output got %q, want %q", got, want)
		}
	})
}

func TestJSONPFormatter_SingletonVsArray(t *testing.T) {
	testRecord1 := map[string]any{"name": "Alice", "age": 30}
	testRecord2 := map[string]any{"name": "Bob", "age": 25}

	t.Run("singleton output", func(t *testing.T) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		formatter := NewJSONPFormatter(writer, true) // isSingletonInput = true

		// Write header, record, footer
		if err := formatter.WriteHeader(); err != nil {
			t.Fatalf("WriteHeader failed: %v", err)
		}
		if err := formatter.WriteRecord(testRecord1); err != nil {
			t.Fatalf("WriteRecord failed: %v", err)
		}
		if err := formatter.WriteFooter(); err != nil {
			t.Fatalf("WriteFooter failed: %v", err)
		}
		writer.Flush()

		got := buf.String()
		want := "{\n  \"age\": 30,\n  \"name\": \"Alice\"\n}"
		if got != want {
			t.Errorf("singleton output got %q, want %q", got, want)
		}
	})

	t.Run("array output", func(t *testing.T) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		formatter := NewJSONPFormatter(writer, false) // isSingletonInput = false

		// Write header, multiple records, footer
		if err := formatter.WriteHeader(); err != nil {
			t.Fatalf("WriteHeader failed: %v", err)
		}
		if err := formatter.WriteRecord(testRecord1); err != nil {
			t.Fatalf("WriteRecord 1 failed: %v", err)
		}
		if err := formatter.WriteRecord(testRecord2); err != nil {
			t.Fatalf("WriteRecord 2 failed: %v", err)
		}
		if err := formatter.WriteFooter(); err != nil {
			t.Fatalf("WriteFooter failed: %v", err)
		}
		writer.Flush()

		got := buf.String()
		want := "[{\n  \"age\": 30,\n  \"name\": \"Alice\"\n},{\n  \"age\": 25,\n  \"name\": \"Bob\"\n}]"
		if got != want {
			t.Errorf("array output got %q, want %q", got, want)
		}
	})
}
