package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"slices"

	"gopkg.in/yaml.v3"
)

// RecordFormatter is an interface for formatting and writing records.
type RecordFormatter interface {
	WriteHeader() error
	WriteRecord(record map[string]any) error
	WriteFooter() error
}

// NewFormatter creates a new RecordFormatter based on the provided config.
func NewFormatter(config *Config, writer *bufio.Writer, isSingletonInput bool) (RecordFormatter, error) {
	switch config.OutputFormat {
	case "json":
		return NewJSONFormatter(writer, isSingletonInput), nil
	case "jsonl":
		return NewJSONLFormatter(writer), nil
	case "jsonp":
		return NewJSONPFormatter(writer, isSingletonInput), nil
	case "yaml":
		return NewYAMLFormatter(writer), nil
	case "csv":
		return NewCSVFormatter(writer, config), nil
	default:
		return nil, fmt.Errorf("unsupported output format: %s", config.OutputFormat)
	}
}

// ========
// JSONFormatter formats records as a single JSON array.
type JSONFormatter struct {
	isFirst          bool
	writer           *bufio.Writer
	isSingletonInput bool
}

func NewJSONFormatter(writer *bufio.Writer, isSingletonInput bool) *JSONFormatter {
	return &JSONFormatter{writer: writer, isFirst: true, isSingletonInput: isSingletonInput}
}

func (f *JSONFormatter) WriteHeader() error {
	if f.isSingletonInput {
		return nil // No header for singleton output
	}
	_, err := f.writer.WriteString("[\n")
	return err
}

func (f *JSONFormatter) WriteRecord(record map[string]any) error {
	if !f.isSingletonInput && !f.isFirst {
		_, err := f.writer.WriteString(",\n")
		if err != nil {
			return err
		}
	}
	f.isFirst = false

	outBytes, err := json.Marshal(record)
	if err != nil {
		log.Printf("Error marshaling JSON: %v", err)
		return err
	}
	_, err = f.writer.Write(outBytes)
	return err
}

func (f *JSONFormatter) WriteFooter() error {
	if f.isSingletonInput {
		return nil // No footer for singleton output
	}
	_, err := f.writer.WriteString("\n]")
	return err
}

// ========
// JSONLFormatter formats records as newline-delimited JSON objects.
type JSONLFormatter struct {
	writer *bufio.Writer
}

func NewJSONLFormatter(writer *bufio.Writer) *JSONLFormatter {
	return &JSONLFormatter{writer: writer}
}

func (f *JSONLFormatter) WriteHeader() error {
	return nil // No header for JSONL
}

func (f *JSONLFormatter) WriteRecord(record map[string]any) error {
	outBytes, err := json.Marshal(record)
	if err != nil {
		log.Printf("Error marshaling JSON: %v", err)
		return err
	}
	outBytes = append(outBytes, '\n')
	_, err = f.writer.Write(outBytes)
	return err
}

func (f *JSONLFormatter) WriteFooter() error {
	return nil // No footer for JSONL
}

// ========
// JSONPFormatter formats records as a pretty-printed JSON array.
type JSONPFormatter struct {
	isFirst          bool
	writer           *bufio.Writer
	isSingletonInput bool
}

func NewJSONPFormatter(writer *bufio.Writer, isSingletonInput bool) *JSONPFormatter {
	return &JSONPFormatter{writer: writer, isFirst: true, isSingletonInput: isSingletonInput}
}

func (f *JSONPFormatter) WriteHeader() error {
	if f.isSingletonInput {
		return nil // No header for singleton output
	}
	_, err := f.writer.WriteString("[")
	return err
}

func (f *JSONPFormatter) WriteRecord(record map[string]any) error {
	if !f.isSingletonInput && !f.isFirst {
		_, err := f.writer.WriteString(",")
		if err != nil {
			return err
		}
	}
	f.isFirst = false

	outBytes, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		log.Printf("Error marshaling JSON: %v", err)
		return err
	}
	_, err = f.writer.Write(outBytes)
	return err
}

func (f *JSONPFormatter) WriteFooter() error {
	if f.isSingletonInput {
		return nil // No footer for singleton output
	}
	_, err := f.writer.WriteString("]")
	return err
}

// ========
// YAMLFormatter formats records as a YAML stream.
type YAMLFormatter struct {
	writer *bufio.Writer
}

func NewYAMLFormatter(writer *bufio.Writer) *YAMLFormatter {
	return &YAMLFormatter{writer: writer}
}

func (f *YAMLFormatter) WriteHeader() error {
	return nil // No header for YAML
}

func (f *YAMLFormatter) WriteRecord(record map[string]any) error {
	outBytes, err := yaml.Marshal(record)
	if err != nil {
		log.Printf("Error marshaling YAML: %v", err)
		return err
	}
	// Prepend a document separator.
	outBytes = append([]byte("---\n"), outBytes...)
	_, err = f.writer.Write(outBytes)
	return err
}

func (f *YAMLFormatter) WriteFooter() error {
	return nil // No footer for YAML
}

// ========
// CSVFormatter formats records as CSV.
type CSVFormatter struct {
	csvWriter   *csv.Writer
	headerOrder []string
}

func NewCSVFormatter(writer *bufio.Writer, config *Config) *CSVFormatter {
	return &CSVFormatter{
		csvWriter:   csv.NewWriter(writer),
		headerOrder: computeHeaderOrder(config),
	}
}

func (f *CSVFormatter) WriteHeader() error {
	return f.csvWriter.Write(f.headerOrder)
}

func (f *CSVFormatter) WriteRecord(rec map[string]any) error {
	row := make([]string, len(f.headerOrder))
	for i, h := range f.headerOrder {
		if val, ok := rec[h]; ok {
			// If the value is a string, use it directly.
			if s, ok := val.(string); ok {
				row[i] = s
			} else {
				b, err := json.Marshal(val)
				if err != nil {
					row[i] = ""
				} else {
					row[i] = string(b)
				}
			}
		} else {
			row[i] = ""
		}
	}
	return f.csvWriter.Write(row)
}

func (f *CSVFormatter) WriteFooter() error {
	f.csvWriter.Flush()
	return f.csvWriter.Error()
}

// computeHeaderOrder computes the CSV header order based on the configuration.
func computeHeaderOrder(config *Config) []string {
	var headers []string
	// Add keys from common-output (in order).
	for _, m := range config.CommonOutput {
		for k := range m {
			if !contains(headers, k) {
				headers = append(headers, k)
			}
		}
	}
	// Then add keys from specific-outputs (in order).
	for _, rule := range config.SpecificOutputs {
		for _, m := range rule.Output {
			for k := range m {
				if !contains(headers, k) {
					headers = append(headers, k)
				}
			}
		}
	}
	return headers
}

func contains(slice []string, s string) bool {
	return slices.Contains(slice, s)
}
