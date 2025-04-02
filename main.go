package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

func main() {
	config := getConfig()
	objs := make(chan map[string]any, 16)

	switch config.InputFormat {
	case "jsonl":
		go readJSONInput(objs, config)
	case "yaml":
		go readYAMLInput(objs, config)
	case "csv":
		go readCSVInput(objs, config)
	default:
		log.Fatalf("Unsupported input format: %s", config.InputFormat)
	}

	writer := bufio.NewWriter(os.Stdout)
	defer writer.Flush()

	if config.OutputFormat == "csv" {
		headerOrder := computeHeaderOrder(&config)
		csvWriter := csv.NewWriter(writer)
		csvWriter.Write(headerOrder)
		for obj := range objs {
			outputCSVRecord(obj, headerOrder, csvWriter, config.Buffered)
		}
	} else {
		for obj := range objs {
			outputRecord(obj, config.OutputFormat, writer, config.Buffered)
		}
	}
}

// Reads the command line flags and build a Config from the flags and an optional yaml config.
func getConfig() Config {
	version := "0.1.0"
	var configPath string
	var config Config

	flag.StringVar(&configPath, "c", "", "Path to configuration YAML file")
	flag.StringVar(&config.InputFormat, "i", "yaml", "Input format: jsonl, yaml, or csv")
	flag.StringVar(&config.OutputFormat, "o", "yaml", "Output format: jsonl, yaml, or csv")
	flag.BoolVar(&config.Buffered, "buffered", false, "Force buffered output (don't flush after each record)")
	versionCmd := flag.Bool("version", false, "Show version info")

	flag.Usage = func() {
		stderrln("Usage of trmg:")
		stderrln("  An application that takes in a jsonl or yaml input stream")
		stderrln("  and lets you customize the output objects and data type.")
		stderrln("  See the README for details:")
		stderrln("  https://github.com/zonkhead/transmogrifier\n")
		stderrln("Options:")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *versionCmd {
		stderrln("Version: " + version)
		os.Exit(0)
	}

	if !contains([]string{"jsonl", "yaml", "csv"}, config.InputFormat) {
		stderrln("Invalid input format: " + config.InputFormat)
		os.Exit(0)
	}
	if !contains([]string{"jsonl", "yaml", "csv"}, config.OutputFormat) {
		stderrln("Invalid output format: " + config.OutputFormat)
		os.Exit(0)
	}

	if configPath != "" {
		// Read and parse the confuration.
		configData, err := os.ReadFile(configPath)
		if err := yaml.Unmarshal(configData, &config); err != nil {
			log.Fatalf("Error parsing config file: %v", err)
		}
		if err != nil {
			log.Fatalf("Error reading config file: %v", err)
		}
	}
	if config.MatchRule == "" {
		config.MatchRule = "all"
	}
	return config
}

// getValueByPath traverses a record (a map) following a dot-separated path.
func getValueByPath(record map[string]any, path string) any {
	parts := strings.Split(path, ".")
	var current any = record
	for _, part := range parts {
		if m, ok := current.(map[string]any); ok {
			current = m[part]
		} else {
			return nil
		}
	}
	return current
}

func hasKeys[K comparable, V any](m map[K]V, ks ...K) bool {
	for _, k := range ks {
		if _, ok := m[k]; !ok {
			return false
		}
	}
	return true
}

func strval(om OutputMap, key string) string {
	if val, ok := om[key].(string); ok {
		return val
	}
	return ""
}

// applyMapping applies a Output to a record.
func applyMapping(name string, in, out map[string]any, outSpec any) {
	switch v := outSpec.(type) {
	case string:
		out[name] = getValueByPath(in, v)
	case OutputMap:
		if hasKeys(v, "src", "regex", "value") {
			src := strval(v, "src")
			regex := strval(v, "regex")
			re, err := regexp.Compile(regex)
			if err != nil {
				return
			}
			srcVal, ok := getValueByPath(in, src).(string)
			if !ok {
				return
			}
			matches := re.FindStringSubmatch(srcVal)
			if len(matches) == 0 {
				return
			}
			val := strval(v, "value")
			if val != "" {
				result := val
				// Replace $1, $2, … with captured groups.
				for i, match := range matches[1:] {
					placeholder := fmt.Sprintf("$%d", i+1)
					result = strings.ReplaceAll(result, placeholder, match)
				}
				out[name] = result
			}
		} else {
			newout := make(OutputMap)
			out[name] = newout
			for k := range v {
				applyMapping(k, in, newout, v[k])
			}
		}
	}
}

// applyFieldMappings applies a list of field mappings to a record.
func applyFieldMappings(record map[string]any, mappings []FieldMapping) map[string]any {
	output := make(map[string]any)
	for _, fm := range mappings {
		applyMapping(fm.Key, record, output, fm.Output)
	}
	return output
}

// processInput processes one record:
// 1. Applies the common mappings.
// 2. Iterates over specific rules (first match wins) and merges in its extra mappings.
// 3. If no specific rule matches and matchRule is "drop-no-match", returns nil.
// 4. If no specific rule matches and matchRule is "all", returns original record.
func processInput(record map[string]any, config Config) map[string]any {
	commonMappings := convertFieldMappings(config.CommonOutput)
	output := applyFieldMappings(record, commonMappings)
	matchedSpecific := false
	for _, rule := range config.SpecificOutputs {
		if rule.Check(record) {
			matchedSpecific = true
			ruleMappings := convertFieldMappings(rule.Output)
			additional := applyFieldMappings(record, ruleMappings)
			for k, v := range additional {
				output[k] = v
			}
			break
		}
	}
	if config.MatchRule == "drop-no-match" && !matchedSpecific {
		return nil
	}

	// Nothing was mapped so we output the whole thing
	if len(output) == 0 {
		return record
	}

	return output
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
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

// outputRecord writes a single record in the given format (jsonl, yaml or csv).
func outputRecord(record map[string]any, format string, writer *bufio.Writer, buffered bool) {
	var outBytes []byte
	var err error
	if format == "jsonl" {
		outBytes, err = json.Marshal(record)
		if err != nil {
			log.Printf("Error marshaling JSON: %v", err)
			return
		}
		outBytes = append(outBytes, '\n')
	} else if format == "yaml" {
		outBytes, err = yaml.Marshal(record)
		if err != nil {
			log.Printf("Error marshaling YAML: %v", err)
			return
		}
		// Prepend a document separator.
		outBytes = append([]byte("---\n"), outBytes...)
	} else {
		log.Printf("Unsupported output format: %s", format)
		return
	}
	writer.Write(outBytes)
	if !buffered {
		writer.Flush()
	}
}

// outputCSV writes all records as CSV using the given header order.
func outputCSVRecord(rec map[string]any, headers []string, writer *csv.Writer, buffered bool) {
	row := make([]string, len(headers))
	for i, h := range headers {
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
	writer.Write(row)
	if !buffered {
		writer.Flush()
	}
}

func readJSONInput(objs chan<- map[string]any, config Config) {
	defer close(objs)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			log.Printf("Error parsing JSON: %v", err)
			continue
		}
		result := processInput(record, config)
		if result != nil {
			objs <- result
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %v", err)
	}
}

func readYAMLInput(objs chan<- map[string]any, config Config) {
	defer close(objs)
	decoder := yaml.NewDecoder(os.Stdin)
	for {
		var doc any
		err := decoder.Decode(&doc)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error decoding YAML: %v", err)
			continue
		}
		// doc may be a sequence or a single mapping.
		switch v := doc.(type) {
		case []any:
			for _, item := range v {
				rec, ok := item.(map[string]any)
				if !ok {
					continue
				}
				result := processInput(rec, config)
				if result != nil {
					objs <- result
				}
			}
		case map[string]any:
			result := processInput(v, config)
			if result != nil {
				objs <- result
			}
		default:
			// Ignore other document types.
		}
	}
}

func readCSVInput(objs chan<- map[string]any, config Config) {
	defer close(objs)
	reader := csv.NewReader(os.Stdin)
	
	// Read header row
	headers, err := reader.Read()
	if err != nil {
		log.Fatalf("Error reading CSV header: %v", err)
	}

	// Read data rows
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading CSV record: %v", err)
			continue
		}

		// Convert CSV record to map
		obj := make(map[string]any, len(headers))
		for i, value := range record {
			if i < len(headers) {
				obj[headers[i]] = value
			}
		}

		processed := processInput(obj, config)
		if processed != nil {
			objs <- processed
		}
	}
}

func stderrln(s string) {
	fmt.Fprintln(os.Stderr, s)
}

func stderrf(fs string, a ...any) {
	fmt.Fprintf(os.Stderr, fs, a...)
}
