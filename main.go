package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"os"
	"reflect"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// InputType represents the structure of the input data.
type InputType int

const (
	SingletonInput InputType = iota
	ArrayInput
	StreamInput
)

func main() {
	config := getConfig()
	objs := make(chan map[string]any, 16)
	inputTypeChan := make(chan InputType, 1) // New channel for input type

	switch config.InputFormat {
	case "json", "jsonl":
		// readJSONInput will determine the type and send it to inputTypeChan
		go readJSONInput(objs, inputTypeChan, config)
	case "yaml":
		go readYAMLInput(objs, inputTypeChan, config)
	case "csv":
		go readCSVInput(objs, inputTypeChan, config)
	default:
		log.Fatalf("Unsupported input format: %s", config.InputFormat)
	}

	writer := bufio.NewWriter(os.Stdout)
	defer writer.Flush()

	// Wait for the input type from the channel.
	// If the channel is closed (e.g., empty input), it receives the zero value, which is SingletonInput.
	inputType := <-inputTypeChan

	formatter, err := NewFormatter(&config, writer, inputType) // Pass InputType to formatter
	if err != nil {
		log.Fatalf("Error creating formatter: %v", err)
	}

	if err := formatter.WriteHeader(); err != nil {
		log.Fatalf("Error writing header: %v", err)
	}

	for obj := range objs {
		if err := formatter.WriteRecord(obj); err != nil {
			log.Printf("Error writing record: %v", err)
		}
	}

	if err := formatter.WriteFooter(); err != nil {
		log.Fatalf("Error writing footer: %v", err)
	}
}

// Reads the command line flags and build a Config from the flags and an optional yaml config.
func getConfig() Config {
	version := "0.1.4"
	var configPath string
	var config Config

	flag.StringVar(&configPath, "c", "", "Path to configuration YAML file")
	flag.StringVar(&config.InputFormat, "i", "yaml", "Input format: json, jsonl, yaml, or csv")
	flag.StringVar(&config.OutputFormat, "o", "yaml", "Output format: json, jsonl, jsonp (pretty), yaml, or csv")
	flag.BoolVar(&config.Buffered, "buffered", false, "Force buffered output (don't flush after each record)")
	versionCmd := flag.Bool("version", false, "Show version info")

	flag.Usage = func() {
		stderrln("Usage of trmg:")
		stderrln("  An application that takes in a jsonl, yaml, or csv input stream")
		stderrln("  and lets you customize the output objects and data type.")
		stderrln("  See the README for details:")
		stderrln("  https://github.com/zonkhead/trmg\n")
		stderrln("Options:")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *versionCmd {
		stderrln("Version: " + version)
		os.Exit(0)
	}

	if !contains([]string{"json", "jsonl", "yaml", "csv"}, config.InputFormat) {
		stderrln("Invalid input format: " + config.InputFormat)
		os.Exit(0)
	}
	if !contains([]string{"json", "jsonl", "jsonp", "yaml", "csv"}, config.OutputFormat) {
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
			maps.Copy(output, additional)
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

func readJSONInput(objs chan<- map[string]any, inputTypeChan chan<- InputType, config Config) {
	defer close(objs)
	defer close(inputTypeChan)

	if config.InputFormat == "json" {
		input, err := io.ReadAll(os.Stdin)
		if err != nil {
			log.Fatalf("Error reading input: %v", err)
		}
		if len(input) == 0 {
			return
		}

		// Try to unmarshal into an array of objects first.
		var records []map[string]any
		errArray := json.Unmarshal(input, &records)
		if errArray == nil {
			inputTypeChan <- ArrayInput // It's an array
			for _, record := range records {
				result := processInput(record, config)
				if result != nil {
					objs <- result
				}
			}
			return
		}

		// If unmarshaling into an array fails, try a single object.
		var record map[string]any
		errObject := json.Unmarshal(input, &record)
		if errObject == nil {
			inputTypeChan <- SingletonInput // It's a single object
			result := processInput(record, config)
			if result != nil {
				objs <- result
			}
			return
		}

		// If both fail, report the most likely error.
		log.Fatalf("Error parsing JSON input: %v", errArray)
	} else {
		// JSONL format
		inputTypeChan <- StreamInput // JSONL is always a stream
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
			log.Fatalf("Error reading JSONL input: %v", err)
		}
	}
}

func readYAMLInput(objs chan<- map[string]any, inputTypeChan chan<- InputType, config Config) {
	defer close(objs)
	defer close(inputTypeChan)
	decoder := yaml.NewDecoder(os.Stdin)

	var firstObj any
	err := decoder.Decode(&firstObj)
	if err != nil {
		if err == io.EOF { // Handle empty input
			return
		}
		log.Fatalf("Error decoding first YAML object: %v", err)
	}

	var secondObj any
	err = decoder.Decode(&secondObj)

	// Case 1: Single document input
	if err == io.EOF {
		// It's a single document. Check if it's an array or a singleton object.
		if reflect.TypeOf(firstObj).Kind() == reflect.Slice {
			inputTypeChan <- ArrayInput
			s := reflect.ValueOf(firstObj)
			for i := 0; i < s.Len(); i++ {
				item := s.Index(i).Interface()
				if rec, ok := item.(map[string]any); ok {
					result := processInput(rec, config)
					if result != nil {
						objs <- result
					}
				} else {
					log.Printf("Skipping item in YAML array; not a map[string]any: %T", item)
				}
			}
		} else {
			inputTypeChan <- SingletonInput
			if rec, ok := firstObj.(map[string]any); ok {
				result := processInput(rec, config)
				if result != nil {
					objs <- result
				}
			} else {
				log.Printf("Skipping YAML document; not a map[string]any: %T", firstObj)
			}
		}
		return // Done
	}

	// Case 2: Error on second document
	if err != nil {
		log.Fatalf("Error decoding second YAML object: %v", err)
	}

	// Case 3: Stream input
	inputTypeChan <- StreamInput

	// Process the two objects we already have
	processDecodedYAML(firstObj, objs, config)
	processDecodedYAML(secondObj, objs, config)

	// Loop for the rest of the stream
	for {
		var doc any
		err := decoder.Decode(&doc)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("Error decoding YAML stream: %v", err)
			continue
		}
		processDecodedYAML(doc, objs, config)
	}
}

// processDecodedYAML is a helper to avoid repetition in readYAMLInput
func processDecodedYAML(doc any, objs chan<- map[string]any, config Config) {
	if rec, ok := doc.(map[string]any); ok {
		result := processInput(rec, config)
		if result != nil {
			objs <- result
		}
	} else {
		log.Printf("Skipping YAML document in stream; not a map[string]any: %T", doc)
	}
}

func readCSVInput(objs chan<- map[string]any, inputTypeChan chan<- InputType, config Config) {
	defer close(objs)
	defer close(inputTypeChan)
	inputTypeChan <- ArrayInput // CSV is always treated as an array

	reader := csv.NewReader(os.Stdin)

	// Read header row
	headers, err := reader.Read()
	if err != nil {
		if err == io.EOF { // Handle empty file
			return
		}
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
