# Transmogrifier (`trmg`)

Transmogrifier is a fast, configurable command-line utility designed to reshape, filter, and convert structured data streams. It reads input in various formats, applies mapping and matching rules defined in a YAML configuration file, and writes the transformed data to a chosen output format.

Transmogrifier supports streaming data concurrently (using Go goroutines) and includes an optimized Rust implementation for sequential high-performance workloads.

---

## Table of Contents
- [Command Line Usage](#command-line-usage)
- [Supported Formats & Behaviors](#supported-formats--behaviors)
- [Configuration Reference](#configuration-reference)
  - [Global Settings](#global-settings)
  - [Field Mappings (Expressions)](#field-mappings-expressions)
  - [Conditional Selection (Rules)](#conditional-selection-rules)
- [Comprehensive Example](#comprehensive-example)
- [Rust Experiment](#rust-experiment)

---

## Command Line Usage

Compile the Go binary first:
```bash
go build -o trmg
```

Run the CLI using flags to specify input/output formats and your configuration file:
```bash
./trmg -i <input_format> -o <output_format> -c <config_path> [options]
```

### Options and Flags

| Flag | Argument Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `-c` | `string` (path) | `""` | Path to the YAML configuration file. |
| `-i` | `string` | `"yaml"` | Input format: `json`, `jsonl`, `yaml`, or `csv`. |
| `-o` | `string` | `"yaml"` | Output format: `json`, `jsonl`, `jsonp` (pretty JSON), `yaml`, or `csv`. |
| `-buffered` | `bool` (flag) | `false` | Force buffered output (reduces flushes, optimizing throughput for large streams). |
| `-version` | `bool` (flag) | `false` | Displays the current version and exits. |

---

## Supported Formats & Behaviors

Transmogrifier adapts its input parsing and output rendering depending on the chosen formats:

| Format | Input Behavior | Output Behavior |
| :--- | :--- | :--- |
| **JSON** | Parses a single object or an array of objects. | Outputs a single JSON object if input was a singleton; otherwise, outputs a JSON array (`[ ... ]`). |
| **JSONL** | Parses line-delimited JSON objects. | Outputs each record as a single line JSON object terminated by a newline. |
| **JSONP** | Parses a single object or an array of objects. | Pretty-printed JSON array (or pretty-printed singleton object if the input was a single object). |
| **YAML** | Parses a single document, a list, or a multi-document stream. | - Singleton input: outputs a single YAML document.<br>- Array input: outputs a single YAML array.<br>- Stream input: outputs multi-document YAML separated by `---`. |
| **CSV** | Parses the first line as header names. Converts each row into a key-value record. | Flushes records to a table. Converts nested objects/arrays to inline JSON string values. |

### CSV Header Ordering
When writing to CSV, the columns are ordered as follows:
1. Keys defined in `common-output` (in the order they are listed).
2. Keys defined in `specific-outputs` (in the order they are encountered).
3. If no configuration is provided, the columns are dynamically extracted from the keys of the very first processed record and sorted alphabetically.

---

## Configuration Reference

The configuration file is written in YAML. It defines global behaviors, universal field mappings, and conditional transformation rules.

The structures under the hood are defined in [config.go](file:///home/tom/git/trmg/config.go) and evaluated in [main.go](file:///home/tom/git/trmg/main.go).

### Global Settings

```yaml
# Control what happens to records that fail to match any "specific-outputs" rule.
# Options:
#  - "all" (default): Keep the record (applying common-output mappings).
#  - "drop-no-match": Discard the record entirely.
match-rule: all

# If true, the output record starts as a clone of the input record.
# Mappings will then add or overwrite fields on top of the original record.
# If false, the output record starts empty and only contains explicitly mapped fields.
clone-original: false
```

---

### Field Mappings (Expressions)

Mappings define how fields from the source record are copied, transformed, or nested in the output record. They are specified as a list of key-value maps:

```yaml
common-output:
  - destination_field: mapping_expression
```

There are four ways to define a `mapping_expression`:

#### 1. Dot-Notation Path Lookup (String)
If the expression is a string, Transmogrifier treats it as a dot-separated path to extract nested values.
```yaml
# Retrieves the value of record["resource"]["labels"]["project_id"]
project: resource.labels.project_id
```
> [!NOTE]
> If the path does not exist in the source record, the literal string of the expression is assigned to the output (e.g. if `resource.labels.project_id` isn't found, the value `"resource.labels.project_id"` will be written).

#### 2. Static Value Assignment
If you map a field to a path that is not present in the input stream, it falls back to the literal string itself.
```yaml
# Since "production" is likely not a path in your input, it assigns "production" as a static string.
environment: production
```

#### 3. Regular Expression Capture
Extract and substitute values using regular expressions. This is defined by a map containing the keys `src`, `regex`, and `value`:
* `src`: Dot-notation path of the source field.
* `regex`: Regular expression containing capture groups.
* `value`: The template for the output string, substituting capture groups using `$1`, `$2`, etc.

```yaml
log-name:
  src: logName
  regex: projects/.*?/logs/(.*)
  value: $1
```

#### 4. Nested Map Construction
If you define a nested YAML map (without `src`, `regex`, `value` keys), Transmogrifier builds a structured nested sub-object in the output record:
```yaml
metadata:
  service: resource.type
  region: resource.labels.region
```
**Resulting JSON Output Structure:**
```json
{
  "metadata": {
    "service": "k8s_container",
    "region": "us-central1"
  }
}
```

---

### Conditional Selection (Rules)

Conditional rules allow you to apply transformations and filter records dynamically using the `specific-outputs` section. Rules are evaluated sequentially (first-match-wins).

#### Anatomy of a Rule
Each rule can check values using `eq` (exact match), `matches` (regex match), and composable logical `and` conditions.

```yaml
specific-outputs:
  - field: path.to.check
    eq: "exact_value"               # (Optional) Checks for exact string equality
    matches: "regex_pattern"        # (Optional) Checks if value matches regex
    and:                            # (Optional) List of additional conditions
      - field: another.field
        eq: "another_exact_value"
      - field: pattern.field
        matches: "^[0-9]+$"
    output:                         # Mappings to apply only if this rule matches
      - extra_field: source_path
```

* **Sequential Evaluation:** Only the *first* rule that matches a record is applied. Once a rule matches, its `output` mappings are merged into the record, and the evaluator skips all subsequent rules.
* **Match Filtering:** If `match-rule: drop-no-match` is set, and a record does not match any rule under `specific-outputs`, it is excluded from the output stream.

---

## Comprehensive Example

Given the following configuration file (`config.yaml`):

```yaml
match-rule: drop-no-match
clone-original: false

common-output:
  - timestamp: timestamp
  - severity: severity
  - project: resource.labels.project_id

specific-outputs:
  # Rule 1: Match k8s audit logs
  - field: resource.type
    eq: k8s_cluster
    and:
      - field: logName
        matches: .*cloudaudit.*
    output:
      - log_type: audit
      - principal: protoPayload.authenticationInfo.principalEmail
      - action: protoPayload.methodName

  # Rule 2: Match compute instance syslog
  - field: resource.type
    eq: gce_instance
    output:
      - log_type: syslog
      - hostname: resource.labels.instance_id
```

### Input Stream (JSONL)
```json
{"timestamp":"2026-06-01T12:00:00Z","severity":"INFO","resource":{"type":"k8s_cluster","labels":{"project_id":"my-gcp-project"}},"logName":"projects/my-gcp-project/logs/cloudaudit.googleapis.com%2Factivity","protoPayload":{"authenticationInfo":{"principalEmail":"alice@company.com"},"methodName":"createNamespace"}}
{"timestamp":"2026-06-01T12:05:00Z","severity":"WARNING","resource":{"type":"gce_instance","labels":{"project_id":"my-gcp-project","instance_id":"vm-prod-01"}}}
{"timestamp":"2026-06-01T12:10:00Z","severity":"INFO","resource":{"type":"cloud_run_revision","labels":{"project_id":"my-gcp-project"}}}
```

### Execution Command
```bash
./trmg -i jsonl -o json -c config.yaml < input.jsonl
```

### Output Stream (JSON Array)
```json
[
  {
    "action": "createNamespace",
    "log_type": "audit",
    "principal": "alice@company.com",
    "project": "my-gcp-project",
    "severity": "INFO",
    "timestamp": "2026-06-01T12:00:00Z"
  },
  {
    "hostname": "vm-prod-01",
    "log_type": "syslog",
    "project": "my-gcp-project",
    "severity": "WARNING",
    "timestamp": "2026-06-01T12:05:00Z"
  }
]
```
*(Note: The third record was dropped entirely because `match-rule` is `drop-no-match` and it did not match either specific rule).*

---

## Rust Experiment

Transmogrifier has been ported to Rust as a highly optimized, sequential stream processor.

### Build
To compile the optimized release binary:
```bash
cargo build --release
```
The compiled binary will be located at `./target/release/trmg-rust`.

### Run Tests
To execute the complete suite of ported unit tests:
```bash
cargo test
```

### Performance Benchmarking
A Python script is included to generate large, high-fidelity log streams for performance testing.

1. **Synthesize Datasets** (generates 200,000 JSONL records and 200,000 YAML records inside `test-data/`):
   ```bash
   python3 test-data/generate.py
   ```

2. **Benchmark JSONL Stream** (200,000 records):
   ```bash
   # Build Go binary first
   go build -o trmg-go

   # Run Go JSONL Benchmark
   time ./trmg-go -c test-data/config.yaml -i jsonl -o jsonl < test-data/large_audit.jsonl > /dev/null

   # Run Rust JSONL Benchmark
   time ./target/release/trmg-rust -c test-data/config.yaml -i jsonl -o jsonl --buffered < test-data/large_audit.jsonl > /dev/null
   ```

3. **Benchmark YAML Stream** (200,000 records):
   ```bash
   # Run Go YAML Benchmark
   time ./trmg-go -c test-data/config.yaml -i yaml -o yaml < test-data/large_audit.yaml > /dev/null

   # Run Rust YAML Benchmark
   time ./target/release/trmg-rust -c test-data/config.yaml -i yaml -o yaml --buffered < test-data/large_audit.yaml > /dev/null
   ```
