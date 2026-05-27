# Transmogrifier

Transmogrifier translates jsonl and yaml streams. You can customize the output objects' structure as well as output json, jsonl, jsonp, yaml, or csv.

Example config:

```yaml
# match-rule: all
match-rule: drop-no-match

common-output:
- timestamp: timestamp
- project: resource.labels.project_id
- log-name:
    src: logName
    regex: projects/.*?/logs/(.*)
    value: $1
- resource-type: resource.type
- resource-labels: resource.labels
- labels: labels

specific-outputs:
- field: logName
  eq: projects/myprojname/logs/cloudaudit.googleapis.com/data_access
  and:
  - field: resource.type
    eq: k8s_cluster
  output:
  - dataset-id: resource.labels.dataset_id
- field: logName
  matches: .*?cloudaudit.googleapis.com/.*
  output:
  - principal: protoPayload.authenticationInfo.principalEmail
  - method-name: protoPayload.methodName
  - caller-ip: protoPayload.requestMetadata.callerIp
  - test-custom:
      mn: protoPayload.methodName 
      rn: protoPayload.methodName 
```

### Pass-through (Clone Original)

If you'd like to output the entire structure of the original matching objects but just add or override certain fields, use `clone-original: true`:

```yaml
clone-original: true

# no common-output needed since we inherit all fields
specific-outputs:
- field: logName
  matches: .*?cloudaudit.googleapis.com/.*
  output:
  - principal: protoPayload.authenticationInfo.principalEmail
```

### Command Line Usage with Heredocs

You can use bash process substitution to elegantly provide the configuration file in-line while parsing standard input:

```bash
./trmg -i jsonl -o jsonl -c <(cat <<'EOF'
clone-original: true
specific-outputs:
- field: level
  eq: "error"
  output:
  - requires_attention: YES
EOF
) <<'EOF'
{"level": "info", "message": "User logged in"}
{"level": "error", "message": "Database connection lost"}
EOF
```

Expected output:

```jsonl
{"level":"info","message":"User logged in"}
{"level":"error","message":"Database connection lost","requires_attention":"YES"}
```

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


