use std::io::Write;
use serde_json::Value;
use crate::config::Config;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputType {
    SingletonInput,
    ArrayInput,
    StreamInput,
}

pub trait RecordFormatter {
    fn write_header(&mut self) -> Result<(), std::io::Error>;
    fn write_record(&mut self, record: &Value) -> Result<(), std::io::Error>;
    fn write_footer(&mut self) -> Result<(), std::io::Error>;
    fn flush(&mut self) -> Result<(), std::io::Error>;
}

pub fn new_formatter<'a, W: Write>(
    config: &Config,
    writer: W,
    input_type: InputType,
) -> Result<Box<dyn RecordFormatter + 'a>, std::io::Error>
where
    W: 'a,
{
    match config.output_format.as_str() {
        "json" => {
            let is_singleton = input_type == InputType::SingletonInput;
            Ok(Box::new(JsonFormatter::new(writer, is_singleton)))
        }
        "jsonl" => Ok(Box::new(JsonlFormatter::new(writer))),
        "jsonp" => {
            let is_singleton = input_type == InputType::SingletonInput;
            Ok(Box::new(JsonpFormatter::new(writer, is_singleton)))
        }
        "yaml" => Ok(Box::new(YamlFormatter::new(writer, input_type))),
        "csv" => Ok(Box::new(CsvFormatter::new(writer, config))),
        other => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("unsupported output format: {}", other),
        )),
    }
}

// ========
// JsonFormatter formats records as a single JSON array or a singleton.
pub struct JsonFormatter<W: Write> {
    writer: W,
    is_singleton_input: bool,
    is_first: bool,
}

impl<W: Write> JsonFormatter<W> {
    pub fn new(writer: W, is_singleton_input: bool) -> Self {
        JsonFormatter {
            writer,
            is_singleton_input,
            is_first: true,
        }
    }
}

impl<W: Write> RecordFormatter for JsonFormatter<W> {
    fn write_header(&mut self) -> Result<(), std::io::Error> {
        if self.is_singleton_input {
            return Ok(());
        }
        self.writer.write_all(b"[\n")
    }

    fn write_record(&mut self, record: &Value) -> Result<(), std::io::Error> {
        if !self.is_singleton_input && !self.is_first {
            self.writer.write_all(b",\n")?;
        }
        self.is_first = false;

        let out_bytes = serde_json::to_vec(record)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        self.writer.write_all(&out_bytes)
    }

    fn write_footer(&mut self) -> Result<(), std::io::Error> {
        if self.is_singleton_input {
            return Ok(());
        }
        self.writer.write_all(b"\n]")
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.writer.flush()
    }
}

// ========
// JsonlFormatter formats records as newline-delimited JSON objects.
pub struct JsonlFormatter<W: Write> {
    writer: W,
}

impl<W: Write> JsonlFormatter<W> {
    pub fn new(writer: W) -> Self {
        JsonlFormatter { writer }
    }
}

impl<W: Write> RecordFormatter for JsonlFormatter<W> {
    fn write_header(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn write_record(&mut self, record: &Value) -> Result<(), std::io::Error> {
        let mut out_bytes = serde_json::to_vec(record)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        out_bytes.push(b'\n');
        self.writer.write_all(&out_bytes)
    }

    fn write_footer(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.writer.flush()
    }
}

// ========
// JsonpFormatter formats records as a pretty-printed JSON array or singleton.
pub struct JsonpFormatter<W: Write> {
    writer: W,
    is_singleton_input: bool,
    is_first: bool,
}

impl<W: Write> JsonpFormatter<W> {
    pub fn new(writer: W, is_singleton_input: bool) -> Self {
        JsonpFormatter {
            writer,
            is_singleton_input,
            is_first: true,
        }
    }
}

impl<W: Write> RecordFormatter for JsonpFormatter<W> {
    fn write_header(&mut self) -> Result<(), std::io::Error> {
        if self.is_singleton_input {
            return Ok(());
        }
        self.writer.write_all(b"[")
    }

    fn write_record(&mut self, record: &Value) -> Result<(), std::io::Error> {
        if !self.is_singleton_input && !self.is_first {
            self.writer.write_all(b",")?;
        }
        self.is_first = false;

        let out_bytes = serde_json::to_vec_pretty(record)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        self.writer.write_all(&out_bytes)
    }

    fn write_footer(&mut self) -> Result<(), std::io::Error> {
        if self.is_singleton_input {
            return Ok(());
        }
        self.writer.write_all(b"]")
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.writer.flush()
    }
}

// ========
// YamlFormatter formats records as a YAML stream, single doc, or array of docs.
pub struct YamlFormatter<W: Write> {
    writer: W,
    input_type: InputType,
    is_first: bool,
    records: Vec<Value>,
}

impl<W: Write> YamlFormatter<W> {
    pub fn new(writer: W, input_type: InputType) -> Self {
        YamlFormatter {
            writer,
            input_type,
            is_first: true,
            records: Vec::new(),
        }
    }
}

impl<W: Write> RecordFormatter for YamlFormatter<W> {
    fn write_header(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn write_record(&mut self, record: &Value) -> Result<(), std::io::Error> {
        match self.input_type {
            InputType::SingletonInput => {
                let out_str = serde_yaml::to_string(record)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                let mut s = out_str.as_str();
                if s.starts_with("---\n") {
                    s = &s[4..];
                }
                self.writer.write_all(s.as_bytes())
            }
            InputType::ArrayInput => {
                self.records.push(record.clone());
                Ok(())
            }
            InputType::StreamInput => {
                if !self.is_first {
                    self.writer.write_all(b"---\n")?;
                }
                self.is_first = false;

                let out_str = serde_yaml::to_string(record)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                let mut s = out_str.as_str();
                if s.starts_with("---\n") {
                    s = &s[4..];
                }
                self.writer.write_all(s.as_bytes())
            }
        }
    }

    fn write_footer(&mut self) -> Result<(), std::io::Error> {
        if self.input_type == InputType::ArrayInput && !self.records.is_empty() {
            let out_str = serde_yaml::to_string(&self.records)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let mut s = out_str.as_str();
            if s.starts_with("---\n") {
                s = &s[4..];
            }
            self.writer.write_all(s.as_bytes())?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.writer.flush()
    }
}

// ========
// CsvFormatter formats records as CSV.
pub struct CsvFormatter<W: Write> {
    csv_writer: csv::Writer<W>,
    header_order: Vec<String>,
    header_written: bool,
}

impl<W: Write> CsvFormatter<W> {
    pub fn new(writer: W, config: &Config) -> Self {
        CsvFormatter {
            csv_writer: csv::Writer::from_writer(writer),
            header_order: compute_header_order(config),
            header_written: false,
        }
    }
}

impl<W: Write> RecordFormatter for CsvFormatter<W> {
    fn write_header(&mut self) -> Result<(), std::io::Error> {
        if !self.header_order.is_empty() {
            self.header_written = true;
            self.csv_writer.write_record(&self.header_order)?;
        }
        Ok(())
    }

    fn write_record(&mut self, record: &Value) -> Result<(), std::io::Error> {
        let map = match record.as_object() {
            Some(m) => m,
            None => return Ok(()),
        };

        if !self.header_written {
            let mut keys: Vec<String> = map.keys().cloned().collect();
            keys.sort();
            self.header_order = keys;
            self.header_written = true;
            self.csv_writer.write_record(&self.header_order)?;
        }

        let mut row = Vec::with_capacity(self.header_order.len());
        for h in &self.header_order {
            if let Some(val) = map.get(h) {
                match val {
                    Value::String(s) => {
                        row.push(s.clone());
                    }
                    Value::Null => {
                        row.push(String::new());
                    }
                    other => {
                        let serialized = serde_json::to_string(other)
                            .unwrap_or_else(|_| String::new());
                        row.push(serialized);
                    }
                }
            } else {
                row.push(String::new());
            }
        }
        self.csv_writer.write_record(&row)?;
        Ok(())
    }

    fn write_footer(&mut self) -> Result<(), std::io::Error> {
        self.csv_writer.flush()
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.csv_writer.flush()
    }
}

pub fn compute_header_order(config: &Config) -> Vec<String> {
    let mut headers = Vec::new();
    for m in &config.common_output {
        for k in m.keys() {
            if !headers.contains(k) {
                headers.push(k.clone());
            }
        }
    }
    for rule in &config.specific_outputs {
        for m in &rule.output {
            for k in m.keys() {
                if !headers.contains(k) {
                    headers.push(k.clone());
                }
            }
        }
    }
    headers
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn must_config(yaml_string: &str) -> Config {
        serde_yaml::from_str(yaml_string).unwrap()
    }

    #[test]
    fn test_compute_header_order() {
        // common only
        {
            let cfg = must_config("
common-output:
- a: foo
- b: bar
");
            let got = compute_header_order(&cfg);
            let want = vec!["a".to_string(), "b".to_string()];
            assert_eq!(got, want);
        }

        // specific only
        {
            let cfg = must_config("
specific-outputs:
- output:
  - x: foo
  - y: bar
");
            let got = compute_header_order(&cfg);
            let want = vec!["x".to_string(), "y".to_string()];
            assert_eq!(got, want);
        }

        // overlap
        {
            let cfg = must_config("
common-output:
- a: foo
specific-outputs:
- output:
  - a: foo
  - b: bar
");
            let got = compute_header_order(&cfg);
            let want = vec!["a".to_string(), "b".to_string()];
            assert_eq!(got, want);
        }
    }

    #[test]
    fn test_json_formatter_singleton_vs_array() {
        let test_record1 = json!({"name": "Alice", "age": 30});
        let test_record2 = json!({"name": "Bob", "age": 25});

        // singleton output
        {
            let mut buf = Vec::new();
            {
                let mut formatter = JsonFormatter::new(&mut buf, true);
                formatter.write_header().unwrap();
                formatter.write_record(&test_record1).unwrap();
                formatter.write_footer().unwrap();
                formatter.flush().unwrap();
            }
            let got = String::from_utf8(buf).unwrap();
            let want = r#"{"age":30,"name":"Alice"}"#;
            assert_eq!(got, want);
        }

        // array output
        {
            let mut buf = Vec::new();
            {
                let mut formatter = JsonFormatter::new(&mut buf, false);
                formatter.write_header().unwrap();
                formatter.write_record(&test_record1).unwrap();
                formatter.write_record(&test_record2).unwrap();
                formatter.write_footer().unwrap();
                formatter.flush().unwrap();
            }
            let got = String::from_utf8(buf).unwrap();
            let want = "[\n{\"age\":30,\"name\":\"Alice\"},\n{\"age\":25,\"name\":\"Bob\"}\n]";
            assert_eq!(got, want);
        }
    }

    #[test]
    fn test_jsonp_formatter_singleton_vs_array() {
        let test_record1 = json!({"name": "Alice", "age": 30});
        let test_record2 = json!({"name": "Bob", "age": 25});

        // singleton output
        {
            let mut buf = Vec::new();
            {
                let mut formatter = JsonpFormatter::new(&mut buf, true);
                formatter.write_header().unwrap();
                formatter.write_record(&test_record1).unwrap();
                formatter.write_footer().unwrap();
                formatter.flush().unwrap();
            }
            let got = String::from_utf8(buf).unwrap();
            let want = "{\n  \"age\": 30,\n  \"name\": \"Alice\"\n}";
            assert_eq!(got, want);
        }

        // array output
        {
            let mut buf = Vec::new();
            {
                let mut formatter = JsonpFormatter::new(&mut buf, false);
                formatter.write_header().unwrap();
                formatter.write_record(&test_record1).unwrap();
                formatter.write_record(&test_record2).unwrap();
                formatter.write_footer().unwrap();
                formatter.flush().unwrap();
            }
            let got = String::from_utf8(buf).unwrap();
            let want = "[{\n  \"age\": 30,\n  \"name\": \"Alice\"\n},{\n  \"age\": 25,\n  \"name\": \"Bob\"\n}]";
            assert_eq!(got, want);
        }
    }

    #[test]
    fn test_yaml_formatter() {
        let test_record1 = json!({"name": "Alice", "age": 30});
        let test_record2 = json!({"name": "Bob", "age": 25});

        // singleton output
        {
            let mut buf = Vec::new();
            {
                let mut formatter = YamlFormatter::new(&mut buf, InputType::SingletonInput);
                formatter.write_header().unwrap();
                formatter.write_record(&test_record1).unwrap();
                formatter.write_footer().unwrap();
                formatter.flush().unwrap();
            }
            let got = String::from_utf8(buf).unwrap();
            let want = "age: 30\nname: Alice\n";
            assert_eq!(got, want);
        }

        // array output
        {
            let mut buf = Vec::new();
            {
                let mut formatter = YamlFormatter::new(&mut buf, InputType::ArrayInput);
                formatter.write_header().unwrap();
                formatter.write_record(&test_record1).unwrap();
                formatter.write_record(&test_record2).unwrap();
                formatter.write_footer().unwrap();
                formatter.flush().unwrap();
            }
            let got = String::from_utf8(buf).unwrap();
            let want = "- age: 30\n  name: Alice\n- age: 25\n  name: Bob\n";
            assert_eq!(got, want);
        }

        // stream output
        {
            let mut buf = Vec::new();
            {
                let mut formatter = YamlFormatter::new(&mut buf, InputType::StreamInput);
                formatter.write_header().unwrap();
                formatter.write_record(&test_record1).unwrap();
                formatter.write_record(&test_record2).unwrap();
                formatter.write_footer().unwrap();
                formatter.flush().unwrap();
            }
            let got = String::from_utf8(buf).unwrap();
            let want = "age: 30\nname: Alice\n---\nage: 25\nname: Bob\n";
            assert_eq!(got, want);
        }
    }

    #[test]
    fn test_jsonl_formatter() {
        let test_record1 = json!({"name": "Alice", "age": 30});
        let test_record2 = json!({"name": "Bob", "age": 25});

        let mut buf = Vec::new();
        {
            let mut formatter = JsonlFormatter::new(&mut buf);
            formatter.write_header().unwrap();
            formatter.write_record(&test_record1).unwrap();
            formatter.write_record(&test_record2).unwrap();
            formatter.write_footer().unwrap();
            formatter.flush().unwrap();
        }
        let got = String::from_utf8(buf).unwrap();
        let want = "{\"age\":30,\"name\":\"Alice\"}\n{\"age\":25,\"name\":\"Bob\"}\n";
        assert_eq!(got, want);
    }

    #[test]
    fn test_csv_formatter() {
        // config headers
        {
            let cfg = must_config("
common-output:
- name: name
- age: age
");
            let test_record1 = json!({"name": "Alice", "age": "30"});
            let test_record2 = json!({"name": "Bob", "age": 25});

            let mut buf = Vec::new();
            {
                let mut formatter = CsvFormatter::new(&mut buf, &cfg);
                formatter.write_header().unwrap();
                formatter.write_record(&test_record1).unwrap();
                formatter.write_record(&test_record2).unwrap();
                formatter.write_footer().unwrap();
                formatter.flush().unwrap();
            }
            let got = String::from_utf8(buf).unwrap();
            let want = "name,age\nAlice,30\nBob,25\n";
            assert_eq!(got, want);
        }

        // dynamic headers
        {
            let cfg = Config {
                match_rule: "all".to_string(),
                clone_original: false,
                common_output: Vec::new(),
                specific_outputs: Vec::new(),
                input_format: String::new(),
                output_format: String::new(),
                buffered: false,
            };
            let test_record1 = json!({"name": "Alice", "age": 30});
            let test_record2 = json!({"name": "Bob", "age": 25});

            let mut buf = Vec::new();
            {
                let mut formatter = CsvFormatter::new(&mut buf, &cfg);
                formatter.write_header().unwrap();
                formatter.write_record(&test_record1).unwrap();
                formatter.write_record(&test_record2).unwrap();
                formatter.write_footer().unwrap();
                formatter.flush().unwrap();
            }
            let got = String::from_utf8(buf).unwrap();
            let want = "age,name\n30,Alice\n25,Bob\n";
            assert_eq!(got, want);
        }
    }
}

