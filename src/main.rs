pub mod config;
pub mod process;
pub mod formatters;

use std::io::{BufReader, BufWriter, Write};
use clap::Parser;
use serde_json::{Map, Value};
use serde::Deserialize;
use crate::config::Config;
use crate::formatters::InputType;

#[derive(Parser, Debug)]
#[command(
    name = "trmg",
    about = "An application that takes in a jsonl, yaml, or csv input stream and lets you customize the output objects and data type.",
    version
)]
struct Args {
    #[arg(short = 'c', help = "Path to configuration YAML file")]
    config: Option<String>,

    #[arg(short = 'i', default_value = "yaml", help = "Input format: json, jsonl, yaml, or csv")]
    input: String,

    #[arg(short = 'o', default_value = "yaml", help = "Output format: json, jsonl, jsonp (pretty), yaml, or csv")]
    output: String,

    #[arg(long = "buffered", help = "Force buffered output (don't flush after each record)")]
    buffered: bool,
}

fn main() {
    let args = Args::parse();

    let valid_inputs = ["json", "jsonl", "yaml", "csv"];
    if !valid_inputs.contains(&args.input.as_str()) {
        eprintln!("Invalid input format: {}", args.input);
        std::process::exit(0);
    }

    let valid_outputs = ["json", "jsonl", "jsonp", "yaml", "csv"];
    if !valid_outputs.contains(&args.output.as_str()) {
        eprintln!("Invalid output format: {}", args.output);
        std::process::exit(0);
    }

    let mut config = if let Some(ref path) = args.config {
        let config_data = std::fs::read(path).unwrap_or_else(|e| {
            eprintln!("Error reading config file: {}", e);
            std::process::exit(1);
        });
        serde_yaml::from_slice(&config_data).unwrap_or_else(|e| {
            eprintln!("Error parsing config file: {}", e);
            std::process::exit(1);
        })
    } else {
        Config {
            match_rule: "all".to_string(),
            clone_original: false,
            common_output: Vec::new(),
            specific_outputs: Vec::new(),
            input_format: String::new(),
            output_format: String::new(),
            buffered: false,
        }
    };

    config.input_format = args.input;
    config.output_format = args.output;
    config.buffered = args.buffered;

    if config.match_rule.is_empty() {
        config.match_rule = "all".to_string();
    }

    let stdin = std::io::stdin();
    let stdout = std::io::stdout();

    let mut reader = BufReader::new(stdin.lock());
    let mut writer = BufWriter::new(stdout.lock());

    let result = match config.input_format.as_str() {
        "json" => read_json_input(&mut reader, &mut writer, &config),
        "jsonl" => read_jsonl_input(reader, &mut writer, &config),
        "yaml" => read_yaml_input(&mut reader, &mut writer, &config),
        "csv" => read_csv_input(&mut reader, &mut writer, &config),
        _ => unreachable!(),
    };

    if let Err(e) = result {
        eprintln!("Error processing input: {}", e);
        std::process::exit(1);
    }

    if let Err(e) = writer.flush() {
        eprintln!("Error flushing output: {}", e);
        std::process::exit(1);
    }
}

fn read_json_input<R: std::io::Read, W: std::io::Write>(
    reader: &mut R,
    writer: &mut W,
    config: &Config,
) -> Result<(), std::io::Error> {
    let mut input = Vec::new();
    reader.read_to_end(&mut input)?;
    if input.is_empty() {
        return Ok(());
    }

    if let Ok(records) = serde_json::from_slice::<Vec<Value>>(&input) {
        let mut formatter = crate::formatters::new_formatter(config, &mut *writer, InputType::ArrayInput)?;
        formatter.write_header()?;
        for record in records {
            if let Some(result) = crate::process::process_input(&record, config) {
                formatter.write_record(&result)?;
                if !config.buffered {
                    formatter.flush()?;
                }
            }
        }
        formatter.write_footer()?;
        return Ok(());
    }

    if let Ok(record) = serde_json::from_slice::<Value>(&input) {
        if record.is_object() {
            let mut formatter = crate::formatters::new_formatter(config, &mut *writer, InputType::SingletonInput)?;
            formatter.write_header()?;
            if let Some(result) = crate::process::process_input(&record, config) {
                formatter.write_record(&result)?;
            }
            formatter.write_footer()?;
            return Ok(());
        }
    }

    eprintln!("Error parsing JSON input");
    std::process::exit(1);
}

fn read_jsonl_input<R: std::io::BufRead, W: std::io::Write>(
    mut reader: R,
    writer: &mut W,
    config: &Config,
) -> Result<(), std::io::Error> {
    let mut formatter = crate::formatters::new_formatter(config, &mut *writer, InputType::StreamInput)?;
    formatter.write_header()?;

    let mut line = String::new();
    while reader.read_line(&mut line)? > 0 {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            line.clear();
            continue;
        }
        match serde_json::from_str::<Value>(trimmed) {
            Ok(record) => {
                if record.is_object() {
                    if let Some(result) = crate::process::process_input(&record, config) {
                        formatter.write_record(&result)?;
                        if !config.buffered {
                            formatter.flush()?;
                        }
                    }
                } else {
                    eprintln!("Error parsing JSON: not an object");
                }
            }
            Err(e) => {
                eprintln!("Error parsing JSON: {}", e);
            }
        }
        line.clear();
    }

    formatter.write_footer()?;
    Ok(())
}

fn read_yaml_input<R: std::io::Read, W: std::io::Write>(
    reader: &mut R,
    writer: &mut W,
    config: &Config,
) -> Result<(), std::io::Error> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    if bytes.is_empty() {
        return Ok(());
    }

    let s = match std::str::from_utf8(&bytes) {
        Ok(v) => v,
        Err(_) => {
            eprintln!("Error: input is not valid UTF-8");
            std::process::exit(1);
        }
    };

    let mut docs = Vec::new();
    for doc_deserializer in serde_yaml::Deserializer::from_str(s) {
        match Value::deserialize(doc_deserializer) {
            Ok(v) => docs.push(v),
            Err(e) => {
                eprintln!("Error decoding YAML: {}", e);
                break;
            }
        }
    }

    if docs.is_empty() {
        return Ok(());
    }

    if docs.len() == 1 {
        let first_doc = &docs[0];
        if let Some(slice) = first_doc.as_array() {
            let mut formatter = crate::formatters::new_formatter(config, &mut *writer, InputType::ArrayInput)?;
            formatter.write_header()?;
            for item in slice {
                process_decoded_yaml(item, &mut *formatter, config)?;
            }
            formatter.write_footer()?;
        } else {
            let mut formatter = crate::formatters::new_formatter(config, &mut *writer, InputType::SingletonInput)?;
            formatter.write_header()?;
            process_decoded_yaml(first_doc, &mut *formatter, config)?;
            formatter.write_footer()?;
        }
    } else {
        let mut formatter = crate::formatters::new_formatter(config, &mut *writer, InputType::StreamInput)?;
        formatter.write_header()?;
        for doc in &docs {
            process_decoded_yaml(doc, &mut *formatter, config)?;
        }
        formatter.write_footer()?;
    }

    Ok(())
}

fn process_decoded_yaml(
    doc: &Value,
    formatter: &mut dyn crate::formatters::RecordFormatter,
    config: &Config,
) -> Result<(), std::io::Error> {
    if doc.is_object() {
        if let Some(result) = crate::process::process_input(doc, config) {
            formatter.write_record(&result)?;
            if !config.buffered {
                formatter.flush()?;
            }
        }
    } else {
        eprintln!("Skipping YAML document in stream; not a map[string]any: {:?}", doc);
    }
    Ok(())
}

fn read_csv_input<R: std::io::Read, W: std::io::Write>(
    reader: R,
    writer: &mut W,
    config: &Config,
) -> Result<(), std::io::Error> {
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(reader);

    let mut records_iter = csv_reader.records();

    let headers = match records_iter.next() {
        Some(Ok(header_record)) => {
            let mut h = Vec::new();
            for field in header_record.iter() {
                h.push(field.to_string());
            }
            h
        }
        Some(Err(e)) => {
            eprintln!("Error reading CSV header: {}", e);
            std::process::exit(1);
        }
        None => return Ok(()),
    };

    let mut formatter = crate::formatters::new_formatter(config, &mut *writer, InputType::ArrayInput)?;
    formatter.write_header()?;

    for record_result in records_iter {
        match record_result {
            Ok(record) => {
                let mut obj = Map::new();
                for (i, value) in record.iter().enumerate() {
                    if i < headers.len() {
                        obj.insert(headers[i].clone(), Value::String(value.to_string()));
                    }
                }
                let val_obj = Value::Object(obj);
                if let Some(processed) = crate::process::process_input(&val_obj, config) {
                    formatter.write_record(&processed)?;
                    if !config.buffered {
                        formatter.flush()?;
                    }
                }
            }
            Err(e) => {
                eprintln!("Error reading CSV record: {}", e);
            }
        }
    }

    formatter.write_footer()?;
    Ok(())
}
