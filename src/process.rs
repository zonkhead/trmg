use serde_json::{Map, Value};
use crate::config::{Config, FieldMapping, convert_field_mappings};
use std::cell::RefCell;
use std::collections::HashMap;

thread_local! {
    static REGEX_CACHE: RefCell<HashMap<String, regex::Regex>> = RefCell::new(HashMap::new());
}

fn get_cached_regex(pattern: &str) -> Option<regex::Regex> {
    REGEX_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(re) = cache.get(pattern) {
            return Some(re.clone());
        }
        if let Ok(re) = regex::Regex::new(pattern) {
            cache.insert(pattern.to_string(), re.clone());
            return Some(re);
        }
        None
    })
}

pub fn get_value_by_path<'a>(record: &'a Value, path: &str) -> Option<&'a Value> {
    let (val, _) = lookup_value_by_path(record, path);
    val
}

pub fn lookup_value_by_path<'a>(record: &'a Value, path: &str) -> (Option<&'a Value>, bool) {
    if path.is_empty() {
        return (None, false);
    }
    let parts = path.split('.');
    let mut current = record;
    for part in parts {
        match current {
            Value::Object(map) => {
                if let Some(val) = map.get(part) {
                    current = val;
                } else {
                    return (None, false);
                }
            }
            _ => return (None, false),
        }
    }
    (Some(current), true)
}

pub fn apply_mapping(
    name: &str,
    in_rec: &Value,
    out_map: &mut Map<String, Value>,
    out_spec: &Value,
) {
    match out_spec {
        Value::String(s) => {
            if let (Some(val), true) = lookup_value_by_path(in_rec, s) {
                out_map.insert(name.to_string(), val.clone());
            } else {
                out_map.insert(name.to_string(), Value::String(s.clone()));
            }
        }
        Value::Object(v) => {
            if v.contains_key("src") && v.contains_key("regex") && v.contains_key("value") {
                let src = get_str_val(v, "src");
                let regex_str = get_str_val(v, "regex");

                let re = match get_cached_regex(&regex_str) {
                    Some(r) => r,
                    None => return,
                };

                let src_val = match get_value_by_path(in_rec, &src) {
                    Some(Value::String(s)) => s,
                    _ => return,
                };

                let caps = match re.captures(src_val) {
                    Some(c) => c,
                    None => return,
                };

                let val_template = get_str_val(v, "value");
                if !val_template.is_empty() {
                    let mut result = val_template;
                    for i in 1..caps.len() {
                        if let Some(m) = caps.get(i) {
                            let placeholder = format!("${}", i);
                            result = result.replace(&placeholder, m.as_str());
                        }
                    }
                    out_map.insert(name.to_string(), Value::String(result));
                }
            } else {
                let mut new_out = Map::new();
                for k in v.keys() {
                    if let Some(spec_val) = v.get(k) {
                        apply_mapping(k, in_rec, &mut new_out, spec_val);
                    }
                }
                out_map.insert(name.to_string(), Value::Object(new_out));
            }
        }
        other => {
            out_map.insert(name.to_string(), other.clone());
        }
    }
}

fn get_str_val(map: &Map<String, Value>, key: &str) -> String {
    if let Some(Value::String(s)) = map.get(key) {
        s.clone()
    } else {
        String::new()
    }
}

pub fn apply_field_mappings(
    record: &Value,
    output: &mut Map<String, Value>,
    mappings: &[FieldMapping],
) {
    for fm in mappings {
        apply_mapping(&fm.key, record, output, &fm.output);
    }
}

pub fn process_input(record: &Value, config: &Config) -> Option<Value> {
    let rec_map = match record.as_object() {
        Some(m) => m,
        None => return Some(record.clone()),
    };

    let mut output = if config.clone_original {
        rec_map.clone()
    } else {
        Map::new()
    };

    let common_mappings = convert_field_mappings(&config.common_output);
    apply_field_mappings(record, &mut output, &common_mappings);

    let mut matched_specific = false;
    for rule in &config.specific_outputs {
        if rule.check(record) {
            matched_specific = true;
            let rule_mappings = convert_field_mappings(&rule.output);
            apply_field_mappings(record, &mut output, &rule_mappings);
            break;
        }
    }

    if config.match_rule == "drop-no-match" && !matched_specific {
        return None;
    }

    if !config.clone_original && output.is_empty() {
        return Some(record.clone());
    }

    Some(Value::Object(output))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn must_config(yaml_string: &str) -> Config {
        serde_yaml::from_str(yaml_string).unwrap()
    }

    #[test]
    fn test_process_input() {
        {
            let record = json!({"foo": "bar"});
            let cfg = must_config("
match-rule: all
common-output:
- baz: foo
");
            let got = process_input(&record, &cfg).unwrap();
            let want = json!({"baz": "bar"});
            assert_eq!(got, want);
        }

        {
            let record = json!({"foo": "yes", "val": 123});
            let cfg = must_config("
match-rule: all
common-output:
- baz: foo
specific-outputs:
- field: foo
  eq: yes
  output:
  - extra: val
");
            let got = process_input(&record, &cfg).unwrap();
            let want = json!({"baz": "yes", "extra": 123});
            assert_eq!(got, want);
        }

        {
            let record = json!({"foo": "no"});
            let cfg = must_config("
match-rule: drop-no-match
common-output:
- baz: foo
specific-outputs:
- field: foo
  eq: yes
  output:
  - extra: val
");
            let got = process_input(&record, &cfg);
            assert!(got.is_none());
        }

        {
            let record = json!({"foo": "no", "bar": 1});
            let cfg = must_config("
match-rule: all
common-output: []
specific-outputs:
- field: foo
  eq: yes
  output:
  - extra: val
");
            let got = process_input(&record, &cfg).unwrap();
            assert_eq!(got, record);
        }

        {
            let record = json!({"foo": "yes", "bar": 1, "unmapped": true});
            let cfg = must_config("
clone-original: true
specific-outputs:
- field: foo
  eq: yes
  output:
  - mapped: bar
");
            let got = process_input(&record, &cfg).unwrap();
            let want = json!({"foo": "yes", "bar": 1, "unmapped": true, "mapped": 1});
            assert_eq!(got, want);
        }
    }

    #[test]
    fn test_apply_mapping() {
        {
            let in_rec = json!({"foo": 42});
            let mut out = Map::new();
            apply_mapping("bar", &in_rec, &mut out, &json!("foo"));
            assert_eq!(out.get("bar"), Some(&json!(42)));
        }

        {
            let in_rec = json!({"foo": 42});
            let mut out = Map::new();
            apply_mapping("bar", &in_rec, &mut out, &json!("YES"));
            assert_eq!(out.get("bar"), Some(&json!("YES")));
        }

        {
            let in_rec = json!({"text": "hello-123"});
            let mut out = Map::new();
            let out_spec = json!({
                "src": "text",
                "regex": "hello-(\\d+)",
                "value": "number=$1"
            });
            apply_mapping("result", &in_rec, &mut out, &out_spec);
            assert_eq!(out.get("result"), Some(&json!("number=123")));
        }

        {
            let in_rec = json!({"a": 1, "b": 2});
            let mut out = Map::new();
            let out_spec = json!({
                "x": "a",
                "y": "b"
            });
            apply_mapping("nested", &in_rec, &mut out, &out_spec);
            let nested = out.get("nested").unwrap().as_object().unwrap();
            assert_eq!(nested.get("x"), Some(&json!(1)));
            assert_eq!(nested.get("y"), Some(&json!(2)));
        }
    }

    #[test]
    fn test_get_value_by_path() {
        assert_eq!(get_value_by_path(&json!({"foo": 42}), "foo"), Some(&json!(42)));
        assert_eq!(get_value_by_path(&json!({"foo": {"bar": "baz"}}), "foo.bar"), Some(&json!("baz")));
        assert_eq!(get_value_by_path(&json!({"foo": {"bar": "baz"}}), "foo"), Some(&json!({"bar": "baz"})));
        assert_eq!(get_value_by_path(&json!({"foo": 42}), "bar"), None);
        assert_eq!(get_value_by_path(&json!({"foo": 42}), "foo.bar"), None);
        assert_eq!(get_value_by_path(&json!({"foo": 42}), ""), None);
    }

    #[test]
    fn test_lookup_value_by_path() {
        assert_eq!(lookup_value_by_path(&json!({"foo": 42}), "foo"), (Some(&json!(42)), true));
        assert_eq!(lookup_value_by_path(&json!({"foo": 42}), "bar"), (None, false));
        assert_eq!(lookup_value_by_path(&json!({"foo": null}), "foo"), (Some(&Value::Null), true));
        assert_eq!(lookup_value_by_path(&json!({"foo": 42}), ""), (None, false));
    }
}
