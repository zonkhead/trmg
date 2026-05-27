use serde::Deserialize;
use serde_json::Value;
use std::sync::OnceLock;

pub type OutputMap = serde_json::Map<String, Value>;

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[serde(default = "default_match_rule")]
    pub match_rule: String,

    #[serde(default)]
    pub clone_original: bool,

    #[serde(default)]
    pub common_output: Vec<OutputMap>,

    #[serde(default)]
    pub specific_outputs: Vec<SpecificOutputRule>,

    #[serde(skip)]
    pub input_format: String,

    #[serde(skip)]
    pub output_format: String,

    #[serde(skip)]
    pub buffered: bool,
}

fn default_match_rule() -> String {
    "all".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct AndCondition {
    #[serde(default)]
    pub field: String,
    pub eq: Option<String>,
    pub matches: Option<String>,
    #[serde(skip)]
    pub regex: OnceLock<Option<regex::Regex>>,
}

impl AndCondition {
    pub fn check(&self, record: &Value) -> bool {
        let val = match crate::process::get_value_by_path(record, &self.field) {
            Some(v) => v,
            None => return false,
        };
        let str_val = match val.as_str() {
            Some(s) => s,
            None => return false,
        };
        if let Some(ref eq) = self.eq {
            return str_val == eq;
        }
        if let Some(ref matches) = self.matches {
            let re_opt = self.regex.get_or_init(|| {
                regex::Regex::new(matches).ok()
            });
            if let Some(re) = re_opt {
                return re.is_match(str_val);
            }
            return false;
        }
        false
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct SpecificOutputRule {
    #[serde(default)]
    pub field: String,
    pub eq: Option<String>,
    pub matches: Option<String>,
    #[serde(default)]
    pub and: Vec<AndCondition>,
    pub output: Vec<OutputMap>,
    #[serde(skip)]
    pub regex: OnceLock<Option<regex::Regex>>,
}

impl SpecificOutputRule {
    pub fn check(&self, record: &Value) -> bool {
        let val = match crate::process::get_value_by_path(record, &self.field) {
            Some(v) => v,
            None => return false,
        };
        let str_val = match val.as_str() {
            Some(s) => s,
            None => return false,
        };
        if let Some(ref eq) = self.eq {
            if str_val != eq {
                return false;
            }
        }
        if let Some(ref matches) = self.matches {
            let re_opt = self.regex.get_or_init(|| {
                regex::Regex::new(matches).ok()
            });
            if let Some(re) = re_opt {
                if !re.is_match(str_val) {
                    return false;
                }
            } else {
                return false;
            }
        }
        for ac in &self.and {
            if !ac.check(record) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone)]
pub struct FieldMapping {
    pub key: String,
    pub output: Value,
}

pub fn convert_field_mappings(maps: &[OutputMap]) -> Vec<FieldMapping> {
    let mut result = Vec::new();
    for m in maps {
        for (k, v) in m {
            result.push(FieldMapping {
                key: k.clone(),
                output: v.clone(),
            });
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_and_condition_check() {
        let active = AndCondition {
            field: "status".to_string(),
            eq: Some("active".to_string()),
            matches: None,
            regex: OnceLock::new(),
        };
        assert!(active.check(&json!({"status": "active"})));
        assert!(!active.check(&json!({"status": "inactive"})));

        let regex_check = AndCondition {
            field: "id".to_string(),
            eq: None,
            matches: Some("^usr_.*".to_string()),
            regex: OnceLock::new(),
        };
        assert!(regex_check.check(&json!({"id": "usr_123"})));
        assert!(!regex_check.check(&json!({"id": "org_123"})));

        let invalid_regex = AndCondition {
            field: "id".to_string(),
            eq: None,
            matches: Some("[invalid".to_string()),
            regex: OnceLock::new(),
        };
        assert!(!invalid_regex.check(&json!({"id": "usr_123"})));

        let missing = AndCondition {
            field: "status".to_string(),
            eq: Some("active".to_string()),
            matches: None,
            regex: OnceLock::new(),
        };
        assert!(!missing.check(&json!({"id": "123"})));

        let non_string = AndCondition {
            field: "count".to_string(),
            eq: Some("1".to_string()),
            matches: None,
            regex: OnceLock::new(),
        };
        assert!(!non_string.check(&json!({"count": 1})));

        let no_conditions = AndCondition {
            field: "status".to_string(),
            eq: None,
            matches: None,
            regex: OnceLock::new(),
        };
        assert!(!no_conditions.check(&json!({"status": "active"})));
    }

    #[test]
    fn test_specific_output_rule_check() {
        let rule_eq = SpecificOutputRule {
            field: "type".to_string(),
            eq: Some("user".to_string()),
            matches: None,
            and: Vec::new(),
            output: Vec::new(),
            regex: OnceLock::new(),
        };
        assert!(rule_eq.check(&json!({"type": "user"})));
        assert!(!rule_eq.check(&json!({"type": "admin"})));

        let rule_matches = SpecificOutputRule {
            field: "email".to_string(),
            eq: None,
            matches: Some(".*@google\\.com$".to_string()),
            and: Vec::new(),
            output: Vec::new(),
            regex: OnceLock::new(),
        };
        assert!(rule_matches.check(&json!({"email": "test@google.com"})));
        assert!(!rule_matches.check(&json!({"email": "test@yahoo.com"})));

        let rule_invalid_regex = SpecificOutputRule {
            field: "email".to_string(),
            eq: None,
            matches: Some("[invalid".to_string()),
            and: Vec::new(),
            output: Vec::new(),
            regex: OnceLock::new(),
        };
        assert!(!rule_invalid_regex.check(&json!({"email": "test@google.com"})));

        let rule_and_passing = SpecificOutputRule {
            field: "type".to_string(),
            eq: Some("user".to_string()),
            and: vec![
                AndCondition {
                    field: "status".to_string(),
                    eq: Some("active".to_string()),
                    matches: None,
                    regex: OnceLock::new(),
                },
                AndCondition {
                    field: "role".to_string(),
                    eq: None,
                    matches: Some("^admin$".to_string()),
                    regex: OnceLock::new(),
                },
            ],
            matches: None,
            output: Vec::new(),
            regex: OnceLock::new(),
        };
        assert!(rule_and_passing.check(&json!({"type": "user", "status": "active", "role": "admin"})));

        let rule_and_failing = SpecificOutputRule {
            field: "type".to_string(),
            eq: Some("user".to_string()),
            and: vec![
                AndCondition {
                    field: "status".to_string(),
                    eq: Some("active".to_string()),
                    matches: None,
                    regex: OnceLock::new(),
                },
            ],
            matches: None,
            output: Vec::new(),
            regex: OnceLock::new(),
        };
        assert!(!rule_and_failing.check(&json!({"type": "user", "status": "inactive"})));

        let rule_missing = SpecificOutputRule {
            field: "type".to_string(),
            eq: Some("user".to_string()),
            matches: None,
            and: Vec::new(),
            output: Vec::new(),
            regex: OnceLock::new(),
        };
        assert!(!rule_missing.check(&json!({"status": "active"})));

        let rule_non_string = SpecificOutputRule {
            field: "count".to_string(),
            eq: Some("1".to_string()),
            matches: None,
            and: Vec::new(),
            output: Vec::new(),
            regex: OnceLock::new(),
        };
        assert!(!rule_non_string.check(&json!({"count": 1})));

        let rule_no_conditions = SpecificOutputRule {
            field: "type".to_string(),
            eq: None,
            matches: None,
            and: Vec::new(),
            output: Vec::new(),
            regex: OnceLock::new(),
        };
        assert!(rule_no_conditions.check(&json!({"type": "user"})));
    }
}
