package main

import (
	"regexp"
)

const DEFAULT_MATCH_RULE = "all"
const DEFAULT_INPUT_FORMAT = "yaml"
const DEFAULT_OUTPUT_FORMAT = "yaml"

type OutputMap map[string]any

// Config represents the configuration as defined in YAML.
type Config struct {
	MatchRule       string               `yaml:"match-rule"`
	CommonOutput    []OutputMap          `yaml:"common-output"`
	SpecificOutputs []SpecificOutputRule `yaml:"specific-outputs"`
	InputFormat     string
	OutputFormat    string
	Buffered        bool
}

func (c *Config) setDefaults() *Config {
	c.MatchRule = DEFAULT_MATCH_RULE
	c.InputFormat = DEFAULT_INPUT_FORMAT
	c.OutputFormat = DEFAULT_OUTPUT_FORMAT
	return c
}

// AndCondition represents one condition in a rule's "and" list.
type AndCondition struct {
	Field   string  `yaml:"field"`
	Eq      *string `yaml:"eq,omitempty"`
	Matches *string `yaml:"matches,omitempty"`
}

// Check returns true if the condition holds for the given record.
func (ac *AndCondition) Check(record map[string]any) bool {
	val := getValueByPath(record, ac.Field)
	strVal, ok := val.(string)
	if !ok {
		return false
	}
	if ac.Eq != nil {
		return strVal == *ac.Eq
	}
	if ac.Matches != nil {
		re, err := regexp.Compile(*ac.Matches)
		if err != nil {
			return false
		}
		return re.MatchString(strVal)
	}
	return false
}

// SpecificOutputRule represents one specific rule.
type SpecificOutputRule struct {
	Field   string         `yaml:"field"`
	Eq      *string        `yaml:"eq,omitempty"`
	Matches *string        `yaml:"matches,omitempty"`
	And     []AndCondition `yaml:"and,omitempty"`
	Output  []OutputMap    `yaml:"output"`
}

// Check returns true if the rule matches the given record.
func (r *SpecificOutputRule) Check(record map[string]any) bool {
	val := getValueByPath(record, r.Field)
	strVal, ok := val.(string)
	if !ok {
		return false
	}
	if r.Eq != nil {
		if strVal != *r.Eq {
			return false
		}
	}
	if r.Matches != nil {
		re, err := regexp.Compile(*r.Matches)
		if err != nil {
			return false
		}
		if !re.MatchString(strVal) {
			return false
		}
	}
	// Check each "and" condition.
	for _, ac := range r.And {
		if !ac.Check(record) {
			return false
		}
	}
	return true
}

// FieldMapping is a helper type for storing a mapping key and its definition.
type FieldMapping struct {
	Key    string
	Output any
}

// convertFieldMappings converts a slice of one-key maps into a slice of FieldMapping.
func convertFieldMappings(maps []OutputMap) []FieldMapping {
	var result []FieldMapping
	for _, m := range maps {
		for k, v := range m {
			result = append(result, FieldMapping{Key: k, Output: v})
		}
	}
	return result
}
