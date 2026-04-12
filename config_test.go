package main

import (
	"testing"
)

func ptr[T any](v T) *T {
	return &v
}

func TestAndCondition_Check(t *testing.T) {
	tests := []struct {
		name   string
		ac     AndCondition
		record map[string]any
		want   bool
	}{
		{
			name:   "eq match",
			ac:     AndCondition{Field: "status", Eq: ptr("active")},
			record: map[string]any{"status": "active"},
			want:   true,
		},
		{
			name:   "eq mismatch",
			ac:     AndCondition{Field: "status", Eq: ptr("active")},
			record: map[string]any{"status": "inactive"},
			want:   false,
		},
		{
			name:   "matches match",
			ac:     AndCondition{Field: "id", Matches: ptr("^usr_.*")},
			record: map[string]any{"id": "usr_123"},
			want:   true,
		},
		{
			name:   "matches mismatch",
			ac:     AndCondition{Field: "id", Matches: ptr("^usr_.*")},
			record: map[string]any{"id": "org_123"},
			want:   false,
		},
		{
			name:   "invalid regex",
			ac:     AndCondition{Field: "id", Matches: ptr("[invalid")},
			record: map[string]any{"id": "usr_123"},
			want:   false,
		},
		{
			name:   "field missing",
			ac:     AndCondition{Field: "status", Eq: ptr("active")},
			record: map[string]any{"id": "123"},
			want:   false,
		},
		{
			name:   "field not string",
			ac:     AndCondition{Field: "count", Eq: ptr("1")},
			record: map[string]any{"count": 1},
			want:   false,
		},
		{
			name:   "no conditions set",
			ac:     AndCondition{Field: "status"},
			record: map[string]any{"status": "active"},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ac.Check(tt.record); got != tt.want {
				t.Errorf("AndCondition.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSpecificOutputRule_Check(t *testing.T) {
	tests := []struct {
		name   string
		rule   SpecificOutputRule
		record map[string]any
		want   bool
	}{
		{
			name:   "eq match",
			rule:   SpecificOutputRule{Field: "type", Eq: ptr("user")},
			record: map[string]any{"type": "user"},
			want:   true,
		},
		{
			name:   "eq mismatch",
			rule:   SpecificOutputRule{Field: "type", Eq: ptr("user")},
			record: map[string]any{"type": "admin"},
			want:   false,
		},
		{
			name:   "matches match",
			rule:   SpecificOutputRule{Field: "email", Matches: ptr(".*@google\\.com$")},
			record: map[string]any{"email": "test@google.com"},
			want:   true,
		},
		{
			name:   "matches mismatch",
			rule:   SpecificOutputRule{Field: "email", Matches: ptr(".*@google\\.com$")},
			record: map[string]any{"email": "test@yahoo.com"},
			want:   false,
		},
		{
			name:   "invalid regex",
			rule:   SpecificOutputRule{Field: "email", Matches: ptr("[invalid")},
			record: map[string]any{"email": "test@google.com"},
			want:   false,
		},
		{
			name:   "with and conditions all passing",
			rule:   SpecificOutputRule{
				Field: "type", Eq: ptr("user"),
				And: []AndCondition{
					{Field: "status", Eq: ptr("active")},
					{Field: "role", Matches: ptr("^admin$")},
				},
			},
			record: map[string]any{"type": "user", "status": "active", "role": "admin"},
			want:   true,
		},
		{
			name:   "with and condition failing",
			rule:   SpecificOutputRule{
				Field: "type", Eq: ptr("user"),
				And: []AndCondition{
					{Field: "status", Eq: ptr("active")},
				},
			},
			record: map[string]any{"type": "user", "status": "inactive"},
			want:   false,
		},
		{
			name:   "field missing",
			rule:   SpecificOutputRule{Field: "type", Eq: ptr("user")},
			record: map[string]any{"status": "active"},
			want:   false,
		},
		{
			name:   "field not string",
			rule:   SpecificOutputRule{Field: "count", Eq: ptr("1")},
			record: map[string]any{"count": 1},
			want:   false,
		},
		{
			name:   "no conditions set just field",
			rule:   SpecificOutputRule{Field: "type"},
			record: map[string]any{"type": "user"},
			want:   true,  // because all eq/matches are nil, it passes the base checks
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.rule.Check(tt.record); got != tt.want {
				t.Errorf("SpecificOutputRule.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}
