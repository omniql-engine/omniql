package validator

import (
	"fmt"
)

// Validator validates translated queries before execution
type Validator interface {
	Validate(query string) error
	ValidateWithDetails(query string) (*ValidationResult, error)
}

// ValidationResult contains detailed validation info
type ValidationResult struct {
	Valid       bool
	Error       string
	Suggestion  string
	Position    int      // Character position of error
	NearText    string   // Text near the error
}

// ValidateSQL validates SQL/command based on database type
func ValidateSQL(query string, dbType string) error {
	switch dbType {
	case "PostgreSQL":
		return ValidatePostgreSQL(query)
	case "MySQL":
		return ValidateMySQL(query)
	case "MongoDB":
		return ValidateMongoDB(query)
	case "Redis":
		return ValidateRedis(query)
	default:
		return fmt.Errorf("unsupported database type: %s", dbType)
	}
}

// ValidateSQLWithDetails returns detailed validation result
func ValidateSQLWithDetails(query string, dbType string) (*ValidationResult, error) {
	switch dbType {
	case "PostgreSQL":
		return ValidatePostgreSQLWithDetails(query)
	case "MySQL":
		return ValidateMySQLWithDetails(query)
	case "MongoDB":
		return ValidateMongoDBWithDetails(query)
	case "Redis":
		return ValidateRedisWithDetails(query)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}
}