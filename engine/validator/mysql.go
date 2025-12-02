package validator

import (
	"github.com/xwb1989/sqlparser"
)

// ValidateMySQL validates MySQL SQL syntax
func ValidateMySQL(query string) error {
	_, err := sqlparser.Parse(query)
	return err
}

// ValidateMySQLWithDetails returns detailed validation result
func ValidateMySQLWithDetails(query string) (*ValidationResult, error) {
	_, err := sqlparser.Parse(query)
	if err != nil {
		return &ValidationResult{
			Valid: false,
			Error: err.Error(),
		}, nil
	}

	return &ValidationResult{Valid: true}, nil
}