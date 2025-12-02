package validator

import (
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

// ValidatePostgreSQL validates PostgreSQL SQL syntax
func ValidatePostgreSQL(query string) error {
	_, err := pg_query.Parse(query)
	return err
}

// ValidatePostgreSQLWithDetails returns detailed validation result
func ValidatePostgreSQLWithDetails(query string) (*ValidationResult, error) {
	_, err := pg_query.Parse(query)
	if err != nil {
		return &ValidationResult{
			Valid: false,
			Error: err.Error(),
		}, nil
	}

	return &ValidationResult{Valid: true}, nil
}