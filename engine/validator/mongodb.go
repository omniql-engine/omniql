package validator

import (
	"encoding/json"
	"fmt"
)

// ValidateMongoDB validates MongoDB command/document syntax
func ValidateMongoDB(query string) error {
	var doc interface{}
	return json.Unmarshal([]byte(query), &doc)
}

// ValidateMongoDBWithDetails returns detailed validation result
func ValidateMongoDBWithDetails(query string) (*ValidationResult, error) {
	var doc interface{}
	err := json.Unmarshal([]byte(query), &doc)
	if err != nil {
		jsonErr, ok := err.(*json.SyntaxError)
		if ok {
			return &ValidationResult{
				Valid:    false,
				Error:    err.Error(),
				Position: int(jsonErr.Offset),
			}, nil
		}
		return &ValidationResult{
			Valid: false,
			Error: err.Error(),
		}, nil
	}

	return &ValidationResult{Valid: true}, nil
}

// ValidateMongoDBDocument validates a BSON document map
func ValidateMongoDBDocument(doc map[string]interface{}) error {
	if doc == nil {
		return fmt.Errorf("document is nil")
	}
	return nil
}