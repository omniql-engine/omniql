package validator

import (
	"fmt"
	"strings"

	"github.com/omniql-engine/omniql/mapping"
)

// ValidateRedis validates Redis command syntax
func ValidateRedis(query string) error {
	query = strings.TrimSpace(query)
	if query == "" {
		return fmt.Errorf("empty Redis command")
	}

	parts := strings.Fields(query)
	command := strings.ToUpper(parts[0])

	if !isValidRedisCommand(command) {
		return fmt.Errorf("unknown Redis command: %s", command)
	}

	return nil
}

// ValidateRedisWithDetails returns detailed validation result
func ValidateRedisWithDetails(query string) (*ValidationResult, error) {
	err := ValidateRedis(query)
	if err != nil {
		return &ValidationResult{
			Valid: false,
			Error: err.Error(),
		}, nil
	}

	return &ValidationResult{Valid: true}, nil
}

// isValidRedisCommand checks if command exists in mapping
func isValidRedisCommand(command string) bool {
	redisOps, exists := mapping.OperationMap["Redis"]
	if !exists {
		return false
	}

	for _, translatedCmd := range redisOps {
		if strings.ToUpper(translatedCmd) == command {
			return true
		}
	}

	return false
}