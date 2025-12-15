package reverse

import (
	"errors"
	"fmt"

	"github.com/omniql-engine/omniql/engine/models"
)

// ============================================================================
// ERRORS
// ============================================================================

var (
	ErrNotSupported = errors.New("feature not supported in OQL")
	ErrParseError   = errors.New("failed to parse query")
	ErrEmptyQuery   = errors.New("empty query")
)

// ============================================================================
// MAIN INTERFACE - Returns TrueAST models.Query
// ============================================================================

// ToQuery converts native query to models.Query (100% TrueAST)
func ToQuery(query string, dbType string) (*models.Query, error) {
	if query == "" {
		return nil, ErrEmptyQuery
	}

	switch dbType {
	case "PostgreSQL":
		return PostgreSQLToQuery(query)
	case "MySQL":
		return MySQLToQuery(query)
	case "MongoDB":
		return MongoDBToQuery(query)
	case "Redis":
		return RedisToQuery(query)
	default:
		return nil, fmt.Errorf("%w: unsupported database %s", ErrNotSupported, dbType)
	}
}