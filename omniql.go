package oql

import (
	"strings"

	"github.com/omniql-engine/omniql/engine/models"
	"github.com/omniql-engine/omniql/engine/parser"
)

// Parse handles OmniQL queries with : prefix
// Returns:
//   - query: parsed AST (nil if not OmniQL)
//   - isOQL: true if input had : prefix
//   - error: parsing error (nil if success or not OmniQL)
func Parse(input string) (*models.Query, bool, error) {
	if !strings.HasPrefix(input, ":") {
		return nil, false, nil // Not OmniQL, pass through as native
	}

	query, err := parser.Parse(strings.TrimPrefix(input, ":"))
	if err != nil {
		return nil, true, err // Is OmniQL, but failed to parse
	}

	return query, true, nil
}