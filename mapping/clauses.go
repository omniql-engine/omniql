package mapping

// ClauseDefinition defines how to extract a clause
type ClauseDefinition struct {
	Keyword   string   // The keyword to search for (e.g., "LIMIT", "ORDER BY")
	Parsers   []string // Which parsers can use this: CRUD, DQL, etc.
	ValueType string   // NUMERIC, STRING, BOOLEAN, FIELD_LIST, CONDITION, NONE
}

// QueryClauses defines all available clauses
var QueryClauses = map[string]ClauseDefinition{
	"LIMIT": {
		Keyword:   "LIMIT",
		Parsers:   []string{"CRUD", "DQL"},
		ValueType: "NUMERIC",
	},
	"OFFSET": {
		Keyword:   "OFFSET",
		Parsers:   []string{"CRUD", "DQL"},
		ValueType: "NUMERIC",
	},
	"DISTINCT": {
		Keyword:   "DISTINCT",
		Parsers:   []string{"CRUD", "DQL"},
		ValueType: "BOOLEAN",
	},
	"ORDER BY": {
		Keyword:   "ORDER BY",
		Parsers:   []string{"CRUD", "DQL"},
		ValueType: "FIELD_LIST",
	},
	"GROUP BY": {
		Keyword:   "GROUP BY",
		Parsers:   []string{"DQL"},
		ValueType: "FIELD_LIST",
	},
	"WHERE": {
		Keyword:   "WHERE",
		Parsers:   []string{"CRUD", "DQL"},
		ValueType: "CONDITION",
	},
	"HAVING": {
		Keyword:   "HAVING",
		Parsers:   []string{"DQL"},
		ValueType: "CONDITION",
	},
}

// ============================================================================
// HELPER FUNCTIONS (like in operations.go)
// ============================================================================

// ClausesByParser - reverse mapping built from QueryClauses
var ClausesByParser map[string][]string

func init() {
	ClausesByParser = make(map[string][]string)
	
	for clauseName, def := range QueryClauses {
		for _, parser := range def.Parsers {
			ClausesByParser[parser] = append(ClausesByParser[parser], clauseName)
		}
	}
}

// GetClausesForParser returns clauses available to a specific parser
func GetClausesForParser(parser string) []string {
	return ClausesByParser[parser]
}