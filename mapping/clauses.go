package mapping

// ClauseDefinition defines how to extract a clause
type ClauseDefinition struct {
	Keyword    string   // The keyword to search for (e.g., "LIMIT", "ORDER BY")
	Parsers    []string // Which parsers can use this: CRUD, DQL, DDL, TCL, DCL
	ValueType  string   // NUMERIC, STRING, BOOLEAN, FIELD_LIST, CONDITION, NONE
	Terminates bool     // true = ends expression parsing (WHERE, LIMIT), false = part of expression (OVER)
}

// QueryClauses defines all available OQL clauses
// This is the SSOT for clause recognition in the tokenizer
var QueryClauses = map[string]ClauseDefinition{
	// ========== PAGINATION ==========
	"LIMIT": {
		Keyword:    "LIMIT",
		Parsers:    []string{"CRUD", "DQL"},
		ValueType:  "NUMERIC",
		Terminates: true,
	},
	"OFFSET": {
		Keyword:    "OFFSET",
		Parsers:    []string{"CRUD", "DQL"},
		ValueType:  "NUMERIC",
		Terminates: true,
	},

	// ========== FILTERING ==========
	"WHERE": {
		Keyword:    "WHERE",
		Parsers:    []string{"CRUD", "DQL"},
		ValueType:  "CONDITION",
		Terminates: true,
	},
	"HAVING": {
		Keyword:    "HAVING",
		Parsers:    []string{"DQL"},
		ValueType:  "CONDITION",
		Terminates: true,
	},

	// ========== ORDERING & GROUPING ==========
	"ORDER BY": {
		Keyword:    "ORDER BY",
		Parsers:    []string{"CRUD", "DQL"},
		ValueType:  "FIELD_LIST",
		Terminates: true,
	},
	"GROUP BY": {
		Keyword:    "GROUP BY",
		Parsers:    []string{"DQL"},
		ValueType:  "FIELD_LIST",
		Terminates: true,
	},
	"DISTINCT": {
		Keyword:    "DISTINCT",
		Parsers:    []string{"CRUD", "DQL"},
		ValueType:  "BOOLEAN",
		Terminates: false,
	},

	// ========== FIELD ASSIGNMENTS ==========
	"WITH": {
		Keyword:    "WITH",
		Parsers:    []string{"CRUD", "DQL", "DCL"},
		ValueType:  "FIELD_LIST",
		Terminates: false,
	},
	"SET": {
		Keyword:    "SET",
		Parsers:    []string{"CRUD", "TCL"},
		ValueType:  "FIELD_LIST",
		Terminates: false,
	},

	// ========== JOIN & CONFLICT ==========
	"ON": {
		Keyword:    "ON",
		Parsers:    []string{"CRUD", "DQL", "DCL"},
		ValueType:  "CONDITION",
		Terminates: false,
	},
	"COLUMNS": {
		Keyword:    "COLUMNS",
		Parsers:    []string{"DQL"},
		ValueType:  "FIELD_LIST",
		Terminates: false,
	},

	// ========== ALIASES & DEFINITIONS ==========
	"AS": {
		Keyword:    "AS",
		Parsers:    []string{"DDL", "DQL"},
		ValueType:  "STRING",
		Terminates: false,
	},

	// ========== WINDOW FUNCTIONS ==========
	"OVER": {
		Keyword:    "OVER",
		Parsers:    []string{"DQL"},
		ValueType:  "NONE",
		Terminates: false,
	},
	"PARTITION BY": {
		Keyword:    "PARTITION BY",
		Parsers:    []string{"DQL"},
		ValueType:  "FIELD_LIST",
		Terminates: false,
	},

	// ========== DCL TARGET ==========
	"TO": {
		Keyword:    "TO",
		Parsers:    []string{"DCL", "DDL", "TCL"},
		ValueType:  "STRING",
		Terminates: false,
	},
	"FROM": {
		Keyword:    "FROM",
		Parsers:    []string{"DCL"},
		ValueType:  "STRING",
		Terminates: false,
	},
}

// ============================================================================
// HELPER FUNCTIONS
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

// IsClause checks if a keyword is a valid clause
func IsClause(keyword string) bool {
	_, exists := QueryClauses[keyword]
	return exists
}

// IsTerminatingClause checks if a clause ends expression parsing
// Terminating clauses (WHERE, ORDER BY, LIMIT, etc.) stop expression collection
// Non-terminating clauses (OVER, PARTITION BY, AS) are part of expression grammar
func IsTerminatingClause(keyword string) bool {
	if def, exists := QueryClauses[keyword]; exists {
		return def.Terminates
	}
	return false
}

// GetClauseValueType returns the expected value type for a clause
func GetClauseValueType(clause string) string {
	if def, exists := QueryClauses[clause]; exists {
		return def.ValueType
	}
	return ""
}