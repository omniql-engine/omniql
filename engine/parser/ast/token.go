package ast

// TokenType represents the category of a token
type TokenType int

const (
	TOKEN_UNKNOWN    TokenType = iota
	TOKEN_OPERATION            // GET, CREATE, UPDATE... (from mapping.OperationGroups)
	TOKEN_CLAUSE               // WHERE, HAVING... (from mapping.QueryClauses)
	TOKEN_OPERATOR             // =, >, <, IN... (from mapping.OperatorMap)
	TOKEN_IDENTIFIER           // User, age, name (table/field names)
	TOKEN_STRING               // 'John', "hello"
	TOKEN_NUMBER               // 25, 3.14
	TOKEN_BOOLEAN              // true, false
	TOKEN_LPAREN               // (
	TOKEN_RPAREN               // )
	TOKEN_LBRACKET             // [  [
    TOKEN_RBRACKET             // [  ]
	TOKEN_LBRACE 		 	   // {
	TOKEN_RBRACE			   // }
	TOKEN_COMMA                // ,
	TOKEN_COLON                // :   
    TOKEN_DOT 				   // .	
	TOKEN_EQUALS               // = (for assignments: name = John)
	TOKEN_EOF                  // End of input
	TOKEN_BACKSLASH			   // \
	TOKEN_SEMICOLON		       // ;
)

// Token represents a single token with position info
type Token struct {
	Type     TokenType
	Value    string // Original value
	Position int    // Character position in input
	Line     int    // Line number (1-indexed)
	Column   int    // Column number (1-indexed)
}

// String returns human-readable token type name
func (t TokenType) String() string {
	names := []string{
		"UNKNOWN",
		"OPERATION",
		"CLAUSE",
		"OPERATOR",
		"IDENTIFIER",
		"STRING",
		"NUMBER",
		"BOOLEAN",
		"LPAREN",
		"RPAREN",
		"LBRACKET",   
    	"RBRACKET", 
		"LBRACE",
		"RBRACE", 
		"COMMA",
		"COLON",
    	"DOT",
		"EQUALS",
		"EOF",
		"BACKSLASH",
		"SEMICOLON",
	}
	if int(t) < len(names) {
		return names[t]
	}
	return "UNKNOWN"
}