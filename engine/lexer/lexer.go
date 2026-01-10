package lexer

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/omniql-engine/omniql/mapping"
)

// Tokenizer converts input string to tokens
type Tokenizer struct {
	input   string
	pos     int
	line    int
	column  int
	tokens  []Token
}

// Tokenize converts OQL string to tokens, validating against mapping
func Tokenize(input string) ([]Token, error) {
	t := &Tokenizer{
		input:  input,
		pos:    0,
		line:   1,
		column: 1,
	}
	return t.tokenize()
}

func (t *Tokenizer) tokenize() ([]Token, error) {
	for t.pos < len(t.input) {
		// Skip whitespace
		if t.skipWhitespace() {
			continue
		}
		
		ch := t.input[t.pos]
		
		// Single character tokens
		switch ch {
		case '(':
			t.addToken(TOKEN_LPAREN, "(")
			t.advance()
			continue
		case ')':
			t.addToken(TOKEN_RPAREN, ")")
			t.advance()
			continue
		case ',':
			t.addToken(TOKEN_COMMA, ",")
			t.advance()
			continue
		case '[':                          
			t.addToken(TOKEN_LBRACKET, "[")
			t.advance()
			continue
		case ']':                          
			t.addToken(TOKEN_RBRACKET, "]")
			t.advance()
			continue
		case '{':                         
			t.addToken(TOKEN_LBRACE, "{")
			t.advance()
			continue
		case '}':                          
			t.addToken(TOKEN_RBRACE, "}")
			t.advance()
			continue
		case ':':
			t.addToken(TOKEN_COLON, ":")
			t.advance()
			continue
		case '.':
			t.addToken(TOKEN_DOT, ".")
			t.advance()
			continue
		case '\\':
			t.addToken(TOKEN_BACKSLASH, "\\")
			t.advance()
			continue
		case ';':
			t.addToken(TOKEN_SEMICOLON, ";")
			t.advance()
			continue
		case '$':
			if t.pos+1 < len(t.input) && t.input[t.pos+1] == '$' {
				token, err := t.scanDollarQuote()
				if err != nil {
					return nil, err
				}
				t.tokens = append(t.tokens, token)
				continue
			}
			return nil, &ParseError{
				Message:  fmt.Sprintf("unexpected character '%c'", ch),
				Position: t.pos,
				Line:     t.line,
				Column:   t.column,
			}
		case '\'', '"':
			token, err := t.scanString(ch)
			if err != nil {
				return nil, err
			}
			t.tokens = append(t.tokens, token)
			continue
		}
		
		// Multi-character tokens
		if unicode.IsLetter(rune(ch)) || ch == '_' {
			token, err := t.scanWord()
			if err != nil {
				return nil, err
			}
			t.tokens = append(t.tokens, token)
			continue
		}
		
		if unicode.IsDigit(rune(ch)) || (ch == '-' && t.peekDigit() && t.canStartNegativeNumber()) {
			token := t.scanNumber()
			t.tokens = append(t.tokens, token)
			continue
		}
		
		// Operators: =, !=, <>, >, <, >=, <=
		if isOperatorChar(ch) {
			token, err := t.scanOperator()
			if err != nil {
				return nil, err
			}
			t.tokens = append(t.tokens, token)
			continue
		}
		
		// Unknown character
		return nil, &ParseError{
			Message:  fmt.Sprintf("unexpected character '%c'", ch),
			Position: t.pos,
			Line:     t.line,
			Column:   t.column,
		}
	}
	
	// Add EOF token
	t.addToken(TOKEN_EOF, "")
	
	return t.tokens, nil
}

func (t *Tokenizer) skipWhitespace() bool {
	skipped := false
	for t.pos < len(t.input) {
		ch := t.input[t.pos]
		if ch == ' ' || ch == '\t' {
			t.column++
			t.pos++
			skipped = true
		} else if ch == '\n' {
			t.line++
			t.column = 1
			t.pos++
			skipped = true
		} else if ch == '\r' {
			t.pos++
			skipped = true
		} else {
			break
		}
	}
	return skipped
}

func (t *Tokenizer) advance() {
	t.pos++
	t.column++
}

func (t *Tokenizer) peekDigit() bool {
	if t.pos+1 < len(t.input) {
		return unicode.IsDigit(rune(t.input[t.pos+1]))
	}
	return false
}

// ADD THIS METHOD:
// canStartNegativeNumber checks if current position can start a negative number
// Returns true only if previous token is an operator, '(', ',', '[', or start of input
func (t *Tokenizer) canStartNegativeNumber() bool {
	if len(t.tokens) == 0 {
		return true // Start of input
	}
	
	lastToken := t.tokens[len(t.tokens)-1]
	switch lastToken.Type {
	case TOKEN_OPERATOR, TOKEN_EQUALS, TOKEN_LPAREN, TOKEN_COMMA, TOKEN_LBRACKET, TOKEN_CLAUSE:
		return true
	}
	return false
}

func (t *Tokenizer) addToken(tokenType TokenType, value string) {
	t.tokens = append(t.tokens, Token{
		Type:     tokenType,
		Value:    value,
		Position: t.pos,
		Line:     t.line,
		Column:   t.column,
	})
}

func (t *Tokenizer) scanString(quote byte) (Token, error) {
	startPos := t.pos
	startLine := t.line
	startCol := t.column
	
	t.advance() // Skip opening quote
	
	var value strings.Builder
	for t.pos < len(t.input) {
		ch := t.input[t.pos]
		
		if ch == '\\' && t.pos+1 < len(t.input) {
			// Escape sequence
			t.advance()
			switch t.input[t.pos] {
			case 'n':
				value.WriteByte('\n')
			case 't':
				value.WriteByte('\t')
			case 'r':
				value.WriteByte('\r')
			case '\\':
				value.WriteByte('\\')
			case '\'':
				value.WriteByte('\'')
			case '"':
				value.WriteByte('"')
			default:
				value.WriteByte(t.input[t.pos])
			}
			t.advance()
			continue
		}
		
		if ch == quote {
			t.advance() // Skip closing quote
			return Token{
				Type:     TOKEN_STRING,
				Value:    value.String(),
				Position: startPos,
				Line:     startLine,
				Column:   startCol,
			}, nil
		}
		
		value.WriteByte(ch)
		t.advance()
	}
	
	return Token{}, &ParseError{
		Message:  fmt.Sprintf("unclosed string, expected %c", quote),
		Position: startPos,
		Line:     startLine,
		Column:   startCol,
	}
}

func (t *Tokenizer) scanDollarQuote() (Token, error) {
	startPos := t.pos
	startLine := t.line
	startCol := t.column
	
	t.advance() // Skip first $
	t.advance() // Skip second $
	
	var value strings.Builder
	for t.pos < len(t.input) {
		if t.input[t.pos] == '$' && t.pos+1 < len(t.input) && t.input[t.pos+1] == '$' {
			t.advance() // Skip first $
			t.advance() // Skip second $
			return Token{
				Type:     TOKEN_STRING,
				Value:    value.String(),
				Position: startPos,
				Line:     startLine,
				Column:   startCol,
			}, nil
		}
		
		if t.input[t.pos] == '\n' {
			t.line++
			t.column = 0
		}
		value.WriteByte(t.input[t.pos])
		t.advance()
	}
	
	return Token{}, &ParseError{
		Message:  "unclosed dollar-quoted string, expected $$",
		Position: startPos,
		Line:     startLine,
		Column:   startCol,
	}
}

func (t *Tokenizer) scanNumber() Token {
	startPos := t.pos
	startCol := t.column
	
	var value strings.Builder
	
	// Handle negative
	if t.input[t.pos] == '-' {
		value.WriteByte('-')
		t.advance()
	}
	
	// Integer part
	for t.pos < len(t.input) && unicode.IsDigit(rune(t.input[t.pos])) {
		value.WriteByte(t.input[t.pos])
		t.advance()
	}
	
	// Decimal part
	if t.pos < len(t.input) && t.input[t.pos] == '.' {
		value.WriteByte('.')
		t.advance()
		for t.pos < len(t.input) && unicode.IsDigit(rune(t.input[t.pos])) {
			value.WriteByte(t.input[t.pos])
			t.advance()
		}
	}
	
	return Token{
		Type:     TOKEN_NUMBER,
		Value:    value.String(),
		Position: startPos,
		Line:     t.line,
		Column:   startCol,
	}
}

func (t *Tokenizer) scanWord() (Token, error) {
	startPos := t.pos
	startCol := t.column
	
	var value strings.Builder
	for t.pos < len(t.input) {
		ch := t.input[t.pos]
		if unicode.IsLetter(rune(ch)) || unicode.IsDigit(rune(ch)) || ch == '_' || ch == '.' || ch == ':' || ch == '@' {
			value.WriteByte(ch)
			t.advance()
		} else {
			break
		}
	}
	
	word := value.String()
	upper := strings.ToUpper(word)
	
	// Check for multi-word keywords (ORDER BY, GROUP BY, etc.)
	multiWord := t.tryMultiWord(upper)
	if multiWord != "" {
		upper = multiWord
		word = multiWord
	}
	
	// Classify token against mapping
	tokenType, err := t.classifyWord(upper, word)
	if err != nil {
		return Token{}, &ParseError{
			Message:  err.Error(),
			Position: startPos,
			Line:     t.line,
			Column:   startCol,
			Token:    word,
		}
	}
	
	return Token{
		Type:     tokenType,
		Value:    word,
		Position: startPos,
		Line:     t.line,
		Column:   startCol,
	}, nil
}

func (t *Tokenizer) tryMultiWord(firstWord string) string {
	// Save position
	savedPos := t.pos
	savedCol := t.column
	
	// Skip whitespace
	t.skipWhitespace()
	
	if t.pos >= len(t.input) {
		t.pos = savedPos
		t.column = savedCol
		return ""
	}
	
	// Try to read next word
	if !unicode.IsLetter(rune(t.input[t.pos])) {
		t.pos = savedPos
		t.column = savedCol
		return ""
	}
	
	var nextWord strings.Builder
	tempPos := t.pos
	for tempPos < len(t.input) {
		ch := t.input[tempPos]
		if unicode.IsLetter(rune(ch)) || unicode.IsDigit(rune(ch)) || ch == '_' {
			nextWord.WriteByte(ch)
			tempPos++
		} else {
			break
		}
	}
	
	nextWordStr := nextWord.String()
	nextWordUpper := strings.ToUpper(nextWordStr)
	combined := firstWord + " " + nextWordUpper
	
	// Check if combined is a valid operation
	if _, exists := mapping.OperationGroups[combined]; exists {
		// Only consume if second word is actually uppercase (USER vs User)
		// This distinguishes "CREATE USER" (DCL) from "CREATE User" (CRUD)
		if nextWordStr == nextWordUpper {
			t.pos = tempPos
			t.column = savedCol + (tempPos - savedPos)
			return combined
		}
	}
	
	// Check if combined is a valid clause
	if _, exists := mapping.QueryClauses[combined]; exists {
		// Clauses like "ORDER BY" should always match
		if nextWordStr == nextWordUpper {
			t.pos = tempPos
			t.column = savedCol + (tempPos - savedPos)
			return combined
		}
	}
	
	// Not a multi-word keyword, restore position
	t.pos = savedPos
	t.column = savedCol
	return ""
}

func (t *Tokenizer) classifyWord(upper, original string) (TokenType, error) {
	// Check mapping.OperationGroups
	if _, exists := mapping.OperationGroups[upper]; exists {
		return TOKEN_OPERATION, nil
	}
	
	// Check mapping.QueryClauses
	if _, exists := mapping.QueryClauses[upper]; exists {
		return TOKEN_CLAUSE, nil
	}
	
	// Check mapping.OperatorMap (word operators like IN, LIKE, BETWEEN)
	for _, dbOps := range mapping.OperatorMap {
		if _, exists := dbOps[upper]; exists {
			return TOKEN_OPERATOR, nil
		}
	}
	
	// Check for boolean
	if upper == "TRUE" || upper == "FALSE" {
		return TOKEN_BOOLEAN, nil
	}
	
	// Check for logical operators and IS keyword
	if upper == "AND" || upper == "OR" || upper == "NOT" || upper == "IS" {
		return TOKEN_OPERATOR, nil
	}
	
	// Check for NULL
	if upper == "NULL" {
		return TOKEN_IDENTIFIER, nil
	}
	
	// Must be an identifier (table name, field name, value)
	return TOKEN_IDENTIFIER, nil
}

func (t *Tokenizer) scanOperator() (Token, error) {
	startPos := t.pos
	startCol := t.column
	
	var value strings.Builder
	
	// Collect operator characters
	for t.pos < len(t.input) && isOperatorChar(t.input[t.pos]) {
		value.WriteByte(t.input[t.pos])
		t.advance()
	}
	
	op := value.String()

	// Special case: single = is assignment
	if op == "=" {
		return Token{
			Type:     TOKEN_EQUALS,
			Value:    op,
			Position: startPos,
			Line:     t.line,
			Column:   startCol,
		}, nil
	}

	// Arithmetic/special operators - valid without mapping check
	switch op {
	case "*", "%", "+", "-", "/":
		return Token{
			Type:     TOKEN_OPERATOR,
			Value:    op,
			Position: startPos,
			Line:     t.line,
			Column:   startCol,
		}, nil
	}
	
	// Validate operator exists in mapping
	normalized := strings.ReplaceAll(op, " ", "_")
	for _, dbOps := range mapping.OperatorMap {
		if _, exists := dbOps[normalized]; exists {
			return Token{
				Type:     TOKEN_OPERATOR,
				Value:    op,
				Position: startPos,
				Line:     t.line,
				Column:   startCol,
			}, nil
		}
	}
	
	return Token{}, &ParseError{
		Message:  fmt.Sprintf("unknown operator '%s'", op),
		Position: startPos,
		Line:     t.line,
		Column:   startCol,
	}
}

func isOperatorChar(ch byte) bool {
    return ch == '=' || ch == '!' || ch == '<' || ch == '>' || ch == '*' || ch == '%' || ch == '+' || ch == '-' || ch == '/'
}