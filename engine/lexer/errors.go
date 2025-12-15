package lexer

import (
	"fmt"
	"strings"

	"github.com/omniql-engine/omniql/mapping"
)

// ParseError represents an error with position info
type ParseError struct {
	Message  string
	Position int
	Line     int
	Column   int
	Token    string
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error at line %d, column %d: %s", e.Line, e.Column, e.Message)
}

// NewParseError creates a new parse error
func NewParseError(token Token, message string) *ParseError {
	return &ParseError{
		Message:  message,
		Position: token.Position,
		Line:     token.Line,
		Column:   token.Column,
		Token:    token.Value,
	}
}

// NewUnknownTokenError creates error with suggestion
func NewUnknownTokenError(token Token) *ParseError {
	suggestion := SuggestSimilar(token.Value)
	msg := fmt.Sprintf("unknown token '%s'", token.Value)
	if suggestion != "" {
		msg += fmt.Sprintf(". Did you mean '%s'?", suggestion)
	}
	return NewParseError(token, msg)
}

// SuggestSimilar finds the closest matching keyword
func SuggestSimilar(unknown string) string {
	unknown = strings.ToUpper(unknown)
	
	var bestMatch string
	bestDistance := 999
	maxDistance := 2 // Only suggest if within 2 edits (tighter matching)
	
	// Check common operators FIRST (highest priority for short keywords like IN)
	commonOperators := []string{"IN", "LIKE", "BETWEEN", "AND", "OR", "NOT"}
	for _, op := range commonOperators {
		dist := levenshtein(unknown, op)
		if dist <= maxDistance && dist < bestDistance {
			bestDistance = dist
			bestMatch = op
		}
	}
	
	// Common operations get priority (checked next, with -1 bonus)
	commonOps := []string{"GET", "CREATE", "UPDATE", "DELETE", "COUNT", "SUM", "AVG", "MIN", "MAX"}
	commonSet := make(map[string]bool)
	for _, op := range commonOps {
		commonSet[op] = true
		dist := levenshtein(unknown, op)
		// Give common ops a -1 bonus to prioritize them
		if dist <= maxDistance && dist-1 < bestDistance {
			bestDistance = dist - 1
			bestMatch = op
		}
	}
	
	// Check remaining operations (no bonus)
	for op := range mapping.OperationGroups {
		if commonSet[op] {
			continue // Already checked with bonus
		}
		dist := levenshtein(unknown, op)
		if dist < bestDistance && dist <= maxDistance {
			bestDistance = dist
			bestMatch = op
		}
	}
	
	// Check clauses (full and first word of two-word clauses)
	for clause := range mapping.QueryClauses {
		// Check full clause
		dist := levenshtein(unknown, clause)
		if dist < bestDistance && dist <= maxDistance {
			bestDistance = dist
			bestMatch = clause
		}
		// Check first word of two-word clauses (ORDER BY -> ORDER, GROUP BY -> GROUP)
		if strings.Contains(clause, " ") {
			firstWord := strings.Split(clause, " ")[0]
			dist = levenshtein(unknown, firstWord)
			if dist < bestDistance && dist <= maxDistance {
				bestDistance = dist
				bestMatch = clause // Suggest full clause
			}
		}
	}
	
	// Check remaining operators (first database only - they're mostly the same)
	for _, dbOps := range mapping.OperatorMap {
		for op := range dbOps {
			dist := levenshtein(unknown, op)
			if dist < bestDistance && dist <= maxDistance {
				bestDistance = dist
				bestMatch = op
			}
		}
		break
	}
	
	return bestMatch
}

// levenshtein calculates edit distance between two strings
func levenshtein(a, b string) int {
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}
	
	// Create matrix
	matrix := make([][]int, len(a)+1)
	for i := range matrix {
		matrix[i] = make([]int, len(b)+1)
		matrix[i][0] = i
	}
	for j := range matrix[0] {
		matrix[0][j] = j
	}
	
	// Fill matrix
	for i := 1; i <= len(a); i++ {
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			matrix[i][j] = min(
				matrix[i-1][j]+1,      // deletion
				matrix[i][j-1]+1,      // insertion
				matrix[i-1][j-1]+cost, // substitution
			)
		}
	}
	
	return matrix[len(a)][len(b)]
}

func min(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}