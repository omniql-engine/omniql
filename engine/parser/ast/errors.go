package ast

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
	maxDistance := 3 // Only suggest if within 3 edits
	
	// Check operations
	for op := range mapping.OperationGroups {
		dist := levenshtein(unknown, op)
		if dist < bestDistance && dist <= maxDistance {
			bestDistance = dist
			bestMatch = op
		}
	}
	
	// Check clauses
	for clause := range mapping.QueryClauses {
		dist := levenshtein(unknown, clause)
		if dist < bestDistance && dist <= maxDistance {
			bestDistance = dist
			bestMatch = clause
		}
	}
	
	// Check operators (first database only - they're mostly the same)
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