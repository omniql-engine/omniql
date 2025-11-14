package parser

import (
	"fmt"
	"strings"
	"github.com/omniql-engine/omniql/engine/models"
)

// tclParsers maps TCL operations to their parser functions
var tclParsers = map[string]func([]string) (*models.Query, error){
	"BEGIN":             ParseBegin,
	"START":             ParseBegin,
	"COMMIT":            ParseCommit,
	"ROLLBACK":          ParseRollback,
	"SAVEPOINT":         ParseSavepoint,
	"ROLLBACK TO":       ParseRollbackTo,
	"RELEASE SAVEPOINT": ParseReleaseSavepoint,
	"SET TRANSACTION":   ParseSetTransaction,
}

// parseTCL routes TCL operations to specific parsers using function map
func parseTCL(operation string, parts []string) (*models.Query, error) {
	parser, exists := tclParsers[operation]
	if !exists {
		return nil, fmt.Errorf("unknown TCL operation: %s", operation)
	}
	return parser(parts)
}

// ============================================================================
// TRANSACTION CONTROL OPERATIONS
// ============================================================================

// ParseBegin handles: BEGIN or START
// Starts a new transaction
func ParseBegin(parts []string) (*models.Query, error) {
	return &models.Query{
		Operation: "BEGIN",
		Transaction: &models.Transaction{
			Operation: "BEGIN",
		},
	}, nil
}

// ParseCommit handles: COMMIT
// Commits the current transaction (saves all changes)
func ParseCommit(parts []string) (*models.Query, error) {
	return &models.Query{
		Operation: "COMMIT",
		Transaction: &models.Transaction{
			Operation: "COMMIT",
		},
	}, nil
}

// ParseRollback handles: ROLLBACK or ROLLBACK TO savepoint_name
// Rolls back transaction (undoes all changes or to savepoint)
func ParseRollback(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "ROLLBACK",
		Transaction: &models.Transaction{
			Operation: "ROLLBACK",
		},
	}
	
	// Check if rolling back to a savepoint
	// Format: ROLLBACK TO savepoint_name
	toIndex := findKeyword(parts, "TO")
	if toIndex != -1 && toIndex+1 < len(parts) {
		// This is ROLLBACK TO SAVEPOINT
		savepointName := parts[toIndex+1]
		query.Operation = "ROLLBACK TO"
		query.Transaction.Operation = "ROLLBACK TO"
		query.Transaction.SavepointName = savepointName
	}
	
	return query, nil
}

// ParseSavepoint handles: SAVEPOINT savepoint_name
// Creates a savepoint within a transaction (partial rollback point)
func ParseSavepoint(parts []string) (*models.Query, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("SAVEPOINT requires savepoint name")
	}
	
	savepointName := parts[1]
	
	return &models.Query{
		Operation: "SAVEPOINT",
		Transaction: &models.Transaction{
			Operation:     "SAVEPOINT",
			SavepointName: savepointName,
		},
	}, nil
}

// ParseRollbackTo handles: ROLLBACK_TO savepoint_name
// Rolls back to a specific savepoint
func ParseRollbackTo(parts []string) (*models.Query, error) {
	// Format can be:
	// 1. ROLLBACK_TO sp1 (parts = ["ROLLBACK TO", "sp1"])
	// 2. ROLLBACK TO sp1 (parts = ["ROLLBACK", "TO", "sp1"]) - handled by ParseRollback
	
	var savepointName string
	
	// Look for TO keyword (might be separate or part of ROLLBACK_TO)
	toIndex := findKeyword(parts, "TO")
	if toIndex != -1 && toIndex+1 < len(parts) {
		// Format: ROLLBACK TO sp1
		savepointName = parts[toIndex+1]
	} else if len(parts) >= 2 {
		// Format: ROLLBACK_TO sp1
		savepointName = parts[1]
	} else {
		return nil, fmt.Errorf("ROLLBACK_TO requires savepoint name")
	}
	
	return &models.Query{
		Operation: "ROLLBACK TO",
		Transaction: &models.Transaction{
			Operation:     "ROLLBACK TO",
			SavepointName: savepointName,
		},
	}, nil
}

// ParseReleaseSavepoint handles: RELEASE SAVEPOINT savepoint_name
// Releases a savepoint (removes it from the transaction)
func ParseReleaseSavepoint(parts []string) (*models.Query, error) {
	// Format: RELEASE SAVEPOINT savepoint_name
	// Or: RELEASE savepoint_name
	
	var savepointName string
	
	// Check for SAVEPOINT keyword
	savepointIndex := findKeyword(parts, "SAVEPOINT")
	if savepointIndex != -1 {
		// Format: RELEASE SAVEPOINT savepoint_name
		if savepointIndex+1 >= len(parts) {
			return nil, fmt.Errorf("RELEASE SAVEPOINT requires savepoint name")
		}
		savepointName = parts[savepointIndex+1]
	} else {
		// Format: RELEASE savepoint_name
		if len(parts) < 2 {
			return nil, fmt.Errorf("RELEASE requires savepoint name")
		}
		savepointName = parts[1]
	}
	
	return &models.Query{
		Operation: "RELEASE SAVEPOINT",
		Transaction: &models.Transaction{
			Operation:     "RELEASE SAVEPOINT",
			SavepointName: savepointName,
		},
	}, nil
}

// ParseSetTransaction handles: SET TRANSACTION ISOLATION LEVEL level [READ ONLY|READ WRITE]
// Sets transaction isolation level and read/write mode
func ParseSetTransaction(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "SET TRANSACTION",
		Transaction: &models.Transaction{
			Operation: "SET TRANSACTION",
		},
	}
	
	// Find ISOLATION keyword
	isolationIndex := findKeyword(parts, "ISOLATION")
	if isolationIndex != -1 {
		// Find LEVEL keyword
		levelIndex := findKeyword(parts, "LEVEL")
		if levelIndex == -1 || levelIndex+1 >= len(parts) {
			return nil, fmt.Errorf("SET TRANSACTION ISOLATION requires LEVEL")
		}
		
		// Isolation level could be multi-word: READ UNCOMMITTED, REPEATABLE READ, etc.
		// Collect words until we hit READ (for READ ONLY/WRITE) or end
		var levelParts []string
		i := levelIndex + 1
		for i < len(parts) {
			upper := strings.ToUpper(parts[i])
			if upper == "READ" {
				// Check if this is part of isolation level (READ UNCOMMITTED/COMMITTED)
				// or read mode (READ ONLY/WRITE)
				if i+1 < len(parts) {
					next := strings.ToUpper(parts[i+1])
					if next == "UNCOMMITTED" || next == "COMMITTED" {
						// Part of isolation level
						levelParts = append(levelParts, parts[i], parts[i+1])
						i += 2
						break
					} else if next == "ONLY" || next == "WRITE" {
						// Read mode, not part of isolation level
						break
					}
				}
				break
			} else {
				levelParts = append(levelParts, parts[i])
				i++
			}
		}
		
		if len(levelParts) > 0 {
			query.Transaction.IsolationLevel = strings.ToUpper(strings.Join(levelParts, " "))
		}
	}
	
	// Find READ keyword for read mode
	readIndex := findKeyword(parts, "READ")
	if readIndex != -1 && readIndex+1 < len(parts) {
		mode := strings.ToUpper(parts[readIndex+1])
		if mode == "ONLY" {
			query.Transaction.ReadOnly = true
		} else if mode == "WRITE" {
			query.Transaction.ReadOnly = false
		}
	}
	
	return query, nil
}