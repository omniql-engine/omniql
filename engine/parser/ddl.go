package parser

import (
	"fmt"
	"strings"
	"github.com/omniql-engine/omniql/engine/models"
)

// ddlParsers maps DDL operations to their parser functions
// Fully dynamic - no switch statement needed!
var ddlParsers = map[string]func([]string) (*models.Query, error){
	"CREATE TABLE":      ParseCreateTable,
	"ALTER TABLE":       ParseAlterTable,
	"DROP TABLE":        ParseDropTable,
	"TRUNCATE TABLE":    ParseTruncateTable,
	"TRUNCATE":          ParseTruncateTable,
	"CREATE INDEX":      ParseCreateIndex,
	"DROP INDEX":        ParseDropIndex,
	"CREATE COLLECTION": ParseCreateCollection,
	"DROP COLLECTION":   ParseDropCollection,
	"CREATE DATABASE":   ParseCreateDatabase,
	"DROP DATABASE":     ParseDropDatabase,
	"CREATE VIEW":       ParseCreateView,
	"DROP VIEW":         ParseDropView,
	"ALTER VIEW":        ParseAlterView,
	"RENAME TABLE":      ParseRenameTable,
}

// parseDDL routes DDL operations to specific parsers using function map
func parseDDL(operation string, parts []string) (*models.Query, error) {
	parser, exists := ddlParsers[operation]
	if !exists {
		return nil, fmt.Errorf("unknown DDL operation: %s", operation)
	}
	return parser(parts)
}

// ParseCreateTable handles: CREATE TABLE users WITH id:AUTO,name:STRING(100),email:STRING(255):UNIQUE,age:INTEGER
// Supports constraints: UNIQUE, NOT_NULL, PRIMARY_KEY, etc.
func ParseCreateTable(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "CREATE TABLE",
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("CREATE TABLE requires table name")
	}
	
	query.Entity = parts[entityIndex]
	
	// Check for WITH clause
	withIndex := findKeyword(parts, "WITH")
	if withIndex != -1 {
		// Join remaining parts and parse column definitions
		// Format: id:AUTO,name:STRING(100),email:STRING(255):UNIQUE:NOT_NULL
		fieldsStr := strings.Join(parts[withIndex+1:], " ")
		fieldPairs := strings.Split(fieldsStr, ",")
		
		for _, pair := range fieldPairs {
			// Split by colon: [name, type, constraint1, constraint2, ...]
			colParts := strings.Split(strings.TrimSpace(pair), ":")
			
			if len(colParts) >= 2 {
				field := models.Field{
					Name:  strings.TrimSpace(colParts[0]),
					Value: strings.TrimSpace(colParts[1]),
				}
				
				// ✅ FIX: Extract constraints (parts 2+)
				// email:STRING(255):UNIQUE:NOT_NULL → constraints = ["UNIQUE", "NOT_NULL"]
				if len(colParts) > 2 {
					for _, constraint := range colParts[2:] {
						trimmed := strings.TrimSpace(constraint)
						if trimmed != "" {
							field.Constraints = append(field.Constraints, trimmed)
						}
					}
				}
				
				query.Fields = append(query.Fields, field)
			}
		}
	}
	
	return query, nil
}

// ParseAlterTable handles: ALTER TABLE products ADD_COLUMN:description:TEXT
func ParseAlterTable(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "ALTER TABLE",
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("ALTER TABLE requires table name")
	}
	
	query.Entity = parts[entityIndex]
	
	// Parse alter operation (parts[entityIndex+1]: ADD_COLUMN:description:TEXT or RENAME_COLUMN:old:new)
	if len(parts) > entityIndex+1 {
		alterStr := strings.Join(parts[entityIndex+1:], " ")
		alterParts := strings.Split(alterStr, ":")
		
		if len(alterParts) >= 2 {
			query.Conditions = []models.Condition{
				{
					Field: alterParts[0],                    // Operation: ADD_COLUMN, DROP_COLUMN, etc.
					Value: strings.Join(alterParts[1:], ":"), // Rest: description:TEXT or old:new
				},
			}
		}
	}
	
	return query, nil
}

// ParseDropTable handles: DROP TABLE products
func ParseDropTable(parts []string) (*models.Query, error) {
	entityIndex := getEntityIndex("DROP TABLE")
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("DROP TABLE requires table name")
	}
	
	return &models.Query{
		Operation: "DROP TABLE",
		Entity:    parts[entityIndex],
	}, nil
}

// ParseTruncateTable handles: TRUNCATE TABLE products
func ParseTruncateTable(parts []string) (*models.Query, error) {
	// Note: TRUNCATE can be 1 or 2 words depending on database
	operation := "TRUNCATE TABLE"
	if len(parts) >= 2 && strings.ToUpper(parts[1]) != "TABLE" {
		operation = "TRUNCATE"
	}
	
	entityIndex := getEntityIndex(operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("TRUNCATE TABLE requires table name")
	}
	
	return &models.Query{
		Operation: operation,
		Entity:    parts[entityIndex],
	}, nil
}

// ParseCreateIndex handles: CREATE INDEX products idx_name:name
// ParseCreateIndex handles: CREATE INDEX products idx_name:name UNIQUE
func ParseCreateIndex(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "CREATE INDEX",
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+2 {
		return nil, fmt.Errorf("CREATE INDEX requires table name and index specification")
	}
	
	query.Entity = parts[entityIndex] // Table name
	
	// Parse index specification: idx_name:column_name
	indexSpec := parts[entityIndex+1]
	indexParts := strings.Split(indexSpec, ":")
	
	if len(indexParts) == 2 {
		field := models.Field{
			Name:  indexParts[0], // Index name
			Value: indexParts[1], // Column name
		}
		
		// Capture modifiers like UNIQUE (parts[entityIndex+2+])
		if len(parts) > entityIndex+2 {
			for _, modifier := range parts[entityIndex+2:] {
				trimmed := strings.TrimSpace(modifier)
				if trimmed != "" {
					field.Constraints = append(field.Constraints, trimmed)
				}
			}
		}
		
		query.Fields = []models.Field{field}
	}
	
	return query, nil
}

// ParseDropIndex handles: DROP INDEX products idx_name
func ParseDropIndex(parts []string) (*models.Query, error) {
	entityIndex := getEntityIndex("DROP INDEX")
	if len(parts) < entityIndex+2 {
		return nil, fmt.Errorf("DROP INDEX requires table name and index name")
	}
	
	return &models.Query{
		Operation: "DROP INDEX",
		Entity:    parts[entityIndex], // Table name
		Fields: []models.Field{
			{Name: parts[entityIndex+1]}, // Index name
		},
	}, nil
}

// ParseCreateCollection handles: CREATE COLLECTION products
func ParseCreateCollection(parts []string) (*models.Query, error) {
	entityIndex := getEntityIndex("CREATE COLLECTION")
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("CREATE COLLECTION requires collection name")
	}
	
	return &models.Query{
		Operation: "CREATE COLLECTION",
		Entity:    parts[entityIndex],
	}, nil
}

// ParseDropCollection handles: DROP COLLECTION products
func ParseDropCollection(parts []string) (*models.Query, error) {
	entityIndex := getEntityIndex("DROP COLLECTION")
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("DROP COLLECTION requires collection name")
	}
	
	return &models.Query{
		Operation: "DROP COLLECTION",
		Entity:    parts[entityIndex],
	}, nil
}

// ============================================================================
// NEW: DATABASE OPERATIONS
// ============================================================================

// ParseCreateDatabase handles: CREATE DATABASE mydb
func ParseCreateDatabase(parts []string) (*models.Query, error) {
	entityIndex := getEntityIndex("CREATE DATABASE")
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("CREATE DATABASE requires database name")
	}
	
	return &models.Query{
		Operation:    "CREATE DATABASE",
		DatabaseName: parts[entityIndex],
	}, nil
}

// ParseDropDatabase handles: DROP DATABASE mydb
func ParseDropDatabase(parts []string) (*models.Query, error) {
	entityIndex := getEntityIndex("DROP DATABASE")
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("DROP DATABASE requires database name")
	}
	
	return &models.Query{
		Operation:    "DROP DATABASE",
		DatabaseName: parts[entityIndex],
	}, nil
}

// ============================================================================
// NEW: VIEW OPERATIONS
// ============================================================================

// ParseCreateView handles: CREATE VIEW active_users AS GET User WHERE status = active
// Views are virtual tables based on SELECT queries
func ParseCreateView(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "CREATE VIEW",
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("CREATE VIEW requires view name")
	}
	
	query.ViewName = parts[entityIndex]
	
	// Find AS keyword
	asIndex := findKeyword(parts, "AS")
	if asIndex == -1 {
		return nil, fmt.Errorf("CREATE VIEW requires AS clause with query definition")
	}
	
	// Everything after AS is the view definition query
	// It should be a valid OQL query (usually GET with WHERE)
	viewQueryParts := parts[asIndex+1:]
	if len(viewQueryParts) == 0 {
		return nil, fmt.Errorf("CREATE VIEW AS clause requires query definition")
	}
	
	// Store the view query as a string
	// The translator will parse this as a nested query
	query.ViewQuery = strings.Join(viewQueryParts, " ")
	
	return query, nil
}

// ParseDropView handles: DROP VIEW active_users
func ParseDropView(parts []string) (*models.Query, error) {
	entityIndex := getEntityIndex("DROP VIEW")
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("DROP VIEW requires view name")
	}
	
	return &models.Query{
		Operation: "DROP VIEW",
		ViewName:  parts[entityIndex],
	}, nil
}

// ParseAlterView handles: ALTER VIEW active_users AS GET User WHERE status = active AND age > 18
// In PostgreSQL: CREATE OR REPLACE VIEW
// In MySQL: ALTER VIEW
// In SQLite: DROP + CREATE (not true ALTER)
func ParseAlterView(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "ALTER VIEW",
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("ALTER VIEW requires view name")
	}
	
	query.ViewName = parts[entityIndex]
	
	// Find AS keyword
	asIndex := findKeyword(parts, "AS")
	if asIndex == -1 {
		return nil, fmt.Errorf("ALTER VIEW requires AS clause with new query definition")
	}
	
	// Everything after AS is the new view definition
	viewQueryParts := parts[asIndex+1:]
	if len(viewQueryParts) == 0 {
		return nil, fmt.Errorf("ALTER VIEW AS clause requires query definition")
	}
	
	query.ViewQuery = strings.Join(viewQueryParts, " ")
	
	return query, nil
}

// ============================================================================
// NEW: RENAME OPERATIONS
// ============================================================================

// ParseRenameTable handles: RENAME TABLE old_users TO new_users
func ParseRenameTable(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "RENAME TABLE",
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("RENAME TABLE requires table name")
	}
	
	query.Entity = parts[entityIndex] // Old table name
	
	// Find TO keyword
	toIndex := findKeyword(parts, "TO")
	if toIndex == -1 || toIndex+1 >= len(parts) {
		return nil, fmt.Errorf("RENAME TABLE requires TO clause with new name")
	}
	
	query.NewName = parts[toIndex+1] // New table name
	
	return query, nil
}