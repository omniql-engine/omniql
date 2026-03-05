package mapping

import (
	"encoding/json"
	"strings"
)

// Wire protocol DDL commands — real commands as they arrive over the network
// Used by: proxy/protocol/*.go (wire protocol interception)
//          api/admin_query.go (raw query DDL detection)
//
// NOTE: mapping/operations.go has OQL builder function names (e.g. "createCollection")
//       This file has actual wire protocol names (e.g. "create")
//       They serve different purposes — do not merge

// MongoDDLCommands — actual MongoDB wire protocol command names
var MongoDDLCommands = map[string]bool{
	"create":           true,
	"drop":             true,
	"createindexes":    true,
	"dropindexes":      true,
	"dropcollection":   true,
	"renamecollection": true,
	"dropdatabase":     true,
	"collmod":          true,
}

// IsDDLQuery checks if a raw query is DDL based on database type
func IsDDLQuery(query string, dbType string) bool {
	if dbType == "MongoDB" {
		return isMongoCommandDDL(query)
	}
	return isSQLDDL(query)
}

// isMongoCommandDDL checks if a MongoDB JSON command is DDL
func isMongoCommandDDL(query string) bool {
	var cmd map[string]interface{}
	if err := json.Unmarshal([]byte(query), &cmd); err != nil {
		return false
	}
	for key := range cmd {
		if MongoDDLCommands[key] {
			return true
		}
	}
	return false
}

// isSQLDDL checks if a SQL query starts with any DDL keyword
// Derives from OperationGroups — no hardcoded SQL keywords
func isSQLDDL(query string) bool {
	upper := strings.ToUpper(strings.TrimSpace(query))
	for op, group := range OperationGroups {
		if group == "DDL" && strings.HasPrefix(upper, op+" ") {
			return true
		}
	}
	return false
}