package mapping

// SupportedDatabases lists all databases that TenantsDB supports
// Users must use these exact names in their database_type field
var SupportedDatabases = []string{
	"PostgreSQL",
	"MySQL",
	"QuestDB",
	"MongoDB",
	"Redis",
}

// IsSupportedDatabase checks if a database type is supported
func IsSupportedDatabase(dbType string) bool {
	for _, db := range SupportedDatabases {
		if db == dbType {
			return true
		}
	}
	return false
}