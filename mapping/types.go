package mapping

// TypeMap - Runtime mapping for schema translators
// Usage: TypeMap["PostgreSQL"]["AUTO"] returns "SERIAL"
// Maps universal type names to database-specific type names
var TypeMap = map[string]map[string]string{
	"PostgreSQL": {
		// Primary Key Types
		"AUTO":      "SERIAL",           // Auto-incrementing integer
		"BIGAUTO":   "BIGSERIAL",        // Auto-incrementing big integer
		
		// Numeric Types
		"INT":       "INTEGER",
		"BIGINT":    "BIGINT",
		"SMALLINT":  "SMALLINT",
		"DECIMAL":   "DECIMAL",
		"NUMERIC":   "NUMERIC",
		"REAL":      "REAL",
		"FLOAT":     "DOUBLE PRECISION",
		
		// String Types
		"STRING":    "VARCHAR",          // Variable length string
		"TEXT":      "TEXT",             // Unlimited text
		"CHAR":      "CHAR",             // Fixed length string
		
		// Boolean
		"BOOLEAN":   "BOOLEAN",
		"BOOL":      "BOOLEAN",
		
		// Date/Time Types
		"TIMESTAMP": "TIMESTAMP",
		"DATETIME":  "TIMESTAMP",
		"DATE":      "DATE",
		"TIME":      "TIME",
		
		// Binary Types
		"BINARY":    "BYTEA",
		"BLOB":      "BYTEA",
		
		// JSON Types
		"JSON":      "JSON",
		"JSONB":     "JSONB",           // PostgreSQL optimized JSON
		
		// UUID
		"UUID":      "UUID",
	},
	
	"MySQL": {
		// Primary Key Types
		"AUTO":      "INT AUTO_INCREMENT",
		"BIGAUTO":   "BIGINT AUTO_INCREMENT",
		
		// Numeric Types
		"INT":       "INT",
		"BIGINT":    "BIGINT",
		"SMALLINT":  "SMALLINT",
		"DECIMAL":   "DECIMAL",
		"NUMERIC":   "DECIMAL",
		"REAL":      "FLOAT",
		"FLOAT":     "DOUBLE",
		
		// String Types
		"STRING":    "VARCHAR(255)",
		"TEXT":      "TEXT",
		"CHAR":      "CHAR",
		
		// Boolean
		"BOOLEAN":   "BOOLEAN",
		"BOOL":      "BOOLEAN",
		
		// Date/Time Types
		"TIMESTAMP": "TIMESTAMP",
		"DATETIME":  "DATETIME",
		"DATE":      "DATE",
		"TIME":      "TIME",
		
		// Binary Types
		"BINARY":    "BLOB",
		"BLOB":      "BLOB",
		
		// JSON Types
		"JSON":      "JSON",
		"JSONB":     "JSON",             // MySQL doesn't have JSONB, use JSON
		
		// UUID
		"UUID":      "CHAR(36)",         // MySQL stores UUID as string
	},
	
	"SQLite": {
		// Primary Key Types
		"AUTO":      "INTEGER PRIMARY KEY AUTOINCREMENT",
		"BIGAUTO":   "INTEGER PRIMARY KEY AUTOINCREMENT",
		
		// Numeric Types (SQLite has fewer types)
		"INT":       "INTEGER",
		"BIGINT":    "INTEGER",
		"SMALLINT":  "INTEGER",
		"DECIMAL":   "REAL",
		"NUMERIC":   "REAL",
		"REAL":      "REAL",
		"FLOAT":     "REAL",
		
		// String Types
		"STRING":    "TEXT",
		"TEXT":      "TEXT",
		"CHAR":      "TEXT",
		
		// Boolean (stored as INTEGER)
		"BOOLEAN":   "INTEGER",
		"BOOL":      "INTEGER",
		
		// Date/Time Types (stored as TEXT or INTEGER)
		"TIMESTAMP": "TEXT",
		"DATETIME":  "TEXT",
		"DATE":      "TEXT",
		"TIME":      "TEXT",
		
		// Binary Types
		"BINARY":    "BLOB",
		"BLOB":      "BLOB",
		
		// JSON Types (stored as TEXT)
		"JSON":      "TEXT",
		"JSONB":     "TEXT",
		
		// UUID (stored as TEXT)
		"UUID":      "TEXT",
	},
	
	"MongoDB": {
		// MongoDB uses different type system
		// These map to BSON types
		"AUTO":      "ObjectId",         // MongoDB's _id
		"BIGAUTO":   "ObjectId",
		
		// Numeric Types
		"INT":       "Int32",
		"BIGINT":    "Int64",
		"SMALLINT":  "Int32",
		"DECIMAL":   "Decimal128",
		"NUMERIC":   "Decimal128",
		"REAL":      "Double",
		"FLOAT":     "Double",
		
		// String Types
		"STRING":    "String",
		"TEXT":      "String",
		"CHAR":      "String",
		
		// Boolean
		"BOOLEAN":   "Boolean",
		"BOOL":      "Boolean",
		
		// Date/Time Types
		"TIMESTAMP": "Date",
		"DATETIME":  "Date",
		"DATE":      "Date",
		"TIME":      "String",
		
		// Binary Types
		"BINARY":    "BinData",
		"BLOB":      "BinData",
		
		// JSON Types (native in MongoDB)
		"JSON":      "Object",
		"JSONB":     "Object",
		
		// UUID
		"UUID":      "UUID",
	},
}

// TypeDefinition defines type documentation for each universal type
type TypeDefinition struct {
	UniversalType string // The universal type name (e.g., "AUTO", "STRING")
	Description   string // Human-readable description
	Example       string // Example usage
	PostgreSQL    string // PostgreSQL equivalent
	MySQL         string // MySQL equivalent
	SQLite        string // SQLite equivalent
	MongoDB       string // MongoDB equivalent
}

// TypeDocs - Documentation of type mappings across databases
var TypeDocs = []TypeDefinition{
	{
		UniversalType: "AUTO",
		Description:   "Auto-incrementing integer primary key",
		Example:       "id: AUTO",
		PostgreSQL:    "SERIAL",
		MySQL:         "INT AUTO_INCREMENT",
		SQLite:        "INTEGER PRIMARY KEY AUTOINCREMENT",
		MongoDB:       "ObjectId",
	},
	{
		UniversalType: "STRING",
		Description:   "Variable-length text field with size limit",
		Example:       "name: STRING(255)",
		PostgreSQL:    "VARCHAR(255)",
		MySQL:         "VARCHAR(255)",
		SQLite:        "TEXT",
		MongoDB:       "String",
	},
	{
		UniversalType: "TEXT",
		Description:   "Unlimited length text field",
		Example:       "description: TEXT",
		PostgreSQL:    "TEXT",
		MySQL:         "TEXT",
		SQLite:        "TEXT",
		MongoDB:       "String",
	},
	{
		UniversalType: "INT",
		Description:   "Standard integer number",
		Example:       "age: INT",
		PostgreSQL:    "INTEGER",
		MySQL:         "INT",
		SQLite:        "INTEGER",
		MongoDB:       "Int32",
	},
	{
		UniversalType: "BOOLEAN",
		Description:   "True/False value",
		Example:       "is_active: BOOLEAN",
		PostgreSQL:    "BOOLEAN",
		MySQL:         "BOOLEAN",
		SQLite:        "INTEGER",
		MongoDB:       "Boolean",
	},
	{
		UniversalType: "TIMESTAMP",
		Description:   "Date and time",
		Example:       "created_at: TIMESTAMP",
		PostgreSQL:    "TIMESTAMP",
		MySQL:         "TIMESTAMP",
		SQLite:        "TEXT",
		MongoDB:       "Date",
	},
	{
		UniversalType: "JSON",
		Description:   "JSON data structure",
		Example:       "metadata: JSON",
		PostgreSQL:    "JSON",
		MySQL:         "JSON",
		SQLite:        "TEXT",
		MongoDB:       "Object",
	},
}