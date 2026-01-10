package postgres

import (
	"fmt"
	"strings"
	"strconv"

	"github.com/omniql-engine/omniql/mapping"
	pb "github.com/omniql-engine/omniql/utilities/proto"
)

// ============================================================================
// DDL OPERATIONS - SQL BUILDERS (Moved from builders.go)
// ============================================================================

func BuildCreateTableSQL(query *pb.RelationalQuery) string {
	if len(query.Fields) == 0 {
		return ""
	}
	var columns []string
	for _, field := range query.Fields {
		columnDef := buildColumnDefinition(field.NameExpr.Value, field.ValueExpr.Value, field.Constraints)
		columns = append(columns, columnDef)
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)", query.Table, strings.Join(columns, ", "))
}

func BuildAlterTableSQL(query *pb.RelationalQuery) (string, error) {
	if query.AlterAction == "" {
		return "", fmt.Errorf("no ALTER operation specified")
	}

	switch strings.ToUpper(query.AlterAction) {
	case "ADD_COLUMN":
		if len(query.Fields) == 0 {
			return "", fmt.Errorf("no column specified for ADD_COLUMN")
		}
		colName := getFieldName(query.Fields[0])
		colType := getFieldValue(query.Fields[0])
		columnDef := buildColumnDefinition(colName, colType, query.Fields[0].Constraints)
		return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", query.Table, columnDef), nil

	case "DROP_COLUMN":
		if len(query.Fields) == 0 {
			return "", fmt.Errorf("no column specified for DROP_COLUMN")
		}
		colName := getFieldName(query.Fields[0])
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", query.Table, colName), nil

	case "RENAME_COLUMN":
		if len(query.Fields) == 0 {
			return "", fmt.Errorf("no column specified for RENAME_COLUMN")
		}
		oldName := getFieldName(query.Fields[0])
		newName := getFieldValue(query.Fields[0])
		return fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", query.Table, oldName, newName), nil

	case "RENAME_TABLE":
		return fmt.Sprintf("ALTER TABLE %s RENAME TO %s", query.Table, query.NewName), nil

	default:
		return "", fmt.Errorf("unknown ALTER operation: %s", query.AlterAction)
	}
}

func BuildDropTableSQL(query *pb.RelationalQuery) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", query.Table)
}

func BuildTruncateTableSQL(query *pb.RelationalQuery) string {
	return fmt.Sprintf("TRUNCATE TABLE %s", query.Table)
}

func BuildRenameTableSQL(query *pb.RelationalQuery) (string, error) {
	if query.Table == "" {
		return "", fmt.Errorf("no table name specified")
	}
	if query.NewName == "" {
		return "", fmt.Errorf("no new table name specified")
	}
	return fmt.Sprintf("ALTER TABLE %s RENAME TO %s", query.Table, query.NewName), nil
}

func BuildCreateIndexSQL(query *pb.RelationalQuery) (string, error) {
	if len(query.Fields) == 0 {
		return "", fmt.Errorf("no index details specified")
	}

	indexName := query.Fields[0].NameExpr.Value
	columnName := query.Fields[0].ValueExpr.Value

	indexType := "INDEX"
	if len(query.Fields[0].Constraints) > 0 {
		for _, constraint := range query.Fields[0].Constraints {
			if strings.ToUpper(constraint) == "UNIQUE" {
				indexType = "UNIQUE INDEX"
				break
			}
		}
	}

	return fmt.Sprintf("CREATE %s %s ON %s (%s)", indexType, indexName, query.Table, columnName), nil
}

func BuildDropIndexSQL(query *pb.RelationalQuery) (string, error) {
	if len(query.Fields) == 0 {
		return "", fmt.Errorf("no index name specified")
	}
	return fmt.Sprintf("DROP INDEX IF EXISTS %s", query.Fields[0].NameExpr.Value), nil
}

func BuildCreateDatabaseSQL(query *pb.RelationalQuery) (string, error) {
	if query.DatabaseName == "" {
		return "", fmt.Errorf("no database name specified")
	}
	return fmt.Sprintf("CREATE DATABASE %s", query.DatabaseName), nil
}

func BuildDropDatabaseSQL(query *pb.RelationalQuery) (string, error) {
	if query.DatabaseName == "" {
		return "", fmt.Errorf("no database name specified")
	}
	return fmt.Sprintf("DROP DATABASE IF EXISTS %s", query.DatabaseName), nil
}

// formatLiteral formats a value as SQL literal (for VIEW definitions)
func formatLiteral(v interface{}) string {
	s := fmt.Sprintf("%v", v)
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return s
	}
	upper := strings.ToUpper(s)
	if upper == "TRUE" || upper == "FALSE" {
		return upper
	}
	return fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''"))
}

func BuildCreateViewSQL(query *pb.RelationalQuery) (string, error) {
	if query.ViewName == "" {
		return "", fmt.Errorf("no view name specified")
	}
	if query.ViewQuery == nil {
		return "", fmt.Errorf("no view query specified")
	}
	viewSQL, args := BuildSelectSQL(query.ViewQuery)
	for i, arg := range args {
		placeholder := fmt.Sprintf("$%d", i+1)
		viewSQL = strings.Replace(viewSQL, placeholder, formatLiteral(arg), 1)
	}
	return fmt.Sprintf("CREATE VIEW %s AS %s", query.ViewName, viewSQL), nil
}

func BuildDropViewSQL(query *pb.RelationalQuery) (string, error) {
	if query.ViewName == "" {
		return "", fmt.Errorf("no view name specified")
	}
	return fmt.Sprintf("DROP VIEW IF EXISTS %s", query.ViewName), nil
}

func BuildAlterViewSQL(query *pb.RelationalQuery) (string, error) {
	if query.ViewName == "" {
		return "", fmt.Errorf("no view name specified")
	}
	if query.ViewQuery == nil {
		return "", fmt.Errorf("no view query specified")
	}
	viewSQL, args := BuildSelectSQL(query.ViewQuery)
	for i, arg := range args {
		placeholder := fmt.Sprintf("$%d", i+1)
		viewSQL = strings.Replace(viewSQL, placeholder, formatLiteral(arg), 1)
	}
	return fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", query.ViewName, viewSQL), nil
}

func buildColumnDefinition(name, columnType string, constraints []string) string {
	baseType := columnType
	params := ""

	if idx := strings.Index(columnType, "("); idx != -1 {
		baseType = columnType[:idx]
		if endIdx := strings.Index(columnType, ")"); endIdx != -1 {
			params = columnType[idx : endIdx+1]
		}
	}

	pgType := baseType
	if mapping.TypeMap != nil && mapping.TypeMap["PostgreSQL"] != nil {
		if mappedType, exists := mapping.TypeMap["PostgreSQL"][strings.ToUpper(baseType)]; exists {
			pgType = mappedType
		}
	}

	columnDef := fmt.Sprintf("%s %s%s", name, pgType, params)

	if strings.ToUpper(baseType) == "AUTO" {
		return fmt.Sprintf("%s SERIAL PRIMARY KEY", name)
	}

	for _, constraint := range constraints {
		switch strings.ToUpper(constraint) {
		case "UNIQUE":
			columnDef += " UNIQUE"
		case "NOT_NULL":
			columnDef += " NOT NULL"
		case "PRIMARY_KEY":
			columnDef += " PRIMARY KEY"
		}
	}

	return columnDef
}

// ============================================================================
// POSTGRESQL-SPECIFIC DDL OPERATIONS
// ============================================================================

// ----------------------------------------------------------------------------
// SEQUENCE OPERATIONS
// ----------------------------------------------------------------------------

func BuildCreateSequenceSQL(query *pb.RelationalQuery) (string, error) {
	if query.SequenceName == "" {
		return "", fmt.Errorf("no sequence name specified")
	}
	sql := fmt.Sprintf("CREATE SEQUENCE %s", query.SequenceName)
	if query.SequenceStart > 0 {
		sql += fmt.Sprintf(" START WITH %d", query.SequenceStart)
	}
	if query.SequenceIncrement > 0 {
		sql += fmt.Sprintf(" INCREMENT BY %d", query.SequenceIncrement)
	}
	if query.SequenceMin > 0 {
		sql += fmt.Sprintf(" MINVALUE %d", query.SequenceMin)
	}
	if query.SequenceMax > 0 {
		sql += fmt.Sprintf(" MAXVALUE %d", query.SequenceMax)
	}
	if query.SequenceCache > 0 {
		sql += fmt.Sprintf(" CACHE %d", query.SequenceCache)
	}
	if query.SequenceCycle {
		sql += " CYCLE"
	}
	return sql, nil
}

func BuildAlterSequenceSQL(query *pb.RelationalQuery) (string, error) {
	if query.SequenceName == "" {
		return "", fmt.Errorf("no sequence name specified")
	}
	sql := fmt.Sprintf("ALTER SEQUENCE %s", query.SequenceName)
	if query.SequenceRestart > 0 {
		sql += fmt.Sprintf(" RESTART WITH %d", query.SequenceRestart)
	}
	if query.SequenceIncrement > 0 {
		sql += fmt.Sprintf(" INCREMENT BY %d", query.SequenceIncrement)
	}
	return sql, nil
}

func BuildDropSequenceSQL(query *pb.RelationalQuery) (string, error) {
	if query.SequenceName == "" {
		return "", fmt.Errorf("no sequence name specified")
	}
	cascade := ""
	if query.Cascade {
		cascade = " CASCADE"
	}
	return fmt.Sprintf("DROP SEQUENCE IF EXISTS %s%s", query.SequenceName, cascade), nil
}

// ----------------------------------------------------------------------------
// EXTENSION OPERATIONS
// ----------------------------------------------------------------------------

func BuildCreateExtensionSQL(query *pb.RelationalQuery) (string, error) {
	if query.ExtensionName == "" {
		return "", fmt.Errorf("no extension name specified")
	}
	sql := fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s", query.ExtensionName)
	if query.SchemaName != "" {
		sql += fmt.Sprintf(" SCHEMA %s", query.SchemaName)
	}
	return sql, nil
}

func BuildDropExtensionSQL(query *pb.RelationalQuery) (string, error) {
	if query.ExtensionName == "" {
		return "", fmt.Errorf("no extension name specified")
	}
	cascade := ""
	if query.Cascade {
		cascade = " CASCADE"
	}
	return fmt.Sprintf("DROP EXTENSION IF EXISTS %s%s", query.ExtensionName, cascade), nil
}

// ----------------------------------------------------------------------------
// SCHEMA OPERATIONS
// ----------------------------------------------------------------------------

func BuildCreateSchemaSQL(query *pb.RelationalQuery) (string, error) {
	if query.SchemaName == "" {
		return "", fmt.Errorf("no schema name specified")
	}
	sql := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", query.SchemaName)
	if query.SchemaOwner != "" {
		sql += fmt.Sprintf(" AUTHORIZATION %s", query.SchemaOwner)
	}
	return sql, nil
}

func BuildDropSchemaSQL(query *pb.RelationalQuery) (string, error) {
	if query.SchemaName == "" {
		return "", fmt.Errorf("no schema name specified")
	}
	cascade := ""
	if query.Cascade {
		cascade = " CASCADE"
	}
	return fmt.Sprintf("DROP SCHEMA IF EXISTS %s%s", query.SchemaName, cascade), nil
}

// ----------------------------------------------------------------------------
// TYPE OPERATIONS (ENUM, COMPOSITE)
// ----------------------------------------------------------------------------

func BuildCreateTypeSQL(query *pb.RelationalQuery) (string, error) {
	if query.TypeName == "" {
		return "", fmt.Errorf("no type name specified")
	}
	// ENUM type
	if query.TypeKind == "ENUM" && len(query.EnumValues) > 0 {
		var quotedValues []string
		for _, v := range query.EnumValues {
			quotedValues = append(quotedValues, fmt.Sprintf("'%s'", v))
		}
		return fmt.Sprintf("CREATE TYPE %s AS ENUM (%s)", query.TypeName, strings.Join(quotedValues, ", ")), nil
	}
	// COMPOSITE type
	if len(query.Fields) > 0 {
		var columns []string
		for _, field := range query.Fields {
			columns = append(columns, fmt.Sprintf("%s %s", getFieldName(field), getFieldValue(field)))
		}
		return fmt.Sprintf("CREATE TYPE %s AS (%s)", query.TypeName, strings.Join(columns, ", ")), nil
	}
	return "", fmt.Errorf("CREATE TYPE requires ENUM values or composite fields")
}

func BuildAlterTypeSQL(query *pb.RelationalQuery) (string, error) {
	if query.TypeName == "" {
		return "", fmt.Errorf("no type name specified")
	}
	if query.AlterAction == "ADD_VALUE" && query.EnumValue != "" {
		return fmt.Sprintf("ALTER TYPE %s ADD VALUE '%s'", query.TypeName, query.EnumValue), nil
	}
	if query.AlterAction == "RENAME_VALUE" && query.EnumValue != "" && query.NewEnumValue != "" {
		return fmt.Sprintf("ALTER TYPE %s RENAME VALUE '%s' TO '%s'", query.TypeName, query.EnumValue, query.NewEnumValue), nil
	}
	return "", fmt.Errorf("ALTER TYPE requires ADD_VALUE or RENAME_VALUE action")
}

func BuildDropTypeSQL(query *pb.RelationalQuery) (string, error) {
	if query.TypeName == "" {
		return "", fmt.Errorf("no type name specified")
	}
	cascade := ""
	if query.Cascade {
		cascade = " CASCADE"
	}
	return fmt.Sprintf("DROP TYPE IF EXISTS %s%s", query.TypeName, cascade), nil
}

// ----------------------------------------------------------------------------
// DOMAIN OPERATIONS
// ----------------------------------------------------------------------------

func BuildCreateDomainSQL(query *pb.RelationalQuery) (string, error) {
	if query.DomainName == "" {
		return "", fmt.Errorf("no domain name specified")
	}
	if query.DomainType == "" {
		return "", fmt.Errorf("no domain type specified")
	}
	sql := fmt.Sprintf("CREATE DOMAIN %s AS %s", query.DomainName, query.DomainType)
	if query.DomainDefault != "" {
		sql += fmt.Sprintf(" DEFAULT %s", query.DomainDefault)
	}
	if query.DomainConstraint != "" {
		sql += fmt.Sprintf(" CHECK (%s)", query.DomainConstraint)
	}
	return sql, nil
}

func BuildDropDomainSQL(query *pb.RelationalQuery) (string, error) {
	if query.DomainName == "" {
		return "", fmt.Errorf("no domain name specified")
	}
	cascade := ""
	if query.Cascade {
		cascade = " CASCADE"
	}
	return fmt.Sprintf("DROP DOMAIN IF EXISTS %s%s", query.DomainName, cascade), nil
}

// ----------------------------------------------------------------------------
// FUNCTION OPERATIONS
// ----------------------------------------------------------------------------

func BuildCreateFunctionSQL(query *pb.RelationalQuery) (string, error) {
	if query.FuncName == "" {
		return "", fmt.Errorf("no function name specified")
	}
	if query.FuncBody == "" {
		return "", fmt.Errorf("no function body specified")
	}

	args := ""
	if len(query.FuncArgs) > 0 {
		// Convert x:INT to x INT (PostgreSQL syntax)
		var formattedArgs []string
		for _, arg := range query.FuncArgs {
			formattedArgs = append(formattedArgs, strings.Replace(arg, ":", " ", 1))
		}
		args = strings.Join(formattedArgs, ", ")
	}

	returnType := query.FuncReturns
	if returnType == "" {
		returnType = "void"
	}

	language := query.FuncLanguage
	if language == "" {
		language = "plpgsql"
	}

	return fmt.Sprintf("CREATE OR REPLACE FUNCTION %s(%s) RETURNS %s LANGUAGE %s AS $$%s$$",
		query.FuncName, args, returnType, language, query.FuncBody), nil
}

func BuildAlterFunctionSQL(query *pb.RelationalQuery) (string, error) {
	if query.FuncName == "" {
		return "", fmt.Errorf("no function name specified")
	}
	if query.FuncOwner != "" {
		return fmt.Sprintf("ALTER FUNCTION %s OWNER TO %s", query.FuncName, query.FuncOwner), nil
	}
	if query.SchemaName != "" {
		return fmt.Sprintf("ALTER FUNCTION %s SET SCHEMA %s", query.FuncName, query.SchemaName), nil
	}
	return "", fmt.Errorf("ALTER FUNCTION requires OWNER or SET SCHEMA")
}

func BuildDropFunctionSQL(query *pb.RelationalQuery) (string, error) {
	if query.FuncName == "" {
		return "", fmt.Errorf("no function name specified")
	}
	cascade := ""
	if query.Cascade {
		cascade = " CASCADE"
	}
	return fmt.Sprintf("DROP FUNCTION IF EXISTS %s%s", query.FuncName, cascade), nil
}

// ----------------------------------------------------------------------------
// TRIGGER OPERATIONS
// ----------------------------------------------------------------------------

func BuildCreateTriggerSQL(query *pb.RelationalQuery) (string, error) {
	if query.TriggerName == "" {
		return "", fmt.Errorf("no trigger name specified")
	}
	if query.Table == "" {
		return "", fmt.Errorf("no table specified for trigger")
	}
	if query.FuncName == "" {
		return "", fmt.Errorf("no function specified for trigger")
	}

	timing := query.TriggerTiming
	if timing == "" {
		timing = "BEFORE"
	}

	events := query.TriggerEvents
	if events == "" {
		events = "INSERT"
	}

	forEach := "ROW"
	if query.TriggerForEach != "" {
		forEach = query.TriggerForEach
	}

	return fmt.Sprintf("CREATE TRIGGER %s %s %s ON %s FOR EACH %s EXECUTE FUNCTION %s()",
		query.TriggerName, timing, events, query.Table, forEach, query.FuncName), nil
}

func BuildDropTriggerSQL(query *pb.RelationalQuery) (string, error) {
	if query.TriggerName == "" {
		return "", fmt.Errorf("no trigger name specified")
	}
	if query.Table == "" {
		return "", fmt.Errorf("no table specified for DROP TRIGGER")
	}
	cascade := ""
	if query.Cascade {
		cascade = " CASCADE"
	}
	return fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s%s", query.TriggerName, query.Table, cascade), nil
}

// ----------------------------------------------------------------------------
// POLICY OPERATIONS (Row Level Security)
// ----------------------------------------------------------------------------

func BuildCreatePolicySQL(query *pb.RelationalQuery) (string, error) {
	if query.PolicyName == "" {
		return "", fmt.Errorf("no policy name specified")
	}
	if query.Table == "" {
		return "", fmt.Errorf("no table specified for policy")
	}

	sql := fmt.Sprintf("CREATE POLICY %s ON %s", query.PolicyName, query.Table)

	if query.PolicyFor != "" {
		sql += fmt.Sprintf(" FOR %s", query.PolicyFor)
	}
	if query.PolicyTo != "" {
		sql += fmt.Sprintf(" TO %s", query.PolicyTo)
	}
	if query.PolicyUsing != "" {
		sql += fmt.Sprintf(" USING (%s)", query.PolicyUsing)
	}
	if query.PolicyCheck != "" {
		sql += fmt.Sprintf(" WITH CHECK (%s)", query.PolicyCheck)
	}

	return sql, nil
}

func BuildDropPolicySQL(query *pb.RelationalQuery) (string, error) {
	if query.PolicyName == "" {
		return "", fmt.Errorf("no policy name specified")
	}
	if query.Table == "" {
		return "", fmt.Errorf("no table specified for DROP POLICY")
	}
	return fmt.Sprintf("DROP POLICY IF EXISTS %s ON %s", query.PolicyName, query.Table), nil
}

// ----------------------------------------------------------------------------
// RULE OPERATIONS
// ----------------------------------------------------------------------------

func BuildCreateRuleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RuleName == "" {
		return "", fmt.Errorf("no rule name specified")
	}
	if query.Table == "" {
		return "", fmt.Errorf("no table specified for rule")
	}
	if query.RuleEvent == "" {
		return "", fmt.Errorf("no event specified for rule")
	}

	action := query.RuleAction
	if action == "" {
		action = "NOTHING"
	}

	return fmt.Sprintf("CREATE OR REPLACE RULE %s AS ON %s TO %s DO %s",
		query.RuleName, query.RuleEvent, query.Table, action), nil
}

func BuildDropRuleSQL(query *pb.RelationalQuery) (string, error) {
	if query.RuleName == "" {
		return "", fmt.Errorf("no rule name specified")
	}
	if query.Table == "" {
		return "", fmt.Errorf("no table specified for DROP RULE")
	}
	cascade := ""
	if query.Cascade {
		cascade = " CASCADE"
	}
	return fmt.Sprintf("DROP RULE IF EXISTS %s ON %s%s", query.RuleName, query.Table, cascade), nil
}

// ----------------------------------------------------------------------------
// COMMENT OPERATION
// ----------------------------------------------------------------------------

func BuildCommentOnSQL(query *pb.RelationalQuery) (string, error) {
	if query.CommentTarget == "" {
		return "", fmt.Errorf("no comment target specified")
	}
	if query.CommentText == "" {
		return fmt.Sprintf("COMMENT ON %s IS NULL", query.CommentTarget), nil
	}
	return fmt.Sprintf("COMMENT ON %s IS '%s'", query.CommentTarget, strings.ReplaceAll(query.CommentText, "'", "''")), nil
}