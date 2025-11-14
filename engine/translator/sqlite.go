package translator

import (
	"strings"
	"github.com/omniql-engine/omniql/mapping"
	"github.com/omniql-engine/omniql/engine/models"
	pb "github.com/omniql-engine/omniql/utilities/proto"
)

// TranslateSQLite converts OQL Query to SQLite RelationalQuery
// Supports 66 of 69 operations: CRUD (7) + DDL (14) + DQL (31) + TCL (8) + DCL (6 unsupported)
// Key SQLite limitations:
// - No DCL: GRANT/REVOKE unsupported (file-based permissions)
// - No TRUNCATE (uses DELETE instead)
// - CREATE/DROP DATABASE use ATTACH/DETACH
// - ALTER VIEW not supported (drop then create)
// - Limited transaction isolation levels
// - Window functions require SQLite 3.25+
// - CTEs require SQLite 3.8.3+
func TranslateSQLite(query *models.Query, tenantID string) (*pb.RelationalQuery, error) {
	operation := mapping.OperationMap["SQLite"][query.Operation]
	table := getSQLiteTableName(query.Entity, query.Operation)
	conditions := mapSQLiteConditions(query.Conditions)
	fields := mapSQLiteFields(query.Fields)
	
	// DQL: Map existing fields
	joins := mapSQLiteJoins(query.Joins)
	aggregate := mapSQLiteAggregate(query.Aggregate)
	orderBy := mapSQLiteOrderBy(query.OrderBy)
	having := mapSQLiteConditions(query.Having)
	
	// DQL: Map new advanced fields
	windowFunctions := mapSQLiteWindowFunctions(query.WindowFunctions)
	cte := mapSQLiteCTE(query.CTE)
	subquery := mapSQLiteSubquery(query.Subquery)
	pattern := query.Pattern
	caseWhen := mapSQLiteCaseStatement(query.CaseStatement)
	
	// TCL: Map transaction fields
	var savepointName string
	var isolationLevel string
	var readOnly bool
	if query.Transaction != nil {
		savepointName = query.Transaction.SavepointName
		isolationLevel = query.Transaction.IsolationLevel
		readOnly = query.Transaction.ReadOnly
	}
	
	// DCL: Map permission fields (will be unsupported for SQLite)
	var permissions []string
	var permissionTarget string
	var roleName string
	var userName string
	var password string
	var userRoles []string
	if query.Permission != nil {
		permissions = query.Permission.Permissions
		permissionTarget = query.Permission.Target
		roleName = query.Permission.RoleName
		userName = query.Permission.UserName
		password = query.Permission.Password
		userRoles = query.Permission.Roles
	}
	
	// CRUD: Map UPSERT and BULK INSERT
	upsert := mapSQLiteUpsert(query.Upsert)
	bulkData := mapSQLiteBulkData(query.BulkData)
	
	// DDL: Map view and database fields
	viewName := query.ViewName
	viewQuery := query.ViewQuery
	databaseName := query.DatabaseName
	newName := query.NewName
	
	return &pb.RelationalQuery{
		Operation:  operation,
		Table:      table,
		Conditions: conditions,
		Fields:     fields,
		Limit:      int32(query.Limit),
		Offset:     int32(query.Offset),
		
		// GROUP 3: DQL (existing)
		Joins:     joins,
		Aggregate: aggregate,
		OrderBy:   orderBy,
		GroupBy:   query.GroupBy,
		Having:    having,
		
		// GROUP 3: DQL (new advanced features)
		WindowFunctions: windowFunctions,
		Cte:             cte,
		Subquery:        subquery,
		Pattern:         pattern,
		CaseWhen:        caseWhen,
		
		// GROUP 4: TCL
		SavepointName:  savepointName,
		IsolationLevel: isolationLevel,
		ReadOnly:       readOnly,
		
		// GROUP 5: DCL (unsupported in SQLite - will return error)
		Permissions:      permissions,
		PermissionTarget: permissionTarget,
		RoleName:         roleName,
		UserName:         userName,
		Password:         password,
		UserRoles:        userRoles,
		
		// CRUD Extensions
		Upsert:   upsert,
		BulkData: bulkData,
		
		// DDL Extensions
		ViewName:     viewName,
		ViewQuery:    viewQuery,
		DatabaseName: databaseName,
		NewName:      newName,
	}, nil
}

// ============================================================================
// CRUD & DDL (Existing)
// ============================================================================

func mapSQLiteConditions(conditions []models.Condition) []*pb.QueryCondition {
	var pbConditions []*pb.QueryCondition
	for _, cond := range conditions {
		pbConditions = append(pbConditions, &pb.QueryCondition{
			Field:    convertSQLiteConditionField(cond.Field),
			Operator: cond.Operator,
			Value:    cond.Value,
		})
	}
	return pbConditions
}

func convertSQLiteConditionField(field string) string {
	parts := strings.Split(field, ".")
	if len(parts) == 2 {
		tableName := strings.ToLower(parts[0]) + "s"
		return tableName + "." + parts[1]
	}
	return strings.ToLower(field)
}

func mapSQLiteFields(fields []models.Field) []*pb.QueryField {
	var pbFields []*pb.QueryField
	for _, field := range fields {
		pbFields = append(pbFields, &pb.QueryField{
			Name:  field.Name,
			Value: field.Value,
		})
	}
	return pbFields
}

func getSQLiteTableName(entity string, operation string) string {
	rule := mapping.TableNamingRules[operation]
	
	if rule == "plural" {
		return strings.ToLower(entity) + "s"
	}
	
	if rule == "none" {
		return ""
	}
	
	return strings.ToLower(entity)
}

// ============================================================================
// GROUP 1: CRUD EXTENSIONS
// ============================================================================

// mapSQLiteUpsert converts UPSERT to SQLite INSERT OR REPLACE
func mapSQLiteUpsert(upsert *models.Upsert) *pb.UpsertClause {
	if upsert == nil {
		return nil
	}
	
	return &pb.UpsertClause{
		ConflictFields: upsert.ConflictFields,
		UpdateFields:   mapSQLiteFields(upsert.UpdateFields),
		ConflictAction: "REPLACE", // SQLite: INSERT OR REPLACE
	}
}

// mapSQLiteBulkData converts bulk insert rows
func mapSQLiteBulkData(bulkData [][]models.Field) []*pb.BulkInsertRow {
	if len(bulkData) == 0 {
		return nil
	}
	
	var pbBulkRows []*pb.BulkInsertRow
	for _, row := range bulkData {
		pbBulkRows = append(pbBulkRows, &pb.BulkInsertRow{
			Fields: mapSQLiteFields(row),
		})
	}
	return pbBulkRows
}

// ============================================================================
// GROUP 3: DQL - JOINS (Existing)
// ============================================================================

func mapSQLiteJoins(joins []models.Join) []*pb.JoinClause {
	var pbJoins []*pb.JoinClause
	for _, join := range joins {
		// SQLite doesn't support FULL JOIN natively
		// Will need to be emulated with UNION in executor
		joinType := string(join.Type)
		if joinType == "FULL" {
			joinType = "FULL_EMULATED" // Mark for special handling
		}
		
		pbJoins = append(pbJoins, &pb.JoinClause{
			JoinType:   joinType,
			Table:      strings.ToLower(join.Table) + "s",
			LeftField:  convertSQLiteJoinField(join.LeftField),
			RightField: convertSQLiteJoinField(join.RightField),
		})
	}
	return pbJoins
}

func convertSQLiteJoinField(field string) string {
	parts := strings.Split(field, ".")
	if len(parts) != 2 {
		return field
	}
	
	tableName := strings.ToLower(parts[0]) + "s"
	columnName := parts[1]
	
	return tableName + "." + columnName
}

// ============================================================================
// GROUP 3: DQL - AGGREGATES (Existing)
// ============================================================================

func mapSQLiteAggregate(agg *models.Aggregation) *pb.AggregateClause {
	if agg == nil {
		return nil
	}
	return &pb.AggregateClause{
		Function: string(agg.Function),
		Field:    agg.Field,
	}
}

func mapSQLiteOrderBy(orderBy []models.OrderBy) []*pb.OrderByClause {
	var pbOrderBy []*pb.OrderByClause
	for _, ob := range orderBy {
		pbOrderBy = append(pbOrderBy, &pb.OrderByClause{
			Field:     ob.Field,
			Direction: string(ob.Direction),
		})
	}
	return pbOrderBy
}

// ============================================================================
// GROUP 3: DQL - WINDOW FUNCTIONS (NEW)
// SQLite 3.25+ supports window functions
// ============================================================================

func mapSQLiteWindowFunctions(windowFuncs []models.WindowFunction) []*pb.WindowClause {
	if len(windowFuncs) == 0 {
		return nil
	}
	
	var pbWindows []*pb.WindowClause
	for _, wf := range windowFuncs {
		pbWindow := &pb.WindowClause{
			Function:    string(wf.Function),
			Alias:       wf.Alias,
			PartitionBy: wf.PartitionBy,
			OrderBy:     mapSQLiteOrderBy(wf.OrderBy),
			Offset:      int32(wf.Offset),
			Buckets:     int32(wf.Buckets),
		}
		
		// For LAG/LEAD, set the field
		if wf.Function == models.Lag || wf.Function == models.Lead {
			pbWindow.Alias = wf.Field
		}
		
		pbWindows = append(pbWindows, pbWindow)
	}
	
	return pbWindows
}

// ============================================================================
// GROUP 3: DQL - COMMON TABLE EXPRESSIONS (NEW)
// SQLite 3.8.3+ supports CTEs
// ============================================================================

func mapSQLiteCTE(cte *models.CTE) *pb.CTEClause {
	if cte == nil {
		return nil
	}
	
	return &pb.CTEClause{
		CteName:   cte.Name,
		CteQuery:  cte.Query,
		Recursive: cte.Recursive,
	}
}

// ============================================================================
// GROUP 3: DQL - SUBQUERIES (NEW)
// ============================================================================

func mapSQLiteSubquery(subquery *models.Subquery) *pb.SubqueryClause {
	if subquery == nil {
		return nil
	}
	
	return &pb.SubqueryClause{
		SubqueryType: subquery.Type,
		Field:        subquery.Field,
		Subquery:     subquery.Query,
		Alias:        subquery.Alias,
	}
}

// ============================================================================
// GROUP 3: DQL - CASE STATEMENTS (NEW)
// ============================================================================

func mapSQLiteCaseStatement(caseStmt *models.CaseStatement) *pb.CaseClause {
	if caseStmt == nil {
		return nil
	}
	
	var pbWhenClauses []*pb.CaseWhen
	for _, when := range caseStmt.WhenClauses {
		pbWhenClauses = append(pbWhenClauses, &pb.CaseWhen{
			Condition: when.Condition,
			ThenValue: when.ThenValue,
		})
	}
	
	return &pb.CaseClause{
		WhenClauses: pbWhenClauses,
		ElseValue:   caseStmt.ElseValue,
		Alias:       caseStmt.Alias,
	}
}