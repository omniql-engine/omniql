package reverse

import (
	"fmt"
	"strings"

	"github.com/omniql-engine/omniql/engine/models"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

// ============================================================================
// ENTRY POINT
// ============================================================================

func PostgreSQLToQuery(sql string) (*models.Query, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrParseError, err)
	}

	if len(tree.Stmts) == 0 {
		return nil, fmt.Errorf("%w: no statements", ErrParseError)
	}

	stmt := tree.Stmts[0].Stmt

	switch {
	// ==================== CRUD ====================
	case stmt.GetSelectStmt() != nil:
		sel := stmt.GetSelectStmt()
		if sel.Op != pg_query.SetOperation_SETOP_NONE {
			return convertSetOperation(sel) // UNION/INTERSECT/EXCEPT → advanced
		}
		return convertSelect(sel)
	case stmt.GetInsertStmt() != nil:
		return convertInsert(stmt.GetInsertStmt())
	case stmt.GetUpdateStmt() != nil:
		return convertUpdate(stmt.GetUpdateStmt())
	case stmt.GetDeleteStmt() != nil:
		return convertDelete(stmt.GetDeleteStmt())

	// ==================== DDL Basic ====================
	case stmt.GetCreateStmt() != nil:
		return convertCreateTable(stmt.GetCreateStmt())
	case stmt.GetAlterTableStmt() != nil:
		return convertAlterTable(stmt.GetAlterTableStmt())
	case stmt.GetDropStmt() != nil:
		return convertDropAll(stmt.GetDropStmt()) // routes to basic or advanced
	case stmt.GetTruncateStmt() != nil:
		return convertTruncate(stmt.GetTruncateStmt())
	case stmt.GetIndexStmt() != nil:
		return convertCreateIndex(stmt.GetIndexStmt())
	case stmt.GetViewStmt() != nil:
		return convertCreateView(stmt.GetViewStmt())

	// ==================== DDL Advanced ====================
	case stmt.GetCreateSeqStmt() != nil:
		return convertCreateSequence(stmt.GetCreateSeqStmt())
	case stmt.GetAlterSeqStmt() != nil:
		return convertAlterSequence(stmt.GetAlterSeqStmt())
	case stmt.GetCreateSchemaStmt() != nil:
		return convertCreateSchema(stmt.GetCreateSchemaStmt())
	case stmt.GetCompositeTypeStmt() != nil:
		return convertCreateType(stmt.GetCompositeTypeStmt())
	case stmt.GetCreateEnumStmt() != nil:
		return convertCreateEnum(stmt.GetCreateEnumStmt())
	case stmt.GetAlterEnumStmt() != nil:
		return convertAlterType(stmt.GetAlterEnumStmt())
	case stmt.GetCreateDomainStmt() != nil:
		return convertCreateDomain(stmt.GetCreateDomainStmt())
	case stmt.GetCreateFunctionStmt() != nil:
		return convertCreateFunction(stmt.GetCreateFunctionStmt())
	case stmt.GetAlterFunctionStmt() != nil:
		return convertAlterFunction(stmt.GetAlterFunctionStmt())
	case stmt.GetCreateTrigStmt() != nil:
		return convertCreateTrigger(stmt.GetCreateTrigStmt())
	case stmt.GetCreatePolicyStmt() != nil:
		return convertCreatePolicy(stmt.GetCreatePolicyStmt())
	case stmt.GetRuleStmt() != nil:
		return convertCreateRule(stmt.GetRuleStmt())
	case stmt.GetCreateExtensionStmt() != nil:
		return convertCreateExtension(stmt.GetCreateExtensionStmt())
	case stmt.GetCommentStmt() != nil:
		return convertComment(stmt.GetCommentStmt())
	case stmt.GetRenameStmt() != nil:
		return convertRenameTable(stmt.GetRenameStmt())
	case stmt.GetCreatedbStmt() != nil:
		return convertCreateDatabase(stmt.GetCreatedbStmt())
	case stmt.GetDropdbStmt() != nil:
		return convertDropDatabase(stmt.GetDropdbStmt())

	// ==================== TCL ====================
	case stmt.GetTransactionStmt() != nil:
		return convertTransaction(stmt.GetTransactionStmt())
	case stmt.GetVariableSetStmt() != nil:
		return convertSetTransaction(stmt.GetVariableSetStmt())

	// ==================== DCL ====================
	case stmt.GetGrantStmt() != nil:
		return convertGrant(stmt.GetGrantStmt())
	case stmt.GetCreateRoleStmt() != nil:
		return convertCreateRole(stmt.GetCreateRoleStmt())
	case stmt.GetDropRoleStmt() != nil:
		return convertDropRole(stmt.GetDropRoleStmt())
	case stmt.GetGrantRoleStmt() != nil:
		return convertGrantRole(stmt.GetGrantRoleStmt())
	case stmt.GetAlterRoleStmt() != nil:
		return convertAlterRole(stmt.GetAlterRoleStmt())
	default:
		return nil, fmt.Errorf("%w: unsupported statement type", ErrNotSupported)
	}
}

// ============================================================================
// CRUD: SELECT → GET
// ============================================================================

func convertSelect(stmt *pg_query.SelectStmt) (*models.Query, error) {
	if len(stmt.FromClause) == 0 {
		return nil, fmt.Errorf("%w: SELECT without FROM", ErrNotSupported)
	}

	entity, joins, err := extractFromClause(stmt.FromClause)
	if err != nil {
		return nil, err
	}

	query := &models.Query{Operation: "GET", Entity: entity, Joins: joins}

	if agg := extractAggregate(stmt.TargetList); agg != nil {
		query.Aggregate = agg
		query.Operation = string(agg.Function)
	}

	if len(stmt.DistinctClause) > 0 {
		query.Distinct = true
	}

	if cols := extractColumns(stmt.TargetList); len(cols) > 0 {
		query.Columns = cols
	}

	if stmt.WhereClause != nil {
		if conds, err := nodeToConditions(stmt.WhereClause); err != nil {
			return nil, fmt.Errorf("WHERE: %w", err)
		} else {
			query.Conditions = conds
		}
	}

	for _, g := range stmt.GroupClause {
		if expr, err := nodeToExpression(g); err != nil {
			return nil, fmt.Errorf("GROUP BY: %w", err)
		} else {
			query.GroupBy = append(query.GroupBy, expr)
		}
	}

	if stmt.HavingClause != nil {
		if conds, err := nodeToConditions(stmt.HavingClause); err != nil {
			return nil, fmt.Errorf("HAVING: %w", err)
		} else {
			query.Having = conds
		}
	}

	if len(stmt.SortClause) > 0 {
		if orderBy, err := extractOrderBy(stmt.SortClause); err != nil {
			return nil, err
		} else {
			query.OrderBy = orderBy
		}
	}

	if stmt.LimitCount != nil {
		if limit, err := nodeToInt(stmt.LimitCount); err != nil {
			return nil, fmt.Errorf("LIMIT: %w", err)
		} else {
			query.Limit = limit
		}
	}

	if stmt.LimitOffset != nil {
		if offset, err := nodeToInt(stmt.LimitOffset); err != nil {
			return nil, fmt.Errorf("OFFSET: %w", err)
		} else {
			query.Offset = offset
		}
	}

	// Window Functions (advanced)
	if wfs := extractWindowFunctions(stmt.TargetList); len(wfs) > 0 {
		query.WindowFunctions = wfs
	}

	// CTE - WITH clause (advanced)
	if stmt.WithClause != nil {
		cte, err := extractCTE(stmt.WithClause, query)
		if err != nil {
			return nil, err
		}
		query.CTE = cte
	}

	return query, nil
}

// ============================================================================
// CRUD: INSERT → CREATE / BULK INSERT / UPSERT
// ============================================================================

func convertInsert(stmt *pg_query.InsertStmt) (*models.Query, error) {
	entity := TableToEntity(stmt.Relation.Relname)

	var columns []string
	for _, col := range stmt.Cols {
		if rt := col.GetResTarget(); rt != nil {
			columns = append(columns, rt.Name)
		}
	}

	selectStmt := stmt.SelectStmt.GetSelectStmt()
	if selectStmt == nil || len(selectStmt.ValuesLists) == 0 {
		return nil, fmt.Errorf("%w: INSERT without VALUES", ErrParseError)
	}

	if stmt.OnConflictClause != nil {
		return convertUpsert(entity, columns, selectStmt.ValuesLists[0], stmt.OnConflictClause)
	}

	if len(selectStmt.ValuesLists) == 1 {
		fields, err := buildFields(columns, selectStmt.ValuesLists[0].GetList().Items)
		if err != nil {
			return nil, err
		}
		return &models.Query{Operation: "CREATE", Entity: entity, Fields: fields}, nil
	}

	var bulkData [][]models.Field
	for _, vl := range selectStmt.ValuesLists {
		fields, err := buildFields(columns, vl.GetList().Items)
		if err != nil {
			return nil, err
		}
		bulkData = append(bulkData, fields)
	}
	return &models.Query{Operation: "BULK INSERT", Entity: entity, BulkData: bulkData}, nil
}

func convertUpsert(entity string, columns []string, values *pg_query.Node, conflict *pg_query.OnConflictClause) (*models.Query, error) {
	fields, err := buildFields(columns, values.GetList().Items)
	if err != nil {
		return nil, err
	}

	var conflictFields []*models.Expression
	if conflict.Infer != nil {
		for _, elem := range conflict.Infer.IndexElems {
			if ie := elem.GetIndexElem(); ie != nil {
				conflictFields = append(conflictFields, FieldExpr(ie.Name))
			}
		}
	}

	var updateFields []models.Field
	for _, target := range conflict.TargetList {
		if rt := target.GetResTarget(); rt != nil {
			val, err := nodeToExpression(rt.Val)
			if err != nil {
				return nil, err
			}
			updateFields = append(updateFields, models.Field{NameExpr: FieldExpr(rt.Name), ValueExpr: val})
		}
	}

	return &models.Query{
		Operation: "UPSERT",
		Entity:    entity,
		Fields:    fields,
		Upsert:    &models.Upsert{ConflictFields: conflictFields, UpdateFields: updateFields},
	}, nil
}

func convertUpdate(stmt *pg_query.UpdateStmt) (*models.Query, error) {
	entity := TableToEntity(stmt.Relation.Relname)

	var fields []models.Field
	for _, target := range stmt.TargetList {
		if rt := target.GetResTarget(); rt != nil {
			val, _ := nodeToExpression(rt.Val)
			fields = append(fields, models.Field{NameExpr: FieldExpr(rt.Name), ValueExpr: val})
		}
	}

	query := &models.Query{Operation: "UPDATE", Entity: entity, Fields: fields}
	if stmt.WhereClause != nil {
		query.Conditions, _ = nodeToConditions(stmt.WhereClause)
	}
	return query, nil
}

func convertDelete(stmt *pg_query.DeleteStmt) (*models.Query, error) {
	entity := TableToEntity(stmt.Relation.Relname)
	query := &models.Query{Operation: "DELETE", Entity: entity}
	if stmt.WhereClause != nil {
		query.Conditions, _ = nodeToConditions(stmt.WhereClause)
	}
	return query, nil
}

// ============================================================================
// DDL: TABLE, INDEX, VIEW, TRUNCATE
// ============================================================================

func convertCreateTable(stmt *pg_query.CreateStmt) (*models.Query, error) {
	var fields []models.Field
	for _, elt := range stmt.TableElts {
		if colDef := elt.GetColumnDef(); colDef != nil {
			field, _ := columnDefToField(colDef)
			fields = append(fields, field)
		}
	}
	return &models.Query{Operation: "CREATE TABLE", Entity: TableToEntity(stmt.Relation.Relname), Fields: fields}, nil
}

func columnDefToField(col *pg_query.ColumnDef) (models.Field, error) {
	typeName := ""
	if col.TypeName != nil && len(col.TypeName.Names) > 0 {
		if str := col.TypeName.Names[len(col.TypeName.Names)-1].GetString_(); str != nil {
			typeName = strings.ToUpper(str.Sval)
		}
	}

	oqlType := GetOQLType(typeName, "PostgreSQL")
	if col.TypeName != nil && len(col.TypeName.Typmods) > 0 {
		if c := col.TypeName.Typmods[0].GetAConst(); c != nil {
			if iv := c.GetIval(); iv != nil {
				oqlType = fmt.Sprintf("%s(%d)", oqlType, iv.Ival)
			}
		}
	}

	var constraints []string
	for _, cons := range col.Constraints {
		if c := cons.GetConstraint(); c != nil {
			switch c.Contype {
			case pg_query.ConstrType_CONSTR_NOTNULL:
				constraints = append(constraints, "NOT_NULL")
			case pg_query.ConstrType_CONSTR_UNIQUE:
				constraints = append(constraints, "UNIQUE")
			case pg_query.ConstrType_CONSTR_PRIMARY:
				constraints = append(constraints, "PRIMARY_KEY")
			case pg_query.ConstrType_CONSTR_DEFAULT:
				constraints = append(constraints, "DEFAULT")
			}
		}
	}
	return models.Field{NameExpr: FieldExpr(col.Colname), ValueExpr: LiteralExpr(oqlType), Constraints: constraints}, nil
}

func convertAlterTable(stmt *pg_query.AlterTableStmt) (*models.Query, error) {
	query := &models.Query{Operation: "ALTER TABLE", Entity: TableToEntity(stmt.Relation.Relname)}
	if len(stmt.Cmds) > 0 {
		if cmd := stmt.Cmds[0].GetAlterTableCmd(); cmd != nil {
			switch cmd.Subtype {
			case pg_query.AlterTableType_AT_AddColumn:
				query.AlterAction = "ADD_COLUMN"
				if colDef := cmd.Def.GetColumnDef(); colDef != nil {
					field, _ := columnDefToField(colDef)
					query.Fields = []models.Field{field}
				}
			case pg_query.AlterTableType_AT_DropColumn:
				query.AlterAction = "DROP_COLUMN"
				query.Fields = []models.Field{{NameExpr: FieldExpr(cmd.Name)}}
			case pg_query.AlterTableType_AT_AlterColumnType:
				query.AlterAction = "MODIFY_COLUMN"
				if colDef := cmd.Def.GetColumnDef(); colDef != nil {
					field, _ := columnDefToField(colDef)
					query.Fields = []models.Field{field}
				}
			}
		}
	}
	return query, nil
}

func convertDropAll(stmt *pg_query.DropStmt) (*models.Query, error) {
	// Route to advanced for non-basic types
	switch stmt.RemoveType {
	case pg_query.ObjectType_OBJECT_SEQUENCE,
		pg_query.ObjectType_OBJECT_SCHEMA,
		pg_query.ObjectType_OBJECT_TYPE,
		pg_query.ObjectType_OBJECT_DOMAIN,
		pg_query.ObjectType_OBJECT_FUNCTION,
		pg_query.ObjectType_OBJECT_TRIGGER,
		pg_query.ObjectType_OBJECT_POLICY,
		pg_query.ObjectType_OBJECT_RULE,
		pg_query.ObjectType_OBJECT_EXTENSION:
		return convertDropAdvanced(stmt)
	}

	// Basic DROP (TABLE, VIEW, INDEX)
	if len(stmt.Objects) == 0 {
		return nil, fmt.Errorf("%w: DROP without objects", ErrParseError)
	}
	objList := stmt.Objects[0].GetList()
	if objList == nil || len(objList.Items) == 0 {
		return nil, fmt.Errorf("%w: DROP without name", ErrParseError)
	}

	name := ""
	if str := objList.Items[len(objList.Items)-1].GetString_(); str != nil {
		name = str.Sval
	}

	var op string
	switch stmt.RemoveType {
	case pg_query.ObjectType_OBJECT_TABLE:
		op = "DROP TABLE"
	case pg_query.ObjectType_OBJECT_VIEW:
		op = "DROP VIEW"
	case pg_query.ObjectType_OBJECT_INDEX:
		op = "DROP INDEX"
	default:
		return nil, fmt.Errorf("%w: unsupported DROP type", ErrNotSupported)
	}
	return &models.Query{Operation: op, Entity: TableToEntity(name)}, nil
}

func convertTruncate(stmt *pg_query.TruncateStmt) (*models.Query, error) {
	if len(stmt.Relations) == 0 {
		return nil, fmt.Errorf("%w: TRUNCATE without table", ErrParseError)
	}
	if rv := stmt.Relations[0].GetRangeVar(); rv != nil {
		return &models.Query{Operation: "TRUNCATE TABLE", Entity: TableToEntity(rv.Relname)}, nil
	}
	return nil, fmt.Errorf("%w: TRUNCATE parse error", ErrParseError)
}

func convertCreateIndex(stmt *pg_query.IndexStmt) (*models.Query, error) {
	var fields []models.Field
	for _, elem := range stmt.IndexParams {
		if ie := elem.GetIndexElem(); ie != nil {
			fields = append(fields, models.Field{NameExpr: FieldExpr(ie.Name)})
		}
	}
	return &models.Query{Operation: "CREATE INDEX", Entity: TableToEntity(stmt.Relation.Relname), Fields: fields, NewName: stmt.Idxname}, nil
}

func convertCreateView(stmt *pg_query.ViewStmt) (*models.Query, error) {
	var viewQuery *models.Query
	if stmt.Query != nil {
		if sel := stmt.Query.GetSelectStmt(); sel != nil {
			viewQuery, _ = convertSelect(sel)
		}
	}
	return &models.Query{Operation: "CREATE VIEW", ViewName: stmt.View.Relname, ViewQuery: viewQuery}, nil
}

// ============================================================================
// TCL: BEGIN, COMMIT, ROLLBACK, SAVEPOINT
// ============================================================================

func convertTransaction(stmt *pg_query.TransactionStmt) (*models.Query, error) {
	query := &models.Query{Transaction: &models.Transaction{}}

	switch stmt.Kind {
	case pg_query.TransactionStmtKind_TRANS_STMT_BEGIN:
		query.Operation, query.Transaction.Operation = "BEGIN", "BEGIN"
	case pg_query.TransactionStmtKind_TRANS_STMT_COMMIT:
		query.Operation, query.Transaction.Operation = "COMMIT", "COMMIT"
	case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK:
		query.Operation, query.Transaction.Operation = "ROLLBACK", "ROLLBACK"
	case pg_query.TransactionStmtKind_TRANS_STMT_SAVEPOINT:
		query.Operation, query.Transaction.Operation = "SAVEPOINT", "SAVEPOINT"
		query.Transaction.SavepointName = extractSavepointName(stmt)
	case pg_query.TransactionStmtKind_TRANS_STMT_RELEASE:
		query.Operation, query.Transaction.Operation = "RELEASE SAVEPOINT", "RELEASE SAVEPOINT"
		query.Transaction.SavepointName = extractSavepointName(stmt)
	case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK_TO:
		query.Operation, query.Transaction.Operation = "ROLLBACK TO", "ROLLBACK TO"
		query.Transaction.SavepointName = extractSavepointName(stmt)
	default:
		return nil, fmt.Errorf("%w: unsupported transaction type", ErrNotSupported)
	}

	for _, opt := range stmt.Options {
		if def := opt.GetDefElem(); def != nil {
			if def.Defname == "transaction_isolation" {
				if str := def.Arg.GetString_(); str != nil {
					query.Transaction.IsolationLevel = str.Sval
				}
			}
			if def.Defname == "transaction_read_only" {
				query.Transaction.ReadOnly = true
			}
		}
	}
	return query, nil
}

func extractSavepointName(stmt *pg_query.TransactionStmt) string {
    return stmt.SavepointName
}

// ============================================================================
// DCL: GRANT, REVOKE, ROLE, USER
// ============================================================================

func convertGrant(stmt *pg_query.GrantStmt) (*models.Query, error) {
	op := "GRANT"
	if !stmt.IsGrant {
		op = "REVOKE"
	}

	var perms []string
	for _, priv := range stmt.Privileges {
		if ap := priv.GetAccessPriv(); ap != nil {
			perms = append(perms, strings.ToUpper(ap.PrivName))
		}
	}

	var entity, target string
	if len(stmt.Objects) > 0 {
		if rv := stmt.Objects[0].GetRangeVar(); rv != nil {
			entity = TableToEntity(rv.Relname)
		}
	}
	if len(stmt.Grantees) > 0 {
		if rs := stmt.Grantees[0].GetRoleSpec(); rs != nil {
			target = rs.Rolename
		}
	}
	return &models.Query{Operation: op, Entity: entity, Permission: &models.Permission{Operation: op, Permissions: perms, Target: target}}, nil
}

func convertCreateRole(stmt *pg_query.CreateRoleStmt) (*models.Query, error) {
	op := "CREATE ROLE"
	if stmt.StmtType == pg_query.RoleStmtType_ROLESTMT_USER {
		op = "CREATE USER"
	}
	query := &models.Query{Operation: op, Permission: &models.Permission{Operation: op, UserName: stmt.Role}}
	for _, opt := range stmt.Options {
		if def := opt.GetDefElem(); def != nil && def.Defname == "password" {
			if str := def.Arg.GetString_(); str != nil {
				query.Permission.Password = str.Sval
			}
		}
	}
	return query, nil
}

func convertDropRole(stmt *pg_query.DropRoleStmt) (*models.Query, error) {
	var userName string
	if len(stmt.Roles) > 0 {
		if rs := stmt.Roles[0].GetRoleSpec(); rs != nil {
			userName = rs.Rolename
		}
	}
	return &models.Query{Operation: "DROP ROLE", Permission: &models.Permission{Operation: "DROP ROLE", UserName: userName}}, nil
}

// ============================================================================
// EXPRESSION CONVERSION
// ============================================================================

func nodeToExpression(node *pg_query.Node) (*models.Expression, error) {
	if node == nil {
		return nil, nil
	}
	switch {
	case node.GetColumnRef() != nil:
		return columnRefToExpr(node.GetColumnRef())
	case node.GetAConst() != nil:
		return constToExpr(node.GetAConst())
	case node.GetAExpr() != nil:
		return aExprToExpression(node.GetAExpr())
	case node.GetFuncCall() != nil:
		return funcCallToExpr(node.GetFuncCall())
	case node.GetTypeCast() != nil:
		return nodeToExpression(node.GetTypeCast().Arg)
	case node.GetParamRef() != nil:
		return LiteralExpr(fmt.Sprintf("$%d", node.GetParamRef().Number)), nil
	case node.GetNullTest() != nil:
		return nullTestToExpr(node.GetNullTest())
	case node.GetCaseExpr() != nil:
		return caseExprToExpression(node.GetCaseExpr()) // advanced
	case node.GetSubLink() != nil:
		return subLinkToExpr(node.GetSubLink()) // advanced
	}
	return nil, fmt.Errorf("%w: unknown expression type", ErrNotSupported)
}

func columnRefToExpr(ref *pg_query.ColumnRef) (*models.Expression, error) {
	var parts []string
	for _, f := range ref.Fields {
		if str := f.GetString_(); str != nil {
			parts = append(parts, str.Sval)
		}
		if f.GetAStar() != nil {
			parts = append(parts, "*")
		}
	}
	return FieldExpr(strings.Join(parts, ".")), nil
}

func constToExpr(c *pg_query.A_Const) (*models.Expression, error) {
	switch {
	case c.GetIval() != nil:
		return LiteralExpr(fmt.Sprintf("%d", c.GetIval().Ival)), nil
	case c.GetFval() != nil:
		return LiteralExpr(c.GetFval().Fval), nil
	case c.GetSval() != nil:
		return LiteralExpr(c.GetSval().Sval), nil
	case c.GetBoolval() != nil:
		if c.GetBoolval().Boolval {
			return LiteralExpr("true"), nil
		}
		return LiteralExpr("false"), nil
	case c.Isnull:
		return LiteralExpr("NULL"), nil
	}
	return LiteralExpr(""), nil
}

func aExprToExpression(expr *pg_query.A_Expr) (*models.Expression, error) {
	left, _ := nodeToExpression(expr.Lexpr)
	right, _ := nodeToExpression(expr.Rexpr)
	op := ""
	if len(expr.Name) > 0 {
		if str := expr.Name[0].GetString_(); str != nil {
			op = str.Sval
		}
	}
	return BinaryExpr(left, op, right), nil
}

func funcCallToExpr(fc *pg_query.FuncCall) (*models.Expression, error) {
	var funcName string
	if len(fc.Funcname) > 0 {
		if str := fc.Funcname[len(fc.Funcname)-1].GetString_(); str != nil {
			funcName = strings.ToUpper(str.Sval)
		}
	}
	var args []*models.Expression
	for _, arg := range fc.Args {
		expr, _ := nodeToExpression(arg)
		args = append(args, expr)
	}
	if fc.AggStar {
		args = []*models.Expression{FieldExpr("*")}
	}
	// Window function (advanced)
	if fc.Over != nil {
		return windowFuncToExpr(funcName, args, fc.Over)
	}
	return FunctionExpr(funcName, args...), nil
}

func nullTestToExpr(nt *pg_query.NullTest) (*models.Expression, error) {
	arg, _ := nodeToExpression(nt.Arg)
	op := "IS_NULL"
	if nt.Nulltesttype == pg_query.NullTestType_IS_NOT_NULL {
		op = "IS_NOT_NULL"
	}
	return &models.Expression{Type: "BINARY", Left: arg, Operator: op}, nil
}

// ============================================================================
// CONDITIONS - ALL 15 OPERATORS
// ============================================================================

func nodeToConditions(node *pg_query.Node) ([]models.Condition, error) {
	if node == nil {
		return nil, nil
	}
	if be := node.GetBoolExpr(); be != nil {
		return boolExprToConditions(be)
	}
	cond, err := nodeToSingleCondition(node)
	if err != nil {
		return nil, err
	}
	return []models.Condition{*cond}, nil
}

func nodeToSingleCondition(node *pg_query.Node) (*models.Condition, error) {
	if node == nil {
		return nil, nil
	}
	if expr := node.GetAExpr(); expr != nil {
		return aExprToCondition(expr)
	}
	if nt := node.GetNullTest(); nt != nil {
		return nullTestToCondition(nt)
	}
	if be := node.GetBoolExpr(); be != nil {
		conds, _ := boolExprToConditions(be)
		if len(conds) > 0 {
			return &models.Condition{Nested: conds}, nil
		}
	}
	return nil, fmt.Errorf("%w: unknown condition type", ErrNotSupported)
}

func aExprToCondition(expr *pg_query.A_Expr) (*models.Condition, error) {
	field, _ := nodeToExpression(expr.Lexpr)
	op := ""
	
	// Get operator name
	if len(expr.Name) > 0 {
		if str := expr.Name[0].GetString_(); str != nil {
			op = str.Sval
		}
	}
	cond := &models.Condition{FieldExpr: field}

	switch expr.Kind {
	case pg_query.A_Expr_Kind_AEXPR_IN:
		if op == "<>" {
			cond.Operator = "NOT_IN"
		} else {
			cond.Operator = "IN"
		}
		if list := expr.Rexpr.GetList(); list != nil {
			for _, item := range list.Items {
				val, _ := nodeToExpression(item)
				cond.ValuesExpr = append(cond.ValuesExpr, val)
			}
		}
		return cond, nil
		
	case pg_query.A_Expr_Kind_AEXPR_BETWEEN:
		cond.Operator = "BETWEEN"
		if list := expr.Rexpr.GetList(); list != nil && len(list.Items) == 2 {
			cond.ValueExpr, _ = nodeToExpression(list.Items[0])
			cond.Value2Expr, _ = nodeToExpression(list.Items[1])
		}
		return cond, nil
		
	case pg_query.A_Expr_Kind_AEXPR_NOT_BETWEEN:
		cond.Operator = "NOT_BETWEEN"
		if list := expr.Rexpr.GetList(); list != nil && len(list.Items) == 2 {
			cond.ValueExpr, _ = nodeToExpression(list.Items[0])
			cond.Value2Expr, _ = nodeToExpression(list.Items[1])
		}
		return cond, nil
		
	case pg_query.A_Expr_Kind_AEXPR_LIKE, pg_query.A_Expr_Kind_AEXPR_ILIKE:
		// !~~ = NOT LIKE, ~~ = LIKE, !~~* = NOT ILIKE, ~~* = ILIKE
		if op == "!~~" {
			cond.Operator = "NOT_LIKE"
		} else if op == "!~~*" {
			cond.Operator = "NOT_ILIKE"
		} else if op == "~~*" {
			cond.Operator = "ILIKE"
		} else {
			cond.Operator = "LIKE"
		}
		cond.ValueExpr, _ = nodeToExpression(expr.Rexpr)
		return cond, nil
	}

	// Default: regular comparison operators (=, <>, >, <, >=, <=)
	cond.Operator = GetOQLOperator(op, "PostgreSQL")
	cond.ValueExpr, _ = nodeToExpression(expr.Rexpr)
	return cond, nil
}


func boolExprToConditions(be *pg_query.BoolExpr) ([]models.Condition, error) {
	var conds []models.Condition
	logic := ""
	switch be.Boolop {
	case pg_query.BoolExprType_AND_EXPR:
		logic = "AND"
	case pg_query.BoolExprType_OR_EXPR:
		logic = "OR"
	case pg_query.BoolExprType_NOT_EXPR:
		if len(be.Args) > 0 {
			c, err := nodeToSingleCondition(be.Args[0])
			if err != nil {
				return nil, err
			}
			// Convert to underscore format: IN → NOT_IN, LIKE → NOT_LIKE, BETWEEN → NOT_BETWEEN
			c.Operator = "NOT_" + c.Operator
			return []models.Condition{*c}, nil
		}
	}
	for i, arg := range be.Args {
		c, _ := nodeToSingleCondition(arg)
		if i > 0 {
			c.Logic = logic
		}
		conds = append(conds, *c)
	}
	return conds, nil
}

func nullTestToCondition(nt *pg_query.NullTest) (*models.Condition, error) {
	field, _ := nodeToExpression(nt.Arg)
	op := "IS_NULL"
	if nt.Nulltesttype == pg_query.NullTestType_IS_NOT_NULL {
		op = "IS_NOT_NULL"
	}
	return &models.Condition{FieldExpr: field, Operator: op}, nil
}

// ============================================================================
// HELPERS
// ============================================================================

func extractFromClause(from []*pg_query.Node) (string, []models.Join, error) {
	if len(from) == 0 {
		return "", nil, fmt.Errorf("%w: empty FROM", ErrParseError)
	}
	node := from[0]
	if rv := node.GetRangeVar(); rv != nil {
		return TableToEntity(rv.Relname), nil, nil
	}
	if je := node.GetJoinExpr(); je != nil {
		return extractJoin(je)
	}
	return "", nil, fmt.Errorf("%w: unsupported FROM", ErrNotSupported)
}

func extractJoin(je *pg_query.JoinExpr) (string, []models.Join, error) {
	var entity string
	if rv := je.Larg.GetRangeVar(); rv != nil {
		entity = TableToEntity(rv.Relname)
	} else if n := je.Larg.GetJoinExpr(); n != nil {
		entity, _, _ = extractJoin(n)
	}

	var joinTable string
	if rv := je.Rarg.GetRangeVar(); rv != nil {
		joinTable = TableToEntity(rv.Relname)
	}

	jt := models.InnerJoin
	switch je.Jointype {
	case pg_query.JoinType_JOIN_LEFT:
		jt = models.LeftJoin
	case pg_query.JoinType_JOIN_RIGHT:
		jt = models.RightJoin
	case pg_query.JoinType_JOIN_FULL:
		jt = models.FullJoin
	}

	var left, right *models.Expression
	if je.Quals != nil {
		if a := je.Quals.GetAExpr(); a != nil {
			left, _ = nodeToExpression(a.Lexpr)
			right, _ = nodeToExpression(a.Rexpr)
		}
	}
	return entity, []models.Join{{Type: jt, Table: joinTable, LeftExpr: left, RightExpr: right}}, nil
}

func extractAggregate(targets []*pg_query.Node) *models.Aggregation {
	for _, t := range targets {
		if rt := t.GetResTarget(); rt != nil {
			if fc := rt.Val.GetFuncCall(); fc != nil && len(fc.Funcname) > 0 {
				if str := fc.Funcname[0].GetString_(); str != nil {
					fn := strings.ToUpper(str.Sval)
					if fn == "COUNT" || fn == "SUM" || fn == "AVG" || fn == "MIN" || fn == "MAX" {
						agg := &models.Aggregation{Function: models.AggregateFunc(fn)}
						if fc.AggStar {
							agg.FieldExpr = FieldExpr("*")
						} else if len(fc.Args) > 0 {
							agg.FieldExpr, _ = nodeToExpression(fc.Args[0])
						}
						return agg
					}
				}
			}
		}
	}
	return nil
}

func extractColumns(targets []*pg_query.Node) []*models.Expression {
	var cols []*models.Expression
	for _, t := range targets {
		if rt := t.GetResTarget(); rt != nil {
			if rt.Val.GetFuncCall() != nil {
				continue
			}
			if ref := rt.Val.GetColumnRef(); ref != nil {
				for _, f := range ref.Fields {
					if f.GetAStar() != nil {
						return nil
					}
				}
			}
			if e, err := nodeToExpression(rt.Val); err == nil && e != nil {
				cols = append(cols, e)
			}
		}
	}
	return cols
}

func extractOrderBy(sort []*pg_query.Node) ([]models.OrderBy, error) {
	var ob []models.OrderBy
	for _, n := range sort {
		if sb := n.GetSortBy(); sb != nil {
			e, _ := nodeToExpression(sb.Node)
			dir := models.Asc
			if sb.SortbyDir == pg_query.SortByDir_SORTBY_DESC {
				dir = models.Desc
			}
			ob = append(ob, models.OrderBy{FieldExpr: e, Direction: dir})
		}
	}
	return ob, nil
}

func buildFields(cols []string, vals []*pg_query.Node) ([]models.Field, error) {
	if len(cols) != len(vals) {
		return nil, fmt.Errorf("%w: column/value mismatch", ErrParseError)
	}
	var fields []models.Field
	for i, c := range cols {
		v, _ := nodeToExpression(vals[i])
		fields = append(fields, models.Field{NameExpr: FieldExpr(c), ValueExpr: v})
	}
	return fields, nil
}

func nodeToInt(node *pg_query.Node) (int, error) {
	if c := node.GetAConst(); c != nil {
		if c.GetIval() != nil {
			return int(c.GetIval().Ival), nil
		}
	}
	return 0, fmt.Errorf("%w: expected integer", ErrParseError)
}