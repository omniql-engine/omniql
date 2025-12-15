package reverse

import (
	"fmt"
	"strings"

	"github.com/omniql-engine/omniql/engine/models"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

// ============================================================================
// DQL: SET OPERATIONS (UNION/INTERSECT/EXCEPT)
// ============================================================================

func convertSetOperation(stmt *pg_query.SelectStmt) (*models.Query, error) {
	var opType models.SetOperationType
	switch stmt.Op {
	case pg_query.SetOperation_SETOP_UNION:
		if stmt.All {
			opType = models.UnionAll
		} else {
			opType = models.Union
		}
	case pg_query.SetOperation_SETOP_INTERSECT:
		opType = models.Intersect
	case pg_query.SetOperation_SETOP_EXCEPT:
		opType = models.Except
	default:
		return nil, fmt.Errorf("%w: unsupported set operation", ErrNotSupported)
	}

	leftQuery, err := convertSelect(stmt.Larg)
	if err != nil {
		return nil, fmt.Errorf("left query: %w", err)
	}
	rightQuery, err := convertSelect(stmt.Rarg)
	if err != nil {
		return nil, fmt.Errorf("right query: %w", err)
	}

	return &models.Query{
		Operation:    "GET",
		Entity:       leftQuery.Entity,  // ← ADD THIS LINE
		SetOperation: &models.SetOperation{
			Type:       opType,
			LeftQuery:  leftQuery,
			RightQuery: rightQuery,
		},
	}, nil
}

// ============================================================================
// DQL: CTE (WITH clause)
// ============================================================================

func extractCTE(with *pg_query.WithClause, mainQuery *models.Query) (*models.CTE, error) {
	if len(with.Ctes) == 0 {
		return nil, nil
	}
	cte := with.Ctes[0].GetCommonTableExpr()
	if cte == nil {
		return nil, nil
	}

	var cteQuery *models.Query
	if cte.Ctequery != nil {
		if sel := cte.Ctequery.GetSelectStmt(); sel != nil {
			cteQuery, _ = convertSelect(sel)
		}
	}
	return &models.CTE{Name: cte.Ctename, Query: cteQuery, MainQuery: mainQuery, Recursive: with.Recursive}, nil
}

// ============================================================================
// DQL: WINDOW FUNCTIONS
// ============================================================================

func extractWindowFunctions(targets []*pg_query.Node) []models.WindowFunction {
	var wfs []models.WindowFunction
	for _, t := range targets {
		rt := t.GetResTarget()
		if rt == nil {
			continue
		}
		fc := rt.Val.GetFuncCall()
		if fc == nil || fc.Over == nil {
			continue
		}

		var funcName string
		if len(fc.Funcname) > 0 {
			if str := fc.Funcname[0].GetString_(); str != nil {
				funcName = strings.ToUpper(str.Sval)
			}
		}

		wf := models.WindowFunction{Function: models.WindowFunc(funcName), Alias: rt.Name}

		if len(fc.Args) > 0 {
			wf.FieldExpr, _ = nodeToExpression(fc.Args[0])
		}

		for _, p := range fc.Over.PartitionClause {
			e, _ := nodeToExpression(p)
			wf.PartitionBy = append(wf.PartitionBy, e)
		}

		for _, o := range fc.Over.OrderClause {
			if sb := o.GetSortBy(); sb != nil {
				e, _ := nodeToExpression(sb.Node)
				dir := models.Asc
				if sb.SortbyDir == pg_query.SortByDir_SORTBY_DESC {
					dir = models.Desc
				}
				wf.OrderBy = append(wf.OrderBy, models.OrderBy{FieldExpr: e, Direction: dir})
			}
		}
		wfs = append(wfs, wf)
	}
	return wfs
}

func windowFuncToExpr(funcName string, args []*models.Expression, over *pg_query.WindowDef) (*models.Expression, error) {
	expr := &models.Expression{Type: "WINDOW", FunctionName: funcName, FunctionArgs: args}

	for _, p := range over.PartitionClause {
		e, _ := nodeToExpression(p)
		expr.PartitionBy = append(expr.PartitionBy, e)
	}

	for _, o := range over.OrderClause {
		if sb := o.GetSortBy(); sb != nil {
			e, _ := nodeToExpression(sb.Node)
			dir := models.Asc
			if sb.SortbyDir == pg_query.SortByDir_SORTBY_DESC {
				dir = models.Desc
			}
			expr.WindowOrderBy = append(expr.WindowOrderBy, models.OrderBy{FieldExpr: e, Direction: dir})
		}
	}
	return expr, nil
}

// ============================================================================
// DQL: CASE EXPRESSION
// ============================================================================

func caseExprToExpression(ce *pg_query.CaseExpr) (*models.Expression, error) {
	expr := &models.Expression{Type: "CASEWHEN"}

	for _, when := range ce.Args {
		if cw := when.GetCaseWhen(); cw != nil {
			cond, _ := nodeToSingleCondition(cw.Expr)
			thenExpr, _ := nodeToExpression(cw.Result)
			expr.CaseConditions = append(expr.CaseConditions, &models.CaseCondition{Condition: cond, ThenExpr: thenExpr})
		}
	}

	if ce.Defresult != nil {
		expr.CaseElse, _ = nodeToExpression(ce.Defresult)
	}
	return expr, nil
}

// ============================================================================
// DQL: SUBQUERY / EXISTS
// ============================================================================

func subLinkToExpr(sl *pg_query.SubLink) (*models.Expression, error) {
	expr := &models.Expression{Type: "SUBQUERY"}

	if sl.SubLinkType == pg_query.SubLinkType_EXISTS_SUBLINK {
		expr.Value = "EXISTS"
	} else {
		expr.Value = "SUBQUERY"
	}
	return expr, nil
}

// ============================================================================
// DDL: SEQUENCE
// ============================================================================

func convertCreateSequence(stmt *pg_query.CreateSeqStmt) (*models.Query, error) {
	name := ""
	if stmt.Sequence != nil {
		name = stmt.Sequence.Relname
	}
	return &models.Query{Operation: "CREATE SEQUENCE", Entity: name}, nil
}

func convertAlterSequence(stmt *pg_query.AlterSeqStmt) (*models.Query, error) {
	name := ""
	if stmt.Sequence != nil {
		name = stmt.Sequence.Relname
	}
	return &models.Query{Operation: "ALTER SEQUENCE", Entity: name}, nil
}

// ============================================================================
// DDL: SCHEMA
// ============================================================================

func convertCreateSchema(stmt *pg_query.CreateSchemaStmt) (*models.Query, error) {
	return &models.Query{Operation: "CREATE SCHEMA", DatabaseName: stmt.Schemaname}, nil
}

// ============================================================================
// DDL: TYPE
// ============================================================================

func convertCreateType(stmt *pg_query.CompositeTypeStmt) (*models.Query, error) {
	name := ""
	if stmt.Typevar != nil {
		name = stmt.Typevar.Relname
	}
	return &models.Query{Operation: "CREATE TYPE", Entity: name}, nil
}

func convertCreateEnum(stmt *pg_query.CreateEnumStmt) (*models.Query, error) {
	var name string
	if len(stmt.TypeName) > 0 {
		if str := stmt.TypeName[len(stmt.TypeName)-1].GetString_(); str != nil {
			name = str.Sval
		}
	}
	return &models.Query{Operation: "CREATE TYPE", Entity: name}, nil
}

func convertAlterType(stmt *pg_query.AlterEnumStmt) (*models.Query, error) {
	var name string
	if len(stmt.TypeName) > 0 {
		if str := stmt.TypeName[len(stmt.TypeName)-1].GetString_(); str != nil {
			name = str.Sval
		}
	}
	return &models.Query{Operation: "ALTER TYPE", Entity: name}, nil
}

// ============================================================================
// DDL: DOMAIN
// ============================================================================

func convertCreateDomain(stmt *pg_query.CreateDomainStmt) (*models.Query, error) {
	var name string
	if len(stmt.Domainname) > 0 {
		if str := stmt.Domainname[len(stmt.Domainname)-1].GetString_(); str != nil {
			name = str.Sval
		}
	}
	return &models.Query{Operation: "CREATE DOMAIN", Entity: name}, nil
}

// ============================================================================
// DDL: FUNCTION
// ============================================================================

func convertCreateFunction(stmt *pg_query.CreateFunctionStmt) (*models.Query, error) {
	var name string
	if len(stmt.Funcname) > 0 {
		if str := stmt.Funcname[len(stmt.Funcname)-1].GetString_(); str != nil {
			name = str.Sval
		}
	}
	return &models.Query{Operation: "CREATE FUNCTION", Entity: name}, nil
}

func convertAlterFunction(stmt *pg_query.AlterFunctionStmt) (*models.Query, error) {
	var name string
	if stmt.Func != nil && len(stmt.Func.Objname) > 0 {
		if str := stmt.Func.Objname[len(stmt.Func.Objname)-1].GetString_(); str != nil {
			name = str.Sval
		}
	}
	return &models.Query{Operation: "ALTER FUNCTION", Entity: name}, nil
}

// ============================================================================
// DDL: TRIGGER
// ============================================================================

func convertCreateTrigger(stmt *pg_query.CreateTrigStmt) (*models.Query, error) {
	entity := ""
	if stmt.Relation != nil {
		entity = TableToEntity(stmt.Relation.Relname)
	}
	return &models.Query{Operation: "CREATE TRIGGER", Entity: entity, NewName: stmt.Trigname}, nil
}

// ============================================================================
// DDL: POLICY
// ============================================================================

func convertCreatePolicy(stmt *pg_query.CreatePolicyStmt) (*models.Query, error) {
	entity := ""
	if stmt.Table != nil {
		entity = TableToEntity(stmt.Table.Relname)
	}
	return &models.Query{Operation: "CREATE POLICY", Entity: entity, NewName: stmt.PolicyName}, nil
}

// ============================================================================
// DDL: RULE
// ============================================================================

func convertCreateRule(stmt *pg_query.RuleStmt) (*models.Query, error) {
	entity := ""
	if stmt.Relation != nil {
		entity = TableToEntity(stmt.Relation.Relname)
	}
	return &models.Query{Operation: "CREATE RULE", Entity: entity, NewName: stmt.Rulename}, nil
}

// ============================================================================
// DDL: EXTENSION
// ============================================================================

func convertCreateExtension(stmt *pg_query.CreateExtensionStmt) (*models.Query, error) {
	return &models.Query{Operation: "CREATE EXTENSION", Entity: stmt.Extname}, nil
}

// ============================================================================
// DDL: COMMENT ON
// ============================================================================

func convertComment(stmt *pg_query.CommentStmt) (*models.Query, error) {
	return &models.Query{Operation: "COMMENT ON"}, nil
}

// ============================================================================
// DDL: RENAME TABLE
// ============================================================================

func convertRenameTable(stmt *pg_query.RenameStmt) (*models.Query, error) {
	entity := ""
	if stmt.Relation != nil {
		entity = TableToEntity(stmt.Relation.Relname)
	}
	return &models.Query{Operation: "RENAME TABLE", Entity: entity, NewName: stmt.Newname}, nil
}

// ============================================================================
// DDL: ALTER VIEW
// ============================================================================

func convertAlterView(stmt *pg_query.AlterTableStmt) (*models.Query, error) {
	entity := ""
	if stmt.Relation != nil {
		entity = stmt.Relation.Relname
	}
	return &models.Query{Operation: "ALTER VIEW", ViewName: entity}, nil
}

// ============================================================================
// DDL: DATABASE
// ============================================================================

func convertCreateDatabase(stmt *pg_query.CreatedbStmt) (*models.Query, error) {
	return &models.Query{Operation: "CREATE DATABASE", DatabaseName: stmt.Dbname}, nil
}

func convertDropDatabase(stmt *pg_query.DropdbStmt) (*models.Query, error) {
	return &models.Query{Operation: "DROP DATABASE", DatabaseName: stmt.Dbname}, nil
}

// ============================================================================
// DDL: DROP ADVANCED (SEQUENCE, SCHEMA, TYPE, DOMAIN, FUNCTION, TRIGGER, etc.)
// ============================================================================

func convertDropAdvanced(stmt *pg_query.DropStmt) (*models.Query, error) {
	if len(stmt.Objects) == 0 {
		return nil, fmt.Errorf("%w: DROP without objects", ErrParseError)
	}

	name := extractDropName(stmt.Objects[0])

	var op string
	switch stmt.RemoveType {
	case pg_query.ObjectType_OBJECT_SEQUENCE:
		op = "DROP SEQUENCE"
	case pg_query.ObjectType_OBJECT_SCHEMA:
		op = "DROP SCHEMA"
	case pg_query.ObjectType_OBJECT_TYPE:
		op = "DROP TYPE"
	case pg_query.ObjectType_OBJECT_DOMAIN:
		op = "DROP DOMAIN"
	case pg_query.ObjectType_OBJECT_FUNCTION:
		op = "DROP FUNCTION"
	case pg_query.ObjectType_OBJECT_TRIGGER:
		op = "DROP TRIGGER"
	case pg_query.ObjectType_OBJECT_POLICY:
		op = "DROP POLICY"
	case pg_query.ObjectType_OBJECT_RULE:
		op = "DROP RULE"
	case pg_query.ObjectType_OBJECT_EXTENSION:
		op = "DROP EXTENSION"
	default:
		return nil, fmt.Errorf("%w: unsupported DROP type", ErrNotSupported)
	}
	return &models.Query{Operation: op, Entity: name}, nil
}

func extractDropName(obj *pg_query.Node) string {
    // Try List first (most DROP statements)
    if list := obj.GetList(); list != nil && len(list.Items) > 0 {
        if str := list.Items[len(list.Items)-1].GetString_(); str != nil {
            return str.Sval
        }
    }
    // Try direct String
    if str := obj.GetString_(); str != nil {
        return str.Sval
    }
    // Try TypeName for TYPE/DOMAIN
    if tn := obj.GetTypeName(); tn != nil && len(tn.Names) > 0 {
        if str := tn.Names[len(tn.Names)-1].GetString_(); str != nil {
            return str.Sval
        }
    }
    // Try ObjectWithArgs for FUNCTION
    if owa := obj.GetObjectWithArgs(); owa != nil && len(owa.Objname) > 0 {
        if str := owa.Objname[len(owa.Objname)-1].GetString_(); str != nil {
            return str.Sval
        }
    }
    return ""
}

// ============================================================================
// TCL: SET TRANSACTION, START
// ============================================================================

func convertSetTransaction(stmt *pg_query.VariableSetStmt) (*models.Query, error) {
	query := &models.Query{
		Operation:   "SET TRANSACTION",
		Transaction: &models.Transaction{Operation: "SET TRANSACTION"},
	}

	if stmt.Name == "transaction_isolation" && len(stmt.Args) > 0 {
		if c := stmt.Args[0].GetAConst(); c != nil {
			if str := c.GetSval(); str != nil {
				query.Transaction.IsolationLevel = str.Sval
			}
		}
	}
	if stmt.Name == "transaction_read_only" {
		query.Transaction.ReadOnly = true
	}
	return query, nil
}

// ============================================================================
// DCL: ASSIGN ROLE, REVOKE ROLE
// ============================================================================

func convertGrantRole(stmt *pg_query.GrantRoleStmt) (*models.Query, error) {
	op := "ASSIGN ROLE"
	if !stmt.IsGrant {
		op = "REVOKE ROLE"
	}

	var roleName, userName string
	if len(stmt.GrantedRoles) > 0 {
		if ar := stmt.GrantedRoles[0].GetAccessPriv(); ar != nil {
			roleName = ar.PrivName
		}
	}
	if len(stmt.GranteeRoles) > 0 {
		if rs := stmt.GranteeRoles[0].GetRoleSpec(); rs != nil {
			userName = rs.Rolename
		}
	}

	return &models.Query{
		Operation:  op,
		Permission: &models.Permission{Operation: op, RoleName: roleName, UserName: userName},
	}, nil
}

// ============================================================================
// DCL: ALTER USER
// ============================================================================

func convertAlterRole(stmt *pg_query.AlterRoleStmt) (*models.Query, error) {
	userName := ""
	if stmt.Role != nil {
		userName = stmt.Role.Rolename
	}

	query := &models.Query{
		Operation:  "ALTER USER",
		Permission: &models.Permission{Operation: "ALTER USER", UserName: userName},
	}

	for _, opt := range stmt.Options {
		if def := opt.GetDefElem(); def != nil && def.Defname == "password" {
			if str := def.Arg.GetString_(); str != nil {
				query.Permission.Password = str.Sval
			}
		}
	}
	return query, nil
}