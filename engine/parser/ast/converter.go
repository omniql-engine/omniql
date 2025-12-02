package ast

import (
	"github.com/omniql-engine/omniql/engine/models"
)

// Convert transforms AST QueryNode to models.Query
func Convert(node *QueryNode) (*models.Query, error) {
	query := &models.Query{
		Operation:    node.Operation,
		Entity:       node.Entity,
		Distinct:     node.Distinct,
		Columns:      node.Columns,
		DatabaseName: node.DatabaseName,
		ViewName:     node.ViewName,
		ViewQuery:    node.ViewQuery,
		NewName:      node.NewName,
	}
	
	// Convert fields
	if len(node.Fields) > 0 {
		query.Fields = convertFields(node.Fields)
	}
	
	// Convert conditions
	if node.Conditions != nil {
		query.Conditions = convertConditions(node.Conditions.Conditions)
	}
	
	// Convert ORDER BY
	if len(node.OrderBy) > 0 {
		query.OrderBy = convertOrderBy(node.OrderBy)
	}
	
	// Convert LIMIT/OFFSET
	if node.Limit != nil {
		query.Limit = *node.Limit
	}
	if node.Offset != nil {
		query.Offset = *node.Offset
	}
	
	// Convert JOINs
	if len(node.Joins) > 0 {
		query.Joins = convertJoins(node.Joins)
	}
	
	// Convert Aggregate
	if node.Aggregate != nil {
		query.Aggregate = convertAggregate(node.Aggregate)
	}
	
	// Convert GROUP BY
	if len(node.GroupBy) > 0 {
		query.GroupBy = node.GroupBy
	}
	
	// Convert HAVING
	if len(node.Having) > 0 {
		query.Having = convertConditions(node.Having)
	}
	
	// Convert Window Functions
	if len(node.WindowFunctions) > 0 {
		query.WindowFunctions = convertWindowFunctions(node.WindowFunctions)
	}
	
	// Convert Set Operation
	if node.SetOperation != nil {
		query.SetOperation = convertSetOperation(node.SetOperation)
	}
	
	// Convert Upsert
	if node.Upsert != nil {
		query.Upsert = convertUpsert(node.Upsert)
	}
	
	// Convert Bulk Data
	if len(node.BulkData) > 0 {
		query.BulkData = convertBulkData(node.BulkData)
	}
	
	// Convert Transaction
	if node.Transaction != nil {
		query.Transaction = convertTransaction(node.Transaction)
	}
	
	// Convert Permission
	if node.Permission != nil {
		query.Permission = convertPermission(node.Permission)
	}
	
	return query, nil
}

func convertFields(nodes []FieldNode) []models.Field {
	fields := make([]models.Field, len(nodes))
	for i, n := range nodes {
		fields[i] = models.Field{
			Name:        n.Name,
			Value:       n.Value,
			Constraints: n.Constraints,
		}
		if n.Expression != nil {
			fields[i].Expression = convertExpression(n.Expression)
		}
	}
	return fields
}

func convertExpression(node *ExpressionNode) *models.FieldExpression {
	if node == nil {
		return nil
	}
	
	expr := &models.FieldExpression{
		Type:         node.Type,
		LeftOperand:  node.LeftOperand,
		Operator:     node.Operator,
		RightOperand: node.RightOperand,
		LeftIsField:  node.LeftIsField,
		RightIsField: node.RightIsField,
		FunctionName: node.FunctionName,
		FunctionArgs: node.FunctionArgs,
		CaseElse:     node.CaseElse,
	}
	
	for _, cc := range node.CaseConditions {
		expr.CaseConditions = append(expr.CaseConditions, models.CaseCondition{
			Condition: cc.Condition,
			ThenValue: cc.ThenValue,
		})
	}
	
	return expr
}

func convertConditions(nodes []ConditionNode) []models.Condition {
	conditions := make([]models.Condition, len(nodes))
	for i, n := range nodes {
		conditions[i] = models.Condition{
			Field:    n.Field,
			Operator: n.Operator,
			Value:    n.Value,
			Value2:   n.Value2,
			Values:   n.Values,
			Logic:    n.Logic,
		}
		if len(n.Nested) > 0 {
			conditions[i].Conditions = convertConditions(n.Nested)
		}
	}
	return conditions
}

func convertOrderBy(nodes []OrderByNode) []models.OrderBy {
	orderBy := make([]models.OrderBy, len(nodes))
	for i, n := range nodes {
		orderBy[i] = models.OrderBy{
			Field:     n.Field,
			Direction: models.SortDirection(n.Direction),
		}
		if n.Expression != nil {
			orderBy[i].Expression = convertExpression(n.Expression)
		}
	}
	return orderBy
}

func convertJoins(nodes []JoinNode) []models.Join {
	joins := make([]models.Join, len(nodes))
	for i, n := range nodes {
		joins[i] = models.Join{
			Type:       models.JoinType(n.Type),
			Table:      n.Table,
			LeftField:  n.LeftField,
			RightField: n.RightField,
		}
	}
	return joins
}

func convertAggregate(node *AggregateNode) *models.Aggregation {
	return &models.Aggregation{
		Function: models.AggregateFunc(node.Function),
		Field:    node.Field,
	}
}

func convertWindowFunctions(nodes []WindowNode) []models.WindowFunction {
	funcs := make([]models.WindowFunction, len(nodes))
	for i, n := range nodes {
		funcs[i] = models.WindowFunction{
			Function:    models.WindowFunc(n.Function),
			Field:       n.Field,
			Alias:       n.Alias,
			PartitionBy: n.PartitionBy,
			Offset:      n.Offset,
			Buckets:     n.Buckets,
		}
		if len(n.OrderBy) > 0 {
			funcs[i].OrderBy = convertOrderBy(n.OrderBy)
		}
	}
	return funcs
}

func convertSetOperation(node *SetOperationNode) *models.SetOperation {
	setOp := &models.SetOperation{
		Type: models.SetOperationType(node.Type),
	}
	
	if node.LeftQuery != nil {
		leftQuery, _ := Convert(node.LeftQuery)
		setOp.LeftQuery = leftQuery
	}
	
	if node.RightQuery != nil {
		rightQuery, _ := Convert(node.RightQuery)
		setOp.RightQuery = rightQuery
	}
	
	return setOp
}

func convertUpsert(node *UpsertNode) *models.Upsert {
	return &models.Upsert{
		ConflictFields: node.ConflictFields,
		UpdateFields:   convertFields(node.UpdateFields),
	}
}

func convertBulkData(data [][]FieldNode) [][]models.Field {
	result := make([][]models.Field, len(data))
	for i, row := range data {
		result[i] = convertFields(row)
	}
	return result
}

func convertTransaction(node *TransactionNode) *models.Transaction {
	return &models.Transaction{
		Operation:      node.Operation,
		SavepointName:  node.SavepointName,
		IsolationLevel: node.IsolationLevel,
		ReadOnly:       node.ReadOnly,
	}
}

func convertPermission(node *PermissionNode) *models.Permission {
	return &models.Permission{
		Operation:   node.Operation,
		Permissions: node.Permissions,
		Target:      node.Target,
		RoleName:    node.RoleName,
		UserName:    node.UserName,
		Password:    node.Password,
		Roles:       node.Roles,
	}
}