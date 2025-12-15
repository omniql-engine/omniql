package reverse

import (
	"strings"

	"github.com/omniql-engine/omniql/engine/models"
)

// ============================================================================
// ADVANCED AGGREGATION PIPELINE PROCESSING
// Called from convertMongoAggregate in mongodb.go for advanced stages
// Only covers features in OQL mapping: Window Functions, Set Operations, CASE
// ============================================================================

// ProcessAdvancedPipelineStages handles advanced aggregation stages
func ProcessAdvancedPipelineStages(query *models.Query, pipeline []interface{}) bool {
	hasAdvanced := false

	for _, stage := range pipeline {
		stageMap, ok := stage.(map[string]interface{})
		if !ok {
			continue
		}

		// $setWindowFields → Window Functions
		if windowFields, ok := stageMap["$setWindowFields"].(map[string]interface{}); ok {
			convertWindowFields(query, windowFields)
			hasAdvanced = true
		}

		// $unionWith → UNION
		if unionWith, ok := stageMap["$unionWith"].(map[string]interface{}); ok {
			convertUnionWith(query, unionWith)
			hasAdvanced = true
		}
		// $unionWith can also be a string (simple form)
		if unionColl, ok := stageMap["$unionWith"].(string); ok {
			query.SetOperation = &models.SetOperation{
				Type:       models.Union,
				RightQuery: &models.Query{Operation: "GET", Entity: TableToEntity(unionColl)},
			}
			hasAdvanced = true
		}
	}

	return hasAdvanced
}

// ============================================================================
// WINDOW FUNCTIONS ($setWindowFields)
// Mapping: ROW NUMBER, RANK, DENSE RANK, LAG, LEAD, NTILE
// ============================================================================

func convertWindowFields(query *models.Query, windowFields map[string]interface{}) {
	// partitionBy
	var partitionBy []*models.Expression
	if partBy := windowFields["partitionBy"]; partBy != nil {
		switch p := partBy.(type) {
		case string:
			if strings.HasPrefix(p, "$") {
				partitionBy = append(partitionBy, FieldExpr(strings.TrimPrefix(p, "$")))
			}
		case map[string]interface{}:
			for _, v := range p {
				if fieldStr, ok := v.(string); ok && strings.HasPrefix(fieldStr, "$") {
					partitionBy = append(partitionBy, FieldExpr(strings.TrimPrefix(fieldStr, "$")))
				}
			}
		}
	}

	// sortBy → OrderBy for window
	var windowOrderBy []models.OrderBy
	if sortBy, ok := windowFields["sortBy"].(map[string]interface{}); ok {
		windowOrderBy = convertMongoSort(sortBy)
	}

	// output → Window function definitions
	if output, ok := windowFields["output"].(map[string]interface{}); ok {
		for alias, def := range output {
			if defMap, ok := def.(map[string]interface{}); ok {
				wf := convertWindowFunctionDef(alias, defMap, partitionBy, windowOrderBy)
				if wf != nil {
					query.WindowFunctions = append(query.WindowFunctions, *wf)
				}
			}
		}
	}
}

func convertWindowFunctionDef(alias string, def map[string]interface{}, partitionBy []*models.Expression, orderBy []models.OrderBy) *models.WindowFunction {
	wf := &models.WindowFunction{
		Alias:       alias,
		PartitionBy: partitionBy,
		OrderBy:     orderBy,
	}

	for op, val := range def {
		switch op {
		case "$documentNumber":
			wf.Function = models.WindowFunc("ROW NUMBER")
			return wf

		case "$rank":
			wf.Function = models.WindowFunc("RANK")
			return wf

		case "$denseRank":
			wf.Function = models.WindowFunc("DENSE RANK")
			return wf

		case "$ntile":
			wf.Function = models.WindowFunc("NTILE")
			if ntileOpts, ok := val.(map[string]interface{}); ok {
				if n, ok := ntileOpts["n"].(float64); ok {
					wf.Buckets = int(n)
				}
			} else if n, ok := val.(float64); ok {
				wf.Buckets = int(n)
			}
			return wf

		case "$shift":
			if shiftDef, ok := val.(map[string]interface{}); ok {
				output, _ := shiftDef["output"].(string)
				by, _ := shiftDef["by"].(float64)

				if strings.HasPrefix(output, "$") {
					wf.FieldExpr = FieldExpr(strings.TrimPrefix(output, "$"))
				}
				wf.Offset = int(by)

				if by < 0 {
					wf.Function = models.WindowFunc("LAG")
					wf.Offset = -wf.Offset // Make positive
				} else {
					wf.Function = models.WindowFunc("LEAD")
				}
			}
			return wf
		}
	}

	return nil
}

// ============================================================================
// SET OPERATIONS ($unionWith, $setIntersection, $setDifference)
// Mapping: UNION, UNION ALL, INTERSECT, EXCEPT
// ============================================================================

func convertUnionWith(query *models.Query, unionWith map[string]interface{}) {
	coll, _ := unionWith["coll"].(string)

	rightQuery := &models.Query{
		Operation: "GET",
		Entity:    TableToEntity(coll),
	}

	// pipeline in $unionWith
	if pipeline, ok := unionWith["pipeline"].([]interface{}); ok {
		for _, stage := range pipeline {
			if stageMap, ok := stage.(map[string]interface{}); ok {
				if match, ok := stageMap["$match"].(map[string]interface{}); ok {
					conditions, _ := convertMongoFilter(match)
					rightQuery.Conditions = conditions
				}
			}
		}
	}

	query.SetOperation = &models.SetOperation{
		Type:       models.Union,
		LeftQuery:  &models.Query{Operation: query.Operation, Entity: query.Entity, Conditions: query.Conditions},
		RightQuery: rightQuery,
	}
}

// ConvertSetExpression handles $setIntersection, $setDifference in $project
// Mapping: INTERSECT, EXCEPT
func ConvertSetExpression(expr map[string]interface{}) (*models.SetOperation, string) {
	// $setIntersection → INTERSECT
	if intersect, ok := expr["$setIntersection"].([]interface{}); ok {
		return &models.SetOperation{Type: models.Intersect}, extractSetArrays(intersect)
	}

	// $setDifference → EXCEPT
	if diff, ok := expr["$setDifference"].([]interface{}); ok {
		return &models.SetOperation{Type: models.Except}, extractSetArrays(diff)
	}

	// $setUnion → UNION
	if union, ok := expr["$setUnion"].([]interface{}); ok {
		return &models.SetOperation{Type: models.Union}, extractSetArrays(union)
	}

	return nil, ""
}

func extractSetArrays(arrays []interface{}) string {
	var fields []string
	for _, arr := range arrays {
		if fieldStr, ok := arr.(string); ok && strings.HasPrefix(fieldStr, "$") {
			fields = append(fields, strings.TrimPrefix(fieldStr, "$"))
		}
	}
	return strings.Join(fields, ",")
}

// ============================================================================
// CASE EXPRESSIONS ($cond, $switch)
// Mapping: CASE → cond
// ============================================================================

// ConvertCaseExpression handles $cond and $switch
func ConvertCaseExpression(expr map[string]interface{}) *models.Expression {
	// $cond - simple if/then/else
	if cond, ok := expr["$cond"]; ok {
		return convertCondExpression(cond)
	}

	// $switch - multiple branches
	if switchExpr, ok := expr["$switch"].(map[string]interface{}); ok {
		return convertSwitchExpression(switchExpr)
	}

	return nil
}

func convertCondExpression(cond interface{}) *models.Expression {
	result := &models.Expression{Type: "CASEWHEN"}

	switch c := cond.(type) {
	case []interface{}:
		// Array form: [condition, thenValue, elseValue]
		if len(c) >= 3 {
			condExpr := convertExpressionValue(c[0])
			thenExpr := convertExpressionValue(c[1])
			elseExpr := convertExpressionValue(c[2])

			result.CaseConditions = []*models.CaseCondition{{
				Condition: expressionToCondition(condExpr),
				ThenExpr:  thenExpr,
			}}
			result.CaseElse = elseExpr
		}
	case map[string]interface{}:
		// Object form: {if: condition, then: value, else: value}
		ifExpr := convertExpressionValue(c["if"])
		thenExpr := convertExpressionValue(c["then"])
		elseExpr := convertExpressionValue(c["else"])

		result.CaseConditions = []*models.CaseCondition{{
			Condition: expressionToCondition(ifExpr),
			ThenExpr:  thenExpr,
		}}
		result.CaseElse = elseExpr
	}

	return result
}

func convertSwitchExpression(switchExpr map[string]interface{}) *models.Expression {
	result := &models.Expression{Type: "CASEWHEN"}

	// branches
	if branches, ok := switchExpr["branches"].([]interface{}); ok {
		for _, branch := range branches {
			if branchMap, ok := branch.(map[string]interface{}); ok {
				caseExpr := convertExpressionValue(branchMap["case"])
				thenExpr := convertExpressionValue(branchMap["then"])

				result.CaseConditions = append(result.CaseConditions, &models.CaseCondition{
					Condition: expressionToCondition(caseExpr),
					ThenExpr:  thenExpr,
				})
			}
		}
	}

	// default
	if defaultExpr := switchExpr["default"]; defaultExpr != nil {
		result.CaseElse = convertExpressionValue(defaultExpr)
	}

	return result
}

// ============================================================================
// COMPLEX EXPRESSION CONVERSION (called from mongodb.go)
// Only handles CASE expressions - other complex expressions use native bypass
// ============================================================================

func convertComplexExpression(expr map[string]interface{}) *models.Expression {
	// Check for CASE expressions (in mapping)
	if caseExpr := ConvertCaseExpression(expr); caseExpr != nil {
		return caseExpr
	}

	// For field references like "$fieldName"
	for _, v := range expr {
		if fieldStr, ok := v.(string); ok && strings.HasPrefix(fieldStr, "$") {
			return FieldExpr(strings.TrimPrefix(fieldStr, "$"))
		}
	}

	return nil
}

// ============================================================================
// HELPERS
// ============================================================================

func convertExpressionValue(val interface{}) *models.Expression {
	switch v := val.(type) {
	case string:
		if strings.HasPrefix(v, "$") {
			return FieldExpr(strings.TrimPrefix(v, "$"))
		}
		return LiteralExpr(v)
	case float64:
		return LiteralExpr(valueToString(v))
	case int:
		return LiteralExpr(valueToString(v))
	case bool:
		return LiteralExpr(valueToString(v))
	case map[string]interface{}:
		// Check for CASE expression
		if caseExpr := ConvertCaseExpression(v); caseExpr != nil {
			return caseExpr
		}
		// Check for comparison operators in expression context
		return convertComparisonInExpr(v)
	default:
		return LiteralExpr(valueToString(v))
	}
}

func convertComparisonInExpr(expr map[string]interface{}) *models.Expression {
	for op, val := range expr {
		switch op {
		case "$eq":
			if arr, ok := val.([]interface{}); ok && len(arr) >= 2 {
				return BinaryExpr(convertExpressionValue(arr[0]), "=", convertExpressionValue(arr[1]))
			}
		case "$ne":
			if arr, ok := val.([]interface{}); ok && len(arr) >= 2 {
				return BinaryExpr(convertExpressionValue(arr[0]), "!=", convertExpressionValue(arr[1]))
			}
		case "$gt":
			if arr, ok := val.([]interface{}); ok && len(arr) >= 2 {
				return BinaryExpr(convertExpressionValue(arr[0]), ">", convertExpressionValue(arr[1]))
			}
		case "$gte":
			if arr, ok := val.([]interface{}); ok && len(arr) >= 2 {
				return BinaryExpr(convertExpressionValue(arr[0]), ">=", convertExpressionValue(arr[1]))
			}
		case "$lt":
			if arr, ok := val.([]interface{}); ok && len(arr) >= 2 {
				return BinaryExpr(convertExpressionValue(arr[0]), "<", convertExpressionValue(arr[1]))
			}
		case "$lte":
			if arr, ok := val.([]interface{}); ok && len(arr) >= 2 {
				return BinaryExpr(convertExpressionValue(arr[0]), "<=", convertExpressionValue(arr[1]))
			}
		}
	}
	return nil
}

func expressionToCondition(expr *models.Expression) *models.Condition {
	if expr == nil {
		return nil
	}

	// If it's a binary comparison, convert directly
	if expr.Type == "BINARY" && isComparisonOperator(expr.Operator) {
		return &models.Condition{
			FieldExpr: expr.Left,
			Operator:  expr.Operator,
			ValueExpr: expr.Right,
		}
	}

	// Otherwise, treat as boolean expression
	return &models.Condition{
		FieldExpr: expr,
		Operator:  "=",
		ValueExpr: LiteralExpr("true"),
	}
}

func isComparisonOperator(op string) bool {
	switch op {
	case "=", "!=", ">", ">=", "<", "<=", "IN", "NOT_IN", "LIKE", "ILIKE":
		return true
	}
	return false
}