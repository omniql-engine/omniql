package parser

import (
	"fmt"
	"strings"
	"strconv" 

	"github.com/omniql-engine/omniql/engine/models"
)

// crudParsers maps CRUD operations to their parser functions
// Fully dynamic - no switch statement needed!
var crudParsers = map[string]func([]string) (*models.Query, error){
	"GET":         ParseGet,
	"CREATE":      ParseCreate,
	"UPDATE":      ParseUpdate,
	"DELETE":      ParseDelete,
	"UPSERT":      ParseUpsert,
	"BULK INSERT": ParseBulkInsert,
	"REPLACE":     ParseReplace,
}

// parseCRUD routes CRUD operations to specific parsers using function map
func parseCRUD(operation string, parts []string) (*models.Query, error) {
	parser, exists := crudParsers[operation]
	if !exists {
		return nil, fmt.Errorf("unknown CRUD operation: %s", operation)
	}
	return parser(parts)
}

// ParseGet handles: GET User WHERE id = 123
func ParseGet(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "GET",
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("GET requires entity name")
	}
	query.Entity = parts[entityIndex]

	// Check for DISTINCT with optional column selection
	distinctIndex := findKeyword(parts, "DISTINCT")
	if distinctIndex != -1 {
		query.Distinct = true
		
		if distinctIndex+1 < len(parts) {
			whereIndex := findKeyword(parts, "WHERE")
			orderIndex := findKeyword(parts, "ORDER")
			limitIndex := findKeyword(parts, "LIMIT")
			offsetIndex := findKeyword(parts, "OFFSET")
			groupIndex := findKeyword(parts, "GROUP")
			havingIndex := findKeyword(parts, "HAVING")
			
			nextKeywordIndex := len(parts)
			for _, idx := range []int{whereIndex, orderIndex, limitIndex, offsetIndex, groupIndex, havingIndex} {
				if idx > distinctIndex && idx < nextKeywordIndex {
					nextKeywordIndex = idx
				}
			}
			
			if distinctIndex+1 < nextKeywordIndex {
				query.Columns = []string{parts[distinctIndex+1]}
			}
		}
	}

	// // ========================================================================
	// // ✅ NEW SECTION: Detect window functions BEFORE SELECT expressions
	// // ========================================================================
	// // Problem: "GET User WITH ROW NUMBER OVER (...)" looks like SELECT expression
	// // Solution: If we detect window function keywords, return error to route to DQL
	
	// withIndex := findKeyword(parts, "WITH")
	// if withIndex != -1 && withIndex+1 < len(parts) {
	// 	// Look at the word(s) after WITH
	// 	nextToken := strings.ToUpper(parts[withIndex+1])
	// 	twoTokens := nextToken
	// 	if withIndex+2 < len(parts) {
	// 		twoTokens += " " + strings.ToUpper(parts[withIndex+2])
	// 	}
		
	// 	// List of window function keywords
	// 	windowFuncs := []string{"ROW NUMBER", "RANK", "DENSE RANK", "DENSE_RANK", "LAG", "LEAD", "NTILE", "ROW_NUMBER"}
		
	// 	// If we find a window function, DON'T parse it here
	// 	for _, wf := range windowFuncs {
	// 		if strings.HasPrefix(twoTokens, wf) || nextToken == wf {
	// 			// Return error so main parser routes to DQL instead
	// 			return nil, fmt.Errorf("window function detected: route to DQL parser")
	// 		}
	// 	}
	// }
	// // ========================================================================

	// ✅ EXISTING: Check for WITH clause (column expressions with aliases)
	// This runs ONLY if window function check didn't trigger
	withIndex := findKeyword(parts, "WITH")  // ← Changed from := to =
	if withIndex != -1 && withIndex > entityIndex {
		var selectColumns []models.SelectColumn
		
		endIndex := len(parts)
		for _, keyword := range []string{"WHERE", "ORDER", "LIMIT", "OFFSET", "GROUP", "HAVING"} {
			idx := findKeyword(parts, keyword)
			if idx != -1 && idx < endIndex && idx > withIndex {
				endIndex = idx
			}
		}
		
		withClause := strings.Join(parts[withIndex+1:endIndex], " ")
		columnTokens := tokenizeWithParens(withClause)
		
		i := 0
		for i < len(columnTokens) {
			if columnTokens[i] == "," {
				i++
				continue
			}
			
			exprEnd := i
			for exprEnd < len(columnTokens) && columnTokens[exprEnd] != "," {
				exprEnd++
			}
			
			exprTokens := columnTokens[i:exprEnd]
			if len(exprTokens) == 0 {
				break
			}
			
			field, isExpr, expr, _, err := ParseExpressionInContext(ContextSelect, exprTokens, 0)
			if err != nil {
				return nil, fmt.Errorf("failed to parse SELECT expression: %w", err)
			}
			
			col := models.SelectColumn{
				Expression: field,
			}
			
			if isExpr && expr != nil {
				col.ExpressionObj = ConvertToModelExpression(expr)
			}
			
			for j := 0; j < len(exprTokens)-1; j++ {
				if strings.ToUpper(exprTokens[j]) == "AS" {
					col.Alias = exprTokens[j+1]
					break
				}
			}
			
			selectColumns = append(selectColumns, col)
			i = exprEnd
		}
		
		query.SelectColumns = selectColumns
	}
	
	// ✅ REST OF FUNCTION UNCHANGED
	whereIndex := findKeyword(parts, "WHERE")
	if whereIndex != -1 {
		endIndex := len(parts)
		for _, keyword := range []string{"LIMIT", "OFFSET", "ORDER BY"} {
			idx := findKeyword(parts, keyword)
			if idx != -1 && idx < endIndex && idx > whereIndex {
				endIndex = idx
			}
		}
		
		conditions, err := parseConditions(parts[whereIndex+1 : endIndex])
		if err != nil {
			return nil, err
		}
		query.Conditions = conditions
	}
	
	likeIndex := findKeyword(parts, "LIKE")
	if likeIndex != -1 && likeIndex+1 < len(parts) {
		query.Pattern = parts[likeIndex+1]
	}
	
	limitIndex := findKeyword(parts, "LIMIT")
	if limitIndex != -1 && limitIndex+1 < len(parts) {
		limit, err := strconv.Atoi(parts[limitIndex+1])
		if err != nil {
			return nil, fmt.Errorf("LIMIT requires numeric value: %w", err)
		}
		query.Limit = limit
	}

	offsetIndex := findKeyword(parts, "OFFSET")
	if offsetIndex != -1 && offsetIndex+1 < len(parts) {
		offset, err := strconv.Atoi(parts[offsetIndex+1])
		if err != nil {
			return nil, fmt.Errorf("OFFSET requires numeric value: %w", err)
		}
		query.Offset = offset
	}
	
	orderByIndex := findKeyword(parts, "ORDER BY")
	if orderByIndex != -1 {
		orderBy, err := parseOrderByClause(parts[orderByIndex:])
		if err != nil {
			return nil, err
		}
		query.OrderBy = orderBy
	}
	
	return query, nil
}

// ParseCreate handles: CREATE User WITH name = John, age = 30
func ParseCreate(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "CREATE",
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("CREATE requires entity name")
	}
	query.Entity = parts[entityIndex]
	
	// Check for WITH clause
	withIndex := findKeyword(parts, "WITH")
	if withIndex != -1 {
		fields, err := parseFields(parts[withIndex+1:])
		if err != nil {
			return nil, err
		}
		query.Fields = fields
	}
	
	return query, nil
}

// ParseUpdate handles: UPDATE User SET age = 30 WHERE id = 123
func ParseUpdate(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "UPDATE",
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("UPDATE requires entity name")
	}
	query.Entity = parts[entityIndex]
	
	// Parse SET clause
	setIndex := findKeyword(parts, "SET")
	whereIndex := findKeyword(parts, "WHERE")
	
	if setIndex == -1 {
		return nil, fmt.Errorf("UPDATE requires SET clause")
	}
	
	// Use parseUpdateFields which supports expressions
	fields, lastIndex, err := parseUpdateFields(parts, setIndex+1)
	if err != nil {
		return nil, err
	}
	query.Fields = fields
	
	// Parse WHERE if exists
	if whereIndex != -1 {
		conditions, err := parseConditions(parts[whereIndex+1:])
		if err != nil {
			return nil, err
		}
		query.Conditions = conditions
	}
	
	_ = lastIndex // We don't need this for now
	
	return query, nil
}

// ParseDelete handles: DELETE User WHERE id = 123
func ParseDelete(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "DELETE",
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("DELETE requires entity name")
	}
	query.Entity = parts[entityIndex]
	
	// Check for WHERE clause
	whereIndex := findKeyword(parts, "WHERE")
	if whereIndex != -1 {
		conditions, err := parseConditions(parts[whereIndex+1:])
		if err != nil {
			return nil, err
		}
		query.Conditions = conditions
	}
	
	return query, nil
}

// ParseUpsert handles: UPSERT User WITH name = John, email = john@test.com ON email
// Inserts if not exists, updates if exists based on conflict field(s)
func ParseUpsert(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "UPSERT",
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("UPSERT requires entity name")
	}
	query.Entity = parts[entityIndex]
	
	// Check for WITH clause (fields to insert/update)
	withIndex := findKeyword(parts, "WITH")
	onIndex := findKeyword(parts, "ON")
	
	if withIndex == -1 {
		return nil, fmt.Errorf("UPSERT requires WITH clause")
	}
	
	// Get fields between WITH and ON (or end)
	endIndex := len(parts)
	if onIndex != -1 {
		endIndex = onIndex
	}
	
	fields, err := parseFields(parts[withIndex+1 : endIndex])
	if err != nil {
		return nil, err
	}
	query.Fields = fields
	
	// Parse ON clause (conflict fields)
	if onIndex != -1 && onIndex+1 < len(parts) {
		// ON email or ON id,email
		conflictFieldsStr := parts[onIndex+1]
		conflictFields := strings.Split(conflictFieldsStr, ",")
		
		// Store conflict fields in Upsert structure
		query.Upsert = &models.Upsert{
			ConflictFields: conflictFields,
			UpdateFields:   fields, // Fields to update on conflict
		}
	}
	
	return query, nil
}

// ParseBulkInsert handles: BULK INSERT User WITH [name = John, age = 30] [name = Jane, age = 25]
// Inserts multiple rows in a single operation
func ParseBulkInsert(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "BULK INSERT",
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("BULK INSERT requires entity name")
	}
	query.Entity = parts[entityIndex]
	
	// Check for WITH clause
	withIndex := findKeyword(parts, "WITH")
	if withIndex == -1 {
		return nil, fmt.Errorf("BULK INSERT requires WITH clause")
	}
	
	// Parse rows enclosed in square brackets
	// Format: WITH [field1 = value1, field2 = value2] [field1 = value3, field2 = value4]
	remainingParts := parts[withIndex+1:]
	joinedStr := strings.Join(remainingParts, " ")
	
	// Extract rows between [ ]
	var bulkRows [][]models.Field
	var currentRow []string
	inBracket := false
	
	for _, part := range strings.Fields(joinedStr) {
		if strings.HasPrefix(part, "[") {
			inBracket = true
			part = strings.TrimPrefix(part, "[")
		}
		
		if strings.HasSuffix(part, "]") {
			part = strings.TrimSuffix(part, "]")
			currentRow = append(currentRow, part)
			
			// Parse this row's fields
			rowFields, err := parseFields(currentRow)
			if err != nil {
				return nil, fmt.Errorf("error parsing bulk insert row: %w", err)
			}
			bulkRows = append(bulkRows, rowFields)
			
			currentRow = []string{}
			inBracket = false
		} else if inBracket {
			currentRow = append(currentRow, part)
		}
	}
	
	// Store bulk data
	query.BulkData = bulkRows
	
	return query, nil
}

// ParseReplace handles: REPLACE User WITH id = 123, name = John, age = 30
// MySQL: REPLACE = DELETE + INSERT
// PostgreSQL/SQLite: Same as UPSERT
func ParseReplace(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "REPLACE",
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("REPLACE requires entity name")
	}
	query.Entity = parts[entityIndex]
	
	// Check for WITH clause
	withIndex := findKeyword(parts, "WITH")
	if withIndex == -1 {
		return nil, fmt.Errorf("REPLACE requires WITH clause")
	}
	
	fields, err := parseFields(parts[withIndex+1:])
	if err != nil {
		return nil, err
	}
	query.Fields = fields
	
	return query, nil
}

// parseUpdateFields parses UPDATE field assignments with expression support
// Supports literals, expressions, functions, and CASE WHEN:
//   SET name = John          -> literal
//   SET value = value + 1    -> expression
//   SET name = UPPER(name)   -> function
//   SET status = CASE WHEN age >= 18 THEN adult ELSE minor END -> CASE WHEN
func parseUpdateFields(tokens []string, startIndex int) ([]models.Field, int, error) {
	var fields []models.Field
	i := startIndex
	
	for i < len(tokens) {
		if i >= len(tokens) {
			break
		}
		
		// Check for WHERE or other keywords that end the SET clause
		upperToken := strings.ToUpper(tokens[i])
		if upperToken == "WHERE" || upperToken == "ORDER" || 
		   upperToken == "LIMIT" || upperToken == "OFFSET" {
			break
		}
		
		// Expect: fieldName = value
		if i+2 >= len(tokens) {
			break
		}
		
		fieldName := tokens[i]
		
		// Next should be "="
		if tokens[i+1] != "=" {
			return nil, i, fmt.Errorf("expected '=' after field name, got: %s", tokens[i+1])
		}
		
		// Get the value (might be multiple tokens for expressions/functions/CASE WHEN)
		// Collect tokens until we hit a comma or WHERE/ORDER/LIMIT/OFFSET
		var valueTokens []string
		i += 2 // Skip fieldName and "="
		
		for i < len(tokens) {
			if tokens[i] == "," {
				i++ // Skip comma
				break
			}
			
			// Check if we hit a keyword
			upperToken := strings.ToUpper(tokens[i])
			if upperToken == "WHERE" || upperToken == "ORDER" || 
			   upperToken == "LIMIT" || upperToken == "OFFSET" {
				break
			}
			
			valueTokens = append(valueTokens, tokens[i])
			i++
		}
		
		if len(valueTokens) == 0 {
			return nil, i, fmt.Errorf("missing value for field: %s", fieldName)
		}
		
		// Join value tokens and parse
		valueStr := strings.Join(valueTokens, " ")
		
		// Check if it's an expression, function, CASE WHEN, or literal
		isExpr, literal, expr, err := ParseFieldValue(valueStr)
		if err != nil {
			return nil, i, fmt.Errorf("failed to parse field value: %w", err)
		}
		
		field := models.Field{
			Name: fieldName,
		}
		
		if isExpr {
			// Convert parser.FieldExpression to models.FieldExpression
			modelExpr := &models.FieldExpression{
				Type:         string(expr.Type),
				LeftOperand:  expr.LeftOperand,
				Operator:     expr.Operator,
				RightOperand: expr.RightOperand,
				LeftIsField:  expr.LeftIsField,
				RightIsField: expr.RightIsField,
				FunctionName: expr.FunctionName,
				FunctionArgs: expr.FunctionArgs,
				CaseElse:     expr.CaseElse,
			}
			
			// Convert CaseConditions
			for _, cc := range expr.CaseConditions {
				modelExpr.CaseConditions = append(modelExpr.CaseConditions, models.CaseCondition{
					Condition: cc.Condition,
					ThenValue: cc.ThenValue,
				})
			}
			
			field.Expression = modelExpr
		} else {
			// It's a literal value
			field.Value = literal
		}
		
		fields = append(fields, field)
	}
	
	return fields, i, nil
}