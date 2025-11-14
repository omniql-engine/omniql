package parser

import (
	"fmt"
	"strings"
	"strconv"
	"github.com/omniql-engine/omniql/mapping"
	"github.com/omniql-engine/omniql/engine/models"
)

// parseDQL routes DQL operations to specific parsers using sub-type mapping
func parseDQL(operation string, parts []string) (*models.Query, error) {
	// Extract base operation (SUM:budget → SUM)
	baseOp := strings.Split(operation, ":")[0]
	
	// Get operation sub-type from mapping (JOIN, AGGREGATE, SET, WINDOW, etc.)
	subType, exists := mapping.OperationSubTypes[baseOp]
	if !exists {
		return nil, fmt.Errorf("unknown DQL operation: %s", operation)
	}
	
	// Route based on sub-type
	switch subType {
	case "JOIN":
		return ParseJoin(operation, parts)
	case "AGGREGATE":
		return ParseAggregate(operation, parts)
	case "SET":
		return ParseSetOperation(operation, parts)
	case "WINDOW":
		return ParseWindowFunction(operation, parts)
	case "ADVANCED":
		return ParseAdvancedDQL(operation, parts)
	case "PATTERN":
		return ParsePatternMatch(operation, parts)
	case "CONDITIONAL":
		return ParseCaseStatement(operation, parts)
	default:
		return nil, fmt.Errorf("unsupported DQL sub-type: %s for operation: %s", subType, operation)
	}
}

// ============================================================================
// JOIN OPERATIONS
// ============================================================================

// isJoinOperation checks if operation is a JOIN type
// Uses mapping.OperationSubTypes - dynamic lookup!
func isJoinOperation(operation string) bool {
	subType, exists := mapping.OperationSubTypes[operation]
	return exists && subType == "JOIN"
}

// ParseJoin handles: INNER_JOIN User Project ON User.id = Project.user_id
// ParseJoin handles: LEFT_JOIN User Task ON User.id = Task.assignee_id WHERE Task.status = active
// ParseJoin handles: CROSS_JOIN User Project (no ON clause - Cartesian product)
func ParseJoin(joinType string, parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: joinType,
	}
	
	if len(parts) < 2 {
		return nil, fmt.Errorf("%s requires entity name", joinType)
	}
	
	// main entity (User)
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("%s requires entity name", joinType)
	}
	query.Entity = parts[entityIndex]

	// Find second entity to join (comes after the entity)
	if len(parts) < entityIndex+2 {
		return nil, fmt.Errorf("%s requires table to join", joinType)
	}

	// Table to join comes after entity
	joinTable := parts[entityIndex + 1]  // ← FIX: entityIndex+1, not 2
	
	// ✅ Find keywords ONCE at the top (used by both CROSS_JOIN and regular JOINs)
	whereIndex := findKeyword(parts, "WHERE")
	columnsIndex := findKeyword(parts, "COLUMNS")
	
	// CROSS_JOIN doesn't require ON clause - it's a Cartesian product
	if joinType == "CROSS JOIN" {
		join := models.Join{
			Type:  models.JoinType("CROSS"),
			Table: joinTable,
		}
		query.Joins = []models.Join{join}

		// ✅ NEW: Look for additional JOINs after CROSS_JOIN
		additionalJoins, whereIdx, colIdx := parseAdditionalJoins(parts, 3)  // Start after "CROSS JOIN Table"
		query.Joins = append(query.Joins, additionalJoins...)

		// Update indexes if additional JOINs were found
		if whereIdx != -1 {
			whereIndex = whereIdx
		}
		if colIdx != -1 {
			columnsIndex = colIdx
		}

		// Parse COLUMNS keyword (column selection)
		if columnsIndex != -1 {
			// Find end of COLUMNS list (before WHERE, ORDER BY, LIMIT, etc.)
			endIndex := len(parts)
			for _, keyword := range []string{"WHERE", "ORDER BY", "LIMIT", "OFFSET", "HAVING"} {
				idx := findKeyword(parts, keyword)
				if idx != -1 && idx < endIndex && idx > columnsIndex {
					endIndex = idx
				}
			}
			
			// Parse columns: "User.name, User.email, Project.title"
			columnTokens := parts[columnsIndex+1 : endIndex]
			columns, err := parseColumns(columnTokens)
			if err != nil {
				return nil, err
			}
			query.Columns = columns
		}
		
		// Parse WHERE if exists
		if whereIndex != -1 {
			conditions, err := parseConditions(parts[whereIndex+1:])
			if err != nil {
				return nil, err
			}
			query.Conditions = conditions
		}
		
		return query, nil
	}
	
	// ===== REGULAR JOINS (INNER, LEFT, RIGHT, FULL) =====
	
	// Find ON keyword (required for other JOIN types)
	onIndex := findKeyword(parts, "ON")
	if onIndex == -1 {
		return nil, fmt.Errorf("%s requires ON clause", joinType)
	}
	
	// Get ON clause parts - stop at next JOIN, COLUMNS, or WHERE (whichever comes first)
	onEndIndex := len(parts)

	// First, check for next JOIN keyword (most important!)
	nextJoinIdx := findNextJoinKeyword(parts[onIndex+1:])
	if nextJoinIdx != -1 {
		onEndIndex = onIndex + 1 + nextJoinIdx
	}

	// Then check COLUMNS/WHERE only if no next JOIN found
	if columnsIndex != -1 && columnsIndex < onEndIndex {
		onEndIndex = columnsIndex
	}
	if whereIndex != -1 && whereIndex < onEndIndex {
		onEndIndex = whereIndex
	}
	
	onParts := parts[onIndex+1 : onEndIndex]
	if len(onParts) < 3 {
		return nil, fmt.Errorf("invalid ON clause format")
	}
	
	// Parse: User.id = Project.user_id
	leftField := onParts[0]   // User.id
	operator := onParts[1]    // =
	rightField := onParts[2]  // Project.user_id
	
	if operator != "=" {
		return nil, fmt.Errorf("JOIN ON clause only supports = operator")
	}
	
	// Create Join struct
	join := models.Join{
		Type:       models.JoinType(strings.Replace(joinType, " JOIN", "", 1)), // INNER JOIN → INNER
		Table:      joinTable,
		LeftField:  leftField,
		RightField: rightField,
	}
	
	query.Joins = []models.Join{join}

	// ✅ NEW: Look for additional JOINs after the first one
	additionalJoins, whereIdx, colIdx := parseAdditionalJoins(parts, onEndIndex)
	query.Joins = append(query.Joins, additionalJoins...)

	// Update indexes if additional JOINs were found
	if whereIdx != -1 {
		whereIndex = whereIdx
	}
	if colIdx != -1 {
		columnsIndex = colIdx
	}

	// Parse COLUMNS keyword (column selection)
	if columnsIndex != -1 {
		// Find end of COLUMNS list (before WHERE, ORDER BY, LIMIT, etc.)
		endIndex := len(parts)
		for _, keyword := range []string{"WHERE", "ORDER BY", "LIMIT", "OFFSET", "HAVING"} {
			idx := findKeyword(parts, keyword)
			if idx != -1 && idx < endIndex && idx > columnsIndex {
				endIndex = idx
			}
		}
		
		// Parse columns: "User.name, User.email, Project.title"
		columnTokens := parts[columnsIndex+1 : endIndex]
		columns, err := parseColumns(columnTokens)
		if err != nil {
			return nil, err
		}
		query.Columns = columns
	}
	
	// Parse WHERE if exists
	if whereIndex != -1 {
		conditions, err := parseConditions(parts[whereIndex+1:])
		if err != nil {
			return nil, err
		}
		query.Conditions = conditions
	}
	
	return query, nil
}

// ============================================================================
// AGGREGATE OPERATIONS
// ============================================================================

// isAggregateOperation checks if operation is an aggregate function
// Uses mapping.OperationSubTypes - dynamic lookup!
func isAggregateOperation(operation string) bool {
	// Extract base function (SUM:budget → SUM)
	baseFunc := strings.Split(operation, ":")[0]
	subType, exists := mapping.OperationSubTypes[baseFunc]
	return exists && subType == "AGGREGATE"
}

// ParseAggregate handles:
// - GET User COUNT
// - GET Project SUM:budget
// - GET Task AVG:completion_time WHERE status = completed
// - GET Task COUNT GROUP BY region
// - GET Item COUNT DISTINCT category
func ParseAggregate(aggFunc string, parts []string) (*models.Query, error) {
	// Extract base function and field
	// aggFunc could be "COUNT" or "SUM:budget"
	aggParts := strings.Split(aggFunc, ":")
	baseFunc := aggParts[0]
	
	query := &models.Query{
		Operation: baseFunc,  // Use base function only (SUM, not SUM:budget)
	}
	
	// Find GET keyword (should be parts[0] in most cases)
	getIndex := -1
	for i, part := range parts {
		if strings.ToUpper(part) == "GET" {
			getIndex = i
			break
		}
	}
	
	if getIndex == -1 {
		return nil, fmt.Errorf("aggregate operations require GET")
	}
	
	// Entity should be after GET
	if getIndex+1 >= len(parts) {
		return nil, fmt.Errorf("aggregate operation requires entity name")
	}
	
	query.Entity = parts[getIndex+1]
	
	// Create aggregation from already-split parts
	aggregation := &models.Aggregation{
		Function: models.AggregateFunc(aggParts[0]),
	}
	
	// If field specified in colon format (e.g., SUM:budget)
	if len(aggParts) > 1 {
		aggregation.Field = aggParts[1]
	} else {
		// Field is in parts array after the aggregate function keyword
		// For: GET User SUM age -> parts = ["GET", "User", "SUM", "age"]
		// For: GET Item COUNT DISTINCT category -> parts = ["GET", "Item", "COUNT", "DISTINCT", "category"]
		for i, part := range parts {
			if strings.ToUpper(part) == strings.ToUpper(baseFunc) {
				// Check if next token is DISTINCT
				if i+1 < len(parts) && strings.ToUpper(parts[i+1]) == "DISTINCT" {
					// DISTINCT found - set flag and get field after DISTINCT
					query.Distinct = true
					if i+2 < len(parts) {
						fieldToken := parts[i+2]
						upperField := strings.ToUpper(fieldToken)
						// Make sure it's not another keyword
						if upperField != "WHERE" && upperField != "GROUP" && upperField != "BY" &&
						   upperField != "ORDER" && upperField != "HAVING" && 
						   upperField != "LIMIT" && upperField != "OFFSET" && fieldToken != "*" {
							aggregation.Field = fieldToken
						}
					}
				} else if i+1 < len(parts) {
					// No DISTINCT - get field normally
					nextToken := parts[i+1]
					upperNext := strings.ToUpper(nextToken)
					// Skip if it's a keyword or wildcard
					if upperNext != "WHERE" && upperNext != "GROUP" && upperNext != "BY" &&
					   upperNext != "ORDER" && upperNext != "HAVING" && 
					   upperNext != "LIMIT" && upperNext != "OFFSET" && 
					   upperNext != "DISTINCT" && nextToken != "*" {
						aggregation.Field = nextToken
					}
				}
				break
			}
		}
	}
	
	query.Aggregate = aggregation
	
	// Check for WHERE clause
	whereIndex := findKeyword(parts, "WHERE")
	if whereIndex != -1 {
		// Find end of WHERE clause
		endIndex := len(parts)
		
		// Check for keywords that end the WHERE clause
		keywords := []string{"GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET", "DISTINCT"}
		for _, keyword := range keywords {
			idx := findKeyword(parts, keyword)
			if idx != -1 && idx < endIndex && idx > whereIndex {
				endIndex = idx
			}
		}
		
		// Also stop at aggregate keywords (COUNT, SUM, AVG, MIN, MAX)
		aggregateKeywords := []string{"COUNT", "SUM", "AVG", "MIN", "MAX"}
		for i := whereIndex + 1; i < endIndex; i++ {
			tokenUpper := strings.ToUpper(parts[i])
			for _, aggKw := range aggregateKeywords {
				if tokenUpper == aggKw {
					endIndex = i
					break
				}
			}
			if endIndex == i {
				break
			}
		}
		
		conditions, err := parseConditions(parts[whereIndex+1 : endIndex])
		if err != nil {
			return nil, err
		}
		query.Conditions = conditions
	}
	
	// ✅ FIXED: Check for GROUP BY clause using findKeyword
	groupByIndex := findKeyword(parts, "GROUP BY")
	
	if groupByIndex != -1 {
		var groupByFields []string
		
		// Find end of GROUP BY clause (stop at aggregate functions and other keywords)
		endIndex := len(parts)
		keywords := []string{"ORDER BY", "HAVING", "LIMIT", "OFFSET", "COUNT", "SUM", "AVG", "MIN", "MAX"}
		for _, keyword := range keywords {
			idx := findKeyword(parts, keyword)
			if idx != -1 && idx < endIndex && idx > groupByIndex {
				endIndex = idx
			}
		}
		
		// Extract GROUP BY fields (start after "GROUP BY", which is 2 tokens)
		for i := groupByIndex + 2; i < endIndex; i++ {
			field := parts[i]
			// Skip commas
			if field != "," {
				groupByFields = append(groupByFields, field)
			}
		}
		
		query.GroupBy = groupByFields
	}
	
	// Check for ORDER BY clause
	orderByIndex := findKeyword(parts, "ORDER BY")
	if orderByIndex != -1 {
		orderBy, err := parseOrderByClause(parts[orderByIndex:])
		if err != nil {
			return nil, err
		}
		query.OrderBy = orderBy
	}
	
	// Check for HAVING clause
	havingIndex := findKeyword(parts, "HAVING")
	if havingIndex != -1 {
		// ✅ FIX: Find end of HAVING clause (stop at ORDER BY, LIMIT, OFFSET)
		endIndex := len(parts)
		for _, keyword := range []string{"ORDER BY", "LIMIT", "OFFSET"} {
			idx := findKeyword(parts, keyword)
			if idx != -1 && idx < endIndex && idx > havingIndex {
				endIndex = idx
			}
		}
		
		having, err := parseConditions(parts[havingIndex+1:endIndex])
		if err != nil {
			return nil, err
		}
		
		// ✅ FIX: Match HAVING aggregate names with SELECT aggregate
		// User writes: HAVING COUNT > 1
		// We need: HAVING COUNT(*) > 1 (to match SELECT COUNT(*))
		for i := range having {
			fieldUpper := strings.ToUpper(having[i].Field)
			
			// Check if this is an aggregate function name (without parentheses)
			if fieldUpper == "COUNT" || fieldUpper == "SUM" || fieldUpper == "AVG" || 
			fieldUpper == "MIN" || fieldUpper == "MAX" {
				
				// Match with the aggregate from SELECT
				if query.Aggregate != nil {
					aggFunc := strings.ToUpper(string(query.Aggregate.Function))
					
					if fieldUpper == aggFunc {
						// Build the full aggregate function to match SELECT
						if query.Aggregate.Field == "" {
							// COUNT * → COUNT(*)
							having[i].Field = aggFunc + "(*)"
						} else {
							// SUM amount → SUM(amount)
							having[i].Field = aggFunc + "(" + query.Aggregate.Field + ")"
						}
					}
				}
			}
			// If field already has parentheses like "COUNT(*)", leave it as is
		}
		
		query.Having = having
	}
	
	// Extract LIMIT
	limitIndex := findKeyword(parts, "LIMIT")
	if limitIndex != -1 && limitIndex+1 < len(parts) {
		limit, err := strconv.Atoi(parts[limitIndex+1])
		if err != nil {
			return nil, fmt.Errorf("LIMIT requires numeric value: %w", err)
		}
		query.Limit = limit
	}

	// Extract OFFSET
	offsetIndex := findKeyword(parts, "OFFSET")
	if offsetIndex != -1 && offsetIndex+1 < len(parts) {
		offset, err := strconv.Atoi(parts[offsetIndex+1])
		if err != nil {
			return nil, fmt.Errorf("OFFSET requires numeric value: %w", err)
		}
		query.Offset = offset
	}
	
	return query, nil
}

// ============================================================================
// SET OPERATIONS
// ============================================================================

// isSetOperation checks if operation is a set operation
// Uses mapping.OperationSubTypes - dynamic lookup!
func isSetOperation(operation string) bool {
	subType, exists := mapping.OperationSubTypes[operation]
	return exists && subType == "SET"
}

// ParseSetOperation handles: GET User WHERE age > 30 UNION GET User WHERE age < 20
func ParseSetOperation(operation string, parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: operation,
	}
	
	// Find the set operation keyword position in the full query string
	// Join parts back to find parentheses-wrapped queries
	fullQuery := strings.Join(parts, " ")
	
	// Find the operation keyword (UNION, INTERSECT, etc.)
	opKeyword := strings.ToUpper(operation)
	opIndex := strings.Index(strings.ToUpper(fullQuery), opKeyword)
	
	if opIndex == -1 {
		return nil, fmt.Errorf("set operation keyword not found: %s", operation)
	}
	
	// Split into left and right queries
	leftQueryStr := strings.TrimSpace(fullQuery[:opIndex])
	rightQueryStr := strings.TrimSpace(fullQuery[opIndex+len(opKeyword):])
	
	// Remove outer parentheses if present
	leftQueryStr = strings.TrimPrefix(leftQueryStr, "(")
	leftQueryStr = strings.TrimSuffix(leftQueryStr, ")")
	rightQueryStr = strings.TrimPrefix(rightQueryStr, "(")
	rightQueryStr = strings.TrimSuffix(rightQueryStr, ")")
	
	// Parse left query recursively
	leftQuery, err := Parse(leftQueryStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse left query: %w", err)
	}
	
	// Parse right query recursively
	rightQuery, err := Parse(rightQueryStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse right query: %w", err)
	}
	
	// Use the first query's entity as the main entity
	query.Entity = leftQuery.Entity
	
	// Create SetOperation
	query.SetOperation = &models.SetOperation{
		Type:       models.SetOperationType(operation),
		LeftQuery:  leftQuery,
		RightQuery: rightQuery,
	}
	
	return query, nil
}

// ============================================================================
// WINDOW FUNCTIONS (NEW)
// ============================================================================

// ParseWindowFunction handles:
// - GET User WITH ROW_NUMBER OVER (PARTITION BY department ORDER BY salary DESC)
// - GET Employee WITH RANK OVER (PARTITION BY team ORDER BY performance DESC)
// - GET Sales WITH LAG:amount OVER (PARTITION BY region ORDER BY date)
func ParseWindowFunction(windowFunc string, parts []string) (*models.Query, error) {
	// Extract base function and field (for LAG/LEAD)
	// windowFunc could be "ROW NUMBER" or "LAG:amount"
	funcParts := strings.Split(windowFunc, ":")
	baseFunc := funcParts[0]
	
	query := &models.Query{
		Operation: baseFunc,
	}
	
	// Find GET keyword
	getIndex := findKeyword(parts, "GET")
	if getIndex == -1 {
		return nil, fmt.Errorf("window functions require GET")
	}
	
	// Entity after GET
	if getIndex+1 >= len(parts) {
		return nil, fmt.Errorf("window function requires entity name")
	}
	query.Entity = parts[getIndex+1]
	
	// Find WITH keyword
	withIndex := findKeyword(parts, "WITH")
	if withIndex == -1 {
		return nil, fmt.Errorf("window functions require WITH clause")
	}
	
	// Find OVER keyword
	overIndex := findKeyword(parts, "OVER")
	if overIndex == -1 {
		return nil, fmt.Errorf("window functions require OVER clause")
	}
	
	// Create window function structure
	windowClause := &models.WindowFunction{
		Function: models.WindowFunc(baseFunc),
	}
	
	// Extract field for LAG/LEAD
	// Supports both formats: "LAG:age" and "LAG age"
	if len(funcParts) > 1 {
		// Format: LAG:age
		windowClause.Field = funcParts[1]
	} else {
		// Format: LAG age (field is between function and OVER)
		// For LAG/LEAD, find the token after the function name
		for i, part := range parts {
			if strings.ToUpper(part) == baseFunc {
				// Next token before OVER is the field
				if i+1 < overIndex {
					nextToken := strings.ToUpper(parts[i+1])
					// Check it's not a keyword
					if nextToken != "OVER" && nextToken != "ORDER" && nextToken != "PARTITION" {
						windowClause.Field = parts[i+1]
					}
				}
				break
			}
		}
	}
	
	// Parse OVER clause: (PARTITION BY department ORDER BY salary DESC)
	// Find content between parentheses
	overContent := strings.Join(parts[overIndex+1:], " ")
	
	// Extract PARTITION BY clause
	if strings.Contains(strings.ToUpper(overContent), "PARTITION BY") {
		partitionStr := extractStringBetween(strings.ToUpper(overContent), "PARTITION BY", "ORDER BY")
		if partitionStr == "" {
			partitionStr = extractStringBetween(strings.ToUpper(overContent), "PARTITION BY", ")")
		}
		partitionStr = strings.TrimSpace(partitionStr)
		
		// Get corresponding part from original (case-preserved) string
		partitionFields := strings.Split(partitionStr, ",")
		for i, field := range partitionFields {
			partitionFields[i] = strings.TrimSpace(field)
		}
		windowClause.PartitionBy = partitionFields
	}
	
	// Extract ORDER BY clause
	if strings.Contains(strings.ToUpper(overContent), "ORDER BY") {
		orderStr := extractStringBetween(strings.ToUpper(overContent), "ORDER BY", ")")
		orderStr = strings.TrimSpace(orderStr)
		
		// Parse ORDER BY fields and direction
		orderParts := strings.Fields(orderStr)
		if len(orderParts) >= 1 {
			orderBy := models.OrderBy{
				Field:     orderParts[0],
				Direction: models.Ascending,
			}
			if len(orderParts) >= 2 && strings.ToUpper(orderParts[1]) == "DESC" {
				orderBy.Direction = models.Descending
			}
			windowClause.OrderBy = []models.OrderBy{orderBy}
		}
	}
	
	query.WindowFunctions = []models.WindowFunction{*windowClause}
	
	// Parse WHERE clause if exists
	// Check for WHERE clause
	whereIndex := findKeyword(parts, "WHERE")
	if whereIndex != -1 {
		// Find end of WHERE (stop at GROUP BY, ORDER BY, LIMIT, OFFSET, DISTINCT, or aggregate keywords)
		endIndex := len(parts)
		
		// Check for keywords that end the WHERE clause
		keywords := []string{"GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET", "DISTINCT"}
		for _, keyword := range keywords {
			idx := findKeyword(parts, keyword)
			if idx != -1 && idx < endIndex && idx > whereIndex {
				endIndex = idx
			}
		}
		
		// ✅ NEW: Also stop at aggregate keywords (COUNT, SUM, AVG, MIN, MAX)
		aggregateKeywords := []string{"COUNT", "SUM", "AVG", "MIN", "MAX"}
		for i := whereIndex + 1; i < endIndex; i++ {
			tokenUpper := strings.ToUpper(parts[i])
			for _, aggKw := range aggregateKeywords {
				if tokenUpper == aggKw {
					endIndex = i
					break
				}
			}
			if endIndex == i {
				break
			}
		}
		
		conditions, err := parseConditions(parts[whereIndex+1 : endIndex])
		if err != nil {
			return nil, err
		}
		query.Conditions = conditions
	}
	
	return query, nil
}

// ============================================================================
// ADVANCED DQL (NEW): CTE, SUBQUERY, EXISTS
// ============================================================================

// ParseAdvancedDQL routes advanced DQL operations
func ParseAdvancedDQL(operation string, parts []string) (*models.Query, error) {
	switch operation {
	case "CTE":
		return parseCTEOperation(parts)
	case "SUBQUERY":
		return parseSubqueryOperation(parts)
	case "EXISTS":
		return parseExistsOperation(parts)
	default:
		return nil, fmt.Errorf("unknown advanced DQL operation: %s", operation)
	}
}

// parseCTEOperation handles: WITH temp_users AS (GET User WHERE status = active) GET temp_users
func parseCTEOperation(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "CTE",
	}
	
	// Find WITH keyword (should be parts[0])
	withIndex := findKeyword(parts, "WITH")
	if withIndex == -1 {
		return nil, fmt.Errorf("CTE requires WITH keyword")
	}
	
	// Find AS keyword
	asIndex := findKeyword(parts, "AS")
	if asIndex == -1 {
		return nil, fmt.Errorf("CTE requires AS keyword")
	}
	
	// CTE name is between WITH and AS
	if withIndex+1 >= asIndex {
		return nil, fmt.Errorf("CTE requires name")
	}
	cteName := parts[withIndex+1]
	
	// Find the CTE query (between parentheses after AS)
	// Format: WITH temp_users AS (GET User WHERE status = active) GET temp_users
	remainingParts := strings.Join(parts[asIndex+1:], " ")
	
	// Extract CTE query between parentheses
	cteQuery := extractStringBetween(remainingParts, "(", ")")
	if cteQuery == "" {
		return nil, fmt.Errorf("CTE requires query definition in parentheses")
	}
	
	// Find main query after CTE definition
	// Look for closing parenthesis, then GET
	closingParenIndex := strings.Index(remainingParts, ")")
	if closingParenIndex == -1 {
		return nil, fmt.Errorf("CTE missing closing parenthesis")
	}
	
	mainQueryStr := strings.TrimSpace(remainingParts[closingParenIndex+1:])
	
	query.CTE = &models.CTE{
		Name:      cteName,
		Query:     cteQuery,
		MainQuery: mainQueryStr,
	}
	
	return query, nil
}

// parseSubqueryOperation handles: GET User WHERE id IN (GET UserRole WHERE role = admin)
func parseSubqueryOperation(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "SUBQUERY",
	}
	
	// Find GET keyword
	getIndex := findKeyword(parts, "GET")
	if getIndex == -1 {
		return nil, fmt.Errorf("subquery requires GET")
	}
	
	// Entity after GET
	if getIndex+1 >= len(parts) {
		return nil, fmt.Errorf("subquery requires entity name")
	}
	query.Entity = parts[getIndex+1]
	
	// Find WHERE keyword
	whereIndex := findKeyword(parts, "WHERE")
	if whereIndex == -1 {
		return nil, fmt.Errorf("subquery requires WHERE clause")
	}
	
	// Find IN or EXISTS keyword
	inIndex := findKeyword(parts, "IN")
	existsIndex := findKeyword(parts, "EXISTS")
	
	if inIndex == -1 && existsIndex == -1 {
		return nil, fmt.Errorf("subquery requires IN or EXISTS operator")
	}
	
	// Extract subquery between parentheses
	remainingParts := strings.Join(parts[whereIndex+1:], " ")
	subqueryStr := extractStringBetween(remainingParts, "(", ")")
	
	if subqueryStr == "" {
		return nil, fmt.Errorf("subquery requires nested query in parentheses")
	}
	
	subqueryType := "IN"
	if existsIndex != -1 {
		subqueryType = "EXISTS"
	}
	
	query.Subquery = &models.Subquery{
		Type:  subqueryType,
		Query: subqueryStr,
	}
	
	// Parse outer WHERE conditions (before IN/EXISTS)
	if whereIndex+1 < inIndex || (existsIndex != -1 && whereIndex+1 < existsIndex) {
		endIndex := inIndex
		if existsIndex != -1 {
			endIndex = existsIndex
		}
		conditions, err := parseConditions(parts[whereIndex+1 : endIndex])
		if err != nil {
			return nil, err
		}
		query.Conditions = conditions
	}
	
	return query, nil
}

// parseExistsOperation handles: GET User WHERE EXISTS (GET Order WHERE Order.user_id = User.id)
func parseExistsOperation(parts []string) (*models.Query, error) {
	// Similar to subquery, but specifically for EXISTS
	return parseSubqueryOperation(parts)
}

// ============================================================================
// PATTERN MATCHING (NEW)
// ============================================================================

// ParsePatternMatch handles: GET User WHERE name LIKE %john%
func ParsePatternMatch(operation string, parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "LIKE",
	}
	
	// Find GET keyword
	getIndex := findKeyword(parts, "GET")
	if getIndex == -1 {
		return nil, fmt.Errorf("LIKE requires GET")
	}
	
	// Entity after GET
	if getIndex+1 >= len(parts) {
		return nil, fmt.Errorf("LIKE requires entity name")
	}
	query.Entity = parts[getIndex+1]
	
	// Find WHERE keyword
	whereIndex := findKeyword(parts, "WHERE")
	if whereIndex == -1 {
		return nil, fmt.Errorf("LIKE requires WHERE clause")
	}
	
	// Find LIKE keyword
	likeIndex := findKeyword(parts, "LIKE")
	if likeIndex == -1 {
		return nil, fmt.Errorf("pattern match requires LIKE operator")
	}
	
	// Field is between WHERE and LIKE
	if whereIndex+1 >= likeIndex {
		return nil, fmt.Errorf("LIKE requires field name")
	}
	
	fieldName := parts[whereIndex+1]
	
	// Pattern is after LIKE
	if likeIndex+1 >= len(parts) {
		return nil, fmt.Errorf("LIKE requires pattern")
	}
	
	pattern := parts[likeIndex+1]
	
	query.Pattern = pattern
	query.Conditions = []models.Condition{
		{
			Field:    fieldName,
			Operator: "LIKE",
			Value:    pattern,
		},
	}
	
	return query, nil
}

// ============================================================================
// CASE STATEMENTS (NEW)
// ============================================================================

// ParseCaseStatement handles: GET User WITH CASE WHEN status = active THEN Active WHEN status = inactive THEN Inactive ELSE Unknown
func ParseCaseStatement(operation string, parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "CASE",
	}
	
	// Find GET keyword
	getIndex := findKeyword(parts, "GET")
	if getIndex == -1 {
		return nil, fmt.Errorf("CASE requires GET")
	}
	
	// Entity after GET
	if getIndex+1 >= len(parts) {
		return nil, fmt.Errorf("CASE requires entity name")
	}
	query.Entity = parts[getIndex+1]
	
	// Find WITH keyword
	withIndex := findKeyword(parts, "WITH")
	if withIndex == -1 {
		return nil, fmt.Errorf("CASE requires WITH clause")
	}
	
	// Find CASE keyword
	caseIndex := findKeyword(parts, "CASE")
	if caseIndex == -1 {
		return nil, fmt.Errorf("CASE statement requires CASE keyword")
	}
	
	// Parse WHEN clauses
	var whenClauses []models.CaseWhen
	var elseValue string
	
	i := caseIndex + 1
	for i < len(parts) {
		if strings.ToUpper(parts[i]) == "WHEN" {
			// Find THEN keyword
			thenIndex := -1
			for j := i + 1; j < len(parts); j++ {
				if strings.ToUpper(parts[j]) == "THEN" {
					thenIndex = j
					break
				}
			}
			
			if thenIndex == -1 {
				break
			}
			
			// Condition is between WHEN and THEN
			condition := strings.Join(parts[i+1:thenIndex], " ")
			
			// Find next WHEN or ELSE
			nextKeywordIndex := len(parts)
			for j := thenIndex + 1; j < len(parts); j++ {
				upper := strings.ToUpper(parts[j])
				if upper == "WHEN" || upper == "ELSE" {
					nextKeywordIndex = j
					break
				}
			}
			
			// Value is between THEN and next keyword
			value := strings.Join(parts[thenIndex+1:nextKeywordIndex], " ")
			
			whenClauses = append(whenClauses, models.CaseWhen{
				Condition: condition,
				ThenValue: value,
			})
			
			i = nextKeywordIndex
		} else if strings.ToUpper(parts[i]) == "ELSE" {
			// ELSE value is remaining parts
			elseValue = strings.Join(parts[i+1:], " ")
			break
		} else {
			i++
		}
	}
	
	query.CaseStatement = &models.CaseStatement{
		WhenClauses: whenClauses,
		ElseValue:   elseValue,
	}
	
	return query, nil
}

// ============================================================================
// HELPER FUNCTIONS (DQL-specific)
// ============================================================================

// parseOrderByClause parses ORDER BY with expression support
// Examples:
//   - ORDER BY name
//   - ORDER BY name:DESC (OQL syntax with colon)
//   - ORDER BY price * quantity DESC
//   - ORDER BY UPPER(name)
func parseOrderByClause(parts []string) ([]models.OrderBy, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("ORDER_BY requires field specification")
	}
	
	// Skip "ORDER" and "BY" tokens if they're at the start
	startIdx := 1 // Default: skip 1 token (for "ORDER BY" as single token)
	if len(parts) >= 2 && strings.ToUpper(parts[0]) == "ORDER" && strings.ToUpper(parts[1]) == "BY" {
		startIdx = 2 // Skip 2 tokens when "ORDER" and "BY" are separate
	}
	
	// Collect tokens until we hit another keyword
	var orderTokens []string
	for i := startIdx; i < len(parts); i++ {
		token := strings.ToUpper(parts[i])
		// Stop at other keywords
		if token == "LIMIT" || token == "OFFSET" || token == "WHERE" || token == "GROUP" || token == "HAVING" {
			break
		}
		orderTokens = append(orderTokens, parts[i])
	}
	
	if len(orderTokens) == 0 {
		return nil, fmt.Errorf("ORDER_BY requires field name")
	}
	
	// ✅ NEW: Split tokens containing colons (field:DESC → field, DESC)
	for i := 0; i < len(orderTokens); i++ {
		if strings.Contains(orderTokens[i], ":") {
			parts := strings.Split(orderTokens[i], ":")
			// Replace "field:DESC" with separate tokens
			newTokens := append(orderTokens[:i], parts...)
			if i+1 < len(orderTokens) {
				newTokens = append(newTokens, orderTokens[i+1:]...)
			}
			orderTokens = newTokens
			break // Only split first occurrence
		}
	}
	
	// Check for direction at the end
	direction := models.Ascending
	lastToken := strings.ToUpper(orderTokens[len(orderTokens)-1])
	
	if lastToken == "DESC" {
		direction = models.Descending
		orderTokens = orderTokens[:len(orderTokens)-1]
	} else if lastToken == "ASC" {
		direction = models.Ascending
		orderTokens = orderTokens[:len(orderTokens)-1]
	}
	
	if len(orderTokens) == 0 {
		return nil, fmt.Errorf("ORDER_BY requires field name")
	}
	
	// Join tokens to form expression
	fieldStr := strings.Join(orderTokens, " ")
	
	// Try to parse as expression
	isExpr, literal, expr, err := ParseFieldValue(fieldStr)
	if err != nil {
		// If parsing fails, use as simple field
		return []models.OrderBy{{
			Field:     orderTokens[0],
			Direction: direction,
		}}, nil
	}
	
	orderBy := models.OrderBy{
		Direction: direction,
	}
	
	if isExpr {
		// Convert to model expression
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
		
		for _, cc := range expr.CaseConditions {
			modelExpr.CaseConditions = append(modelExpr.CaseConditions, models.CaseCondition{
				Condition: cc.Condition,
				ThenValue: cc.ThenValue,
			})
		}
		
		orderBy.Expression = modelExpr
		orderBy.Field = expressionToStringForOrderBy(expr)
	} else {
		orderBy.Field = literal
	}
	
	return []models.OrderBy{orderBy}, nil
}

// expressionToStringForOrderBy converts expression to string for ORDER BY
func expressionToStringForOrderBy(expr *FieldExpression) string {
	switch expr.Type {
	case ExprTypeBinary:
		return fmt.Sprintf("%s %s %s", expr.LeftOperand, expr.Operator, expr.RightOperand)
	case ExprTypeFunction:
		return fmt.Sprintf("%s(%s)", expr.FunctionName, strings.Join(expr.FunctionArgs, ", "))
	case ExprTypeCaseWhen:
		var parts []string
		parts = append(parts, "CASE")
		for _, cond := range expr.CaseConditions {
			parts = append(parts, fmt.Sprintf("WHEN %s THEN %s", cond.Condition, cond.ThenValue))
		}
		if expr.CaseElse != "" {
			parts = append(parts, fmt.Sprintf("ELSE %s", expr.CaseElse))
		}
		parts = append(parts, "END")
		return strings.Join(parts, " ")
	}
	return ""
}

// parseAdditionalJoins checks for more JOINs after the first one
func parseAdditionalJoins(parts []string, startIndex int) ([]models.Join, int, int) {
	var joins []models.Join
	currentIndex := startIndex
	whereIndex := -1
	columnsIndex := -1
	
	// Keep looking for more JOIN keywords
	for currentIndex < len(parts) {
		// Check if next token is a JOIN keyword
		if currentIndex+1 >= len(parts) {
			break
		}
		
		token := strings.ToUpper(parts[currentIndex])
		nextToken := ""
		if currentIndex+1 < len(parts) {
			nextToken = strings.ToUpper(parts[currentIndex+1])
		}
		
		// Check for WHERE or COLUMNS - stop parsing JOINs
		if token == "WHERE" {
			whereIndex = currentIndex
			break
		}
		if token == "COLUMNS" {
			columnsIndex = currentIndex
			break
		}
		
		// Check for JOIN keywords
		var joinType string
		var joinStartIndex int
		
		if token == "INNER" && nextToken == "JOIN" {
			joinType = "INNER"
			joinStartIndex = currentIndex + 2
		} else if token == "LEFT" && nextToken == "JOIN" {
			joinType = "LEFT"
			joinStartIndex = currentIndex + 2
		} else if token == "RIGHT" && nextToken == "JOIN" {
			joinType = "RIGHT"
			joinStartIndex = currentIndex + 2
		} else if token == "FULL" && nextToken == "JOIN" {
			joinType = "FULL"
			joinStartIndex = currentIndex + 2
		} else if token == "CROSS" && nextToken == "JOIN" {
			joinType = "CROSS"
			joinStartIndex = currentIndex + 2
		} else {
			currentIndex++
			continue
		}
		
		// Found a JOIN! Parse it
		if joinStartIndex >= len(parts) {
			break
		}
		
		joinTable := parts[joinStartIndex]
		
		// CROSS JOIN doesn't need ON clause
		if joinType == "CROSS" {
			join := models.Join{
				Type:  models.JoinType(joinType), // Already correct - "CROSS"
				Table: joinTable,
			}
			joins = append(joins, join)
			currentIndex = joinStartIndex + 1
			continue
		}
		
		// Find ON clause for this JOIN
		onIndex := -1
		for i := joinStartIndex + 1; i < len(parts); i++ {
			if strings.ToUpper(parts[i]) == "ON" {
				onIndex = i
				break
			}
		}
		
		if onIndex == -1 {
			// No ON clause found, stop
			break
		}
		
		// Find end of this ON clause (next JOIN or WHERE/COLUMNS)
		onEndIndex := len(parts)
		for i := onIndex + 1; i < len(parts); i++ {
			token := strings.ToUpper(parts[i])
			nextToken := ""
			if i+1 < len(parts) {
				nextToken = strings.ToUpper(parts[i+1])
			}
			
			// Stop at next JOIN
			if (token == "INNER" || token == "LEFT" || token == "RIGHT" || 
			    token == "FULL" || token == "CROSS") && nextToken == "JOIN" {
				onEndIndex = i
				break
			}
			
			// Stop at WHERE/COLUMNS
			if token == "WHERE" || token == "COLUMNS" {
				onEndIndex = i
				if token == "WHERE" {
					whereIndex = i
				}
				if token == "COLUMNS" {
					columnsIndex = i
				}
				break
			}
		}
		
		// Parse ON clause
		onParts := parts[onIndex+1 : onEndIndex]
		if len(onParts) < 3 {
			break
		}
		
		leftField := onParts[0]
		operator := onParts[1]
		rightField := onParts[2]
		
		if operator != "=" {
			break
		}
		
		// Create JOIN
		join := models.Join{
			Type:       models.JoinType(joinType),
			Table:      joinTable,
			LeftField:  leftField,
			RightField: rightField,
		}
		joins = append(joins, join)
		
		// Move to end of this ON clause
		currentIndex = onEndIndex
	}
	
	return joins, whereIndex, columnsIndex
}

// findNextJoinKeyword finds the next JOIN keyword in parts
func findNextJoinKeyword(parts []string) int {
	for i := 0; i < len(parts)-1; i++ {
		token := strings.ToUpper(parts[i])
		nextToken := strings.ToUpper(parts[i+1])
		
		// Check for two-word JOIN keywords
		if nextToken == "JOIN" {
			if token == "INNER" || token == "LEFT" || token == "RIGHT" || 
			   token == "FULL" || token == "CROSS" {
				return i
			}
		}
	}
	
	return -1
}