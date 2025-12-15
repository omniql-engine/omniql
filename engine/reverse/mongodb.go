package reverse

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/omniql-engine/omniql/engine/models"
)

// ============================================================================
// ENTRY POINT
// ============================================================================

func MongoDBToQuery(jsonStr string) (*models.Query, error) {
	var doc map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &doc); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %v", ErrParseError, err)
	}

	// ==================== CRUD ====================
	if collection, ok := doc["find"].(string); ok {
		return convertMongoFind(collection, doc)
	}
	if collection, ok := doc["insertOne"].(string); ok {
		return convertMongoInsertOne(collection, doc)
	}
	if collection, ok := doc["insertMany"].(string); ok {
		return convertMongoInsertMany(collection, doc)
	}
	if collection, ok := doc["updateOne"].(string); ok {
		return convertMongoUpdateOne(collection, doc)
	}
	if collection, ok := doc["updateMany"].(string); ok {
		return convertMongoUpdateMany(collection, doc)
	}
	if collection, ok := doc["deleteOne"].(string); ok {
		return convertMongoDeleteOne(collection, doc)
	}
	if collection, ok := doc["deleteMany"].(string); ok {
		return convertMongoDeleteMany(collection, doc)
	}
	if collection, ok := doc["replaceOne"].(string); ok {
		return convertMongoReplaceOne(collection, doc)
	}
	if collection, ok := doc["distinct"].(string); ok {
		return convertMongoDistinct(collection, doc)
	}
	if collection, ok := doc["aggregate"].(string); ok {
		return convertMongoAggregate(collection, doc)
	}

	// ==================== DDL ====================
	if collection, ok := doc["create"].(string); ok {
		return &models.Query{
			Operation: "CREATE COLLECTION",
			Entity:    TableToEntity(collection),
		}, nil
	}
	if collection, ok := doc["drop"].(string); ok {
		return &models.Query{
			Operation: "DROP COLLECTION",
			Entity:    TableToEntity(collection),
		}, nil
	}
	if collection, ok := doc["collMod"].(string); ok {
		return convertMongoCollMod(collection, doc)
	}
	if collection, ok := doc["renameCollection"].(string); ok {
		newName, _ := doc["to"].(string)
		return &models.Query{
			Operation: "RENAME TABLE",
			Entity:    TableToEntity(collection),
			NewName:   newName,
		}, nil
	}
	if collection, ok := doc["createIndexes"].(string); ok {
		return convertMongoCreateIndex(collection, doc)
	}
	if collection, ok := doc["dropIndexes"].(string); ok {
		return convertMongoDropIndex(collection, doc)
	}
	if viewName, ok := doc["createView"].(string); ok {
		return convertMongoCreateView(viewName, doc)
	}
	if viewName, ok := doc["dropView"].(string); ok {
		return &models.Query{
			Operation: "DROP VIEW",
			ViewName:  viewName,
		}, nil
	}
	if dbName, ok := doc["use"].(string); ok {
		return &models.Query{
			Operation:    "CREATE DATABASE",
			DatabaseName: dbName,
		}, nil
	}
	if dbName, ok := doc["dropDatabase"].(string); ok {
		return &models.Query{
			Operation:    "DROP DATABASE",
			DatabaseName: dbName,
		}, nil
	}
	// Handle dropDatabase: 1 format
	if _, ok := doc["dropDatabase"]; ok {
		dbName, _ := doc["$db"].(string)
		return &models.Query{
			Operation:    "DROP DATABASE",
			DatabaseName: dbName,
		}, nil
	}

	// ==================== TCL ====================
	if opts, ok := doc["startTransaction"].(map[string]interface{}); ok {
		return convertMongoStartTransaction(opts)
	}
	if _, ok := doc["startTransaction"]; ok {
		return &models.Query{
			Operation:   "BEGIN",
			Transaction: &models.Transaction{Operation: "BEGIN"},
		}, nil
	}
	if _, ok := doc["commitTransaction"]; ok {
		return &models.Query{
			Operation:   "COMMIT",
			Transaction: &models.Transaction{Operation: "COMMIT"},
		}, nil
	}
	if _, ok := doc["abortTransaction"]; ok {
		return &models.Query{
			Operation:   "ROLLBACK",
			Transaction: &models.Transaction{Operation: "ROLLBACK"},
		}, nil
	}

	// ==================== DCL ====================
	if userName, ok := doc["createUser"].(string); ok {
		return convertMongoCreateUser(userName, doc)
	}
	if userName, ok := doc["dropUser"].(string); ok {
		return &models.Query{
			Operation:  "DROP USER",
			Permission: &models.Permission{Operation: "DROP USER", UserName: userName},
		}, nil
	}
	if userName, ok := doc["updateUser"].(string); ok {
		return convertMongoUpdateUser(userName, doc)
	}
	if roleName, ok := doc["createRole"].(string); ok {
		return convertMongoCreateRole(roleName, doc)
	}
	if roleName, ok := doc["dropRole"].(string); ok {
		return &models.Query{
			Operation:  "DROP ROLE",
			Permission: &models.Permission{Operation: "DROP ROLE", RoleName: roleName},
		}, nil
	}
	if _, ok := doc["grantRolesToUser"]; ok {
		return convertMongoGrantRoles(doc)
	}
	if _, ok := doc["revokeRolesFromUser"]; ok {
		return convertMongoRevokeRoles(doc)
	}
	if _, ok := doc["grantPrivilegesToRole"]; ok {
		return convertMongoGrantPrivileges(doc)
	}
	if _, ok := doc["revokePrivilegesFromRole"]; ok {
		return convertMongoRevokePrivileges(doc)
	}

	return nil, fmt.Errorf("%w: unknown MongoDB command", ErrNotSupported)
}

// ============================================================================
// CRUD: FIND → GET
// ============================================================================

func convertMongoFind(collection string, doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "GET",
		Entity:    TableToEntity(collection),
	}

	if filter, ok := doc["filter"].(map[string]interface{}); ok {
		conditions, err := convertMongoFilter(filter)
		if err != nil {
			return nil, err
		}
		query.Conditions = conditions
	}

	if projection, ok := doc["projection"].(map[string]interface{}); ok {
		convertMongoProject(query, projection)
	}

	if sort, ok := doc["sort"].(map[string]interface{}); ok {
		query.OrderBy = convertMongoSort(sort)
	}

	if limit, ok := doc["limit"].(float64); ok {
		query.Limit = int(limit)
	}

	if skip, ok := doc["skip"].(float64); ok {
		query.Offset = int(skip)
	}

	return query, nil
}

// ============================================================================
// CRUD: INSERT ONE → CREATE
// ============================================================================

func convertMongoInsertOne(collection string, doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "CREATE",
		Entity:    TableToEntity(collection),
	}

	if document, ok := doc["document"].(map[string]interface{}); ok {
		query.Fields = convertMongoDocument(document)
	}

	return query, nil
}

// ============================================================================
// CRUD: INSERT MANY → BULK INSERT
// ============================================================================

func convertMongoInsertMany(collection string, doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "BULK INSERT",
		Entity:    TableToEntity(collection),
	}

	if documents, ok := doc["documents"].([]interface{}); ok {
		for _, d := range documents {
			if docMap, ok := d.(map[string]interface{}); ok {
				query.BulkData = append(query.BulkData, convertMongoDocument(docMap))
			}
		}
	}

	return query, nil
}

// ============================================================================
// CRUD: UPDATE ONE → UPDATE / UPSERT
// ============================================================================

func convertMongoUpdateOne(collection string, doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "UPDATE",
		Entity:    TableToEntity(collection),
	}

	if filter, ok := doc["filter"].(map[string]interface{}); ok {
		conditions, err := convertMongoFilter(filter)
		if err != nil {
			return nil, err
		}
		query.Conditions = conditions
	}

	if update, ok := doc["update"].(map[string]interface{}); ok {
		query.Fields = convertMongoUpdate(update)
	}

	if upsert, ok := doc["upsert"].(bool); ok && upsert {
		query.Operation = "UPSERT"
		query.Upsert = &models.Upsert{}
	}

	return query, nil
}

// ============================================================================
// CRUD: UPDATE MANY → UPDATE
// ============================================================================

func convertMongoUpdateMany(collection string, doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "UPDATE",
		Entity:    TableToEntity(collection),
	}

	if filter, ok := doc["filter"].(map[string]interface{}); ok {
		conditions, err := convertMongoFilter(filter)
		if err != nil {
			return nil, err
		}
		query.Conditions = conditions
	}

	if update, ok := doc["update"].(map[string]interface{}); ok {
		query.Fields = convertMongoUpdate(update)
	}

	return query, nil
}

// ============================================================================
// CRUD: REPLACE ONE → REPLACE
// ============================================================================

func convertMongoReplaceOne(collection string, doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "REPLACE",
		Entity:    TableToEntity(collection),
	}

	if filter, ok := doc["filter"].(map[string]interface{}); ok {
		conditions, err := convertMongoFilter(filter)
		if err != nil {
			return nil, err
		}
		query.Conditions = conditions
	}

	if replacement, ok := doc["replacement"].(map[string]interface{}); ok {
		query.Fields = convertMongoDocument(replacement)
	}

	if upsert, ok := doc["upsert"].(bool); ok && upsert {
		query.Upsert = &models.Upsert{}
	}

	return query, nil
}

// ============================================================================
// CRUD: DELETE ONE → DELETE
// ============================================================================

func convertMongoDeleteOne(collection string, doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "DELETE",
		Entity:    TableToEntity(collection),
	}

	if filter, ok := doc["filter"].(map[string]interface{}); ok {
		conditions, err := convertMongoFilter(filter)
		if err != nil {
			return nil, err
		}
		query.Conditions = conditions
	}

	return query, nil
}

// ============================================================================
// CRUD: DELETE MANY → DELETE / TRUNCATE
// ============================================================================

func convertMongoDeleteMany(collection string, doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "DELETE",
		Entity:    TableToEntity(collection),
	}

	if filter, ok := doc["filter"].(map[string]interface{}); ok {
		if len(filter) == 0 {
			query.Operation = "TRUNCATE"
			return query, nil
		}
		conditions, err := convertMongoFilter(filter)
		if err != nil {
			return nil, err
		}
		query.Conditions = conditions
	}

	return query, nil
}

// ============================================================================
// CRUD: DISTINCT → GET with DISTINCT
// ============================================================================

func convertMongoDistinct(collection string, doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "GET",
		Entity:    TableToEntity(collection),
		Distinct:  true,
	}

	if key, ok := doc["key"].(string); ok {
		query.Columns = []*models.Expression{FieldExpr(key)}
	}

	if filter, ok := doc["query"].(map[string]interface{}); ok {
		conditions, err := convertMongoFilter(filter)
		if err != nil {
			return nil, err
		}
		query.Conditions = conditions
	}

	return query, nil
}

// ============================================================================
// AGGREGATE → GET with aggregation features
// ============================================================================

func convertMongoAggregate(collection string, doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "GET",
		Entity:    TableToEntity(collection),
	}

	pipeline, ok := doc["pipeline"].([]interface{})
	if !ok || len(pipeline) == 0 {
		return query, nil
	}

	hasGroup := false

	for _, stage := range pipeline {
		stageMap, ok := stage.(map[string]interface{})
		if !ok {
			continue
		}

		// $match → Conditions (before $group) or Having (after $group)
		if match, ok := stageMap["$match"].(map[string]interface{}); ok {
			conditions, err := convertMongoFilter(match)
			if err != nil {
				return nil, err
			}
			if hasGroup {
				query.Having = conditions
			} else {
				query.Conditions = conditions
			}
		}

		// $group → GroupBy + Aggregate
		if group, ok := stageMap["$group"].(map[string]interface{}); ok {
			hasGroup = true
			convertMongoGroup(query, group)
		}

		// $project → Columns (basic) or SelectColumns (with expressions)
		if project, ok := stageMap["$project"].(map[string]interface{}); ok {
			convertMongoProject(query, project)
		}

		// $sort → OrderBy
		if sort, ok := stageMap["$sort"].(map[string]interface{}); ok {
			query.OrderBy = convertMongoSort(sort)
		}

		// $limit
		if limit, ok := stageMap["$limit"].(float64); ok {
			query.Limit = int(limit)
		}

		// $skip
		if skip, ok := stageMap["$skip"].(float64); ok {
			query.Offset = int(skip)
		}

		// $lookup → Join
		if lookup, ok := stageMap["$lookup"].(map[string]interface{}); ok {
			if join := convertMongoLookup(lookup); join != nil {
				query.Joins = append(query.Joins, *join)
			}
		}

		// $count → COUNT operation
		if countField, ok := stageMap["$count"].(string); ok {
			query.Operation = "COUNT"
			query.Aggregate = &models.Aggregation{
				Function:  models.AggregateFunc("COUNT"),
				FieldExpr: FieldExpr(countField),
			}
		}
	}

	// Process advanced stages (window functions, set operations, etc.)
	ProcessAdvancedPipelineStages(query, pipeline)

	return query, nil
}

// convertMongoProject handles $project stage with expressions
func convertMongoProject(query *models.Query, project map[string]interface{}) {
	for field, val := range project {
		// Exclusion: {field: 0}
		if v, ok := val.(float64); ok && v == 0 {
			continue
		}
		if v, ok := val.(int); ok && v == 0 {
			continue
		}

		// Inclusion: {field: 1}
		if v, ok := val.(float64); ok && v == 1 {
			query.Columns = append(query.Columns, FieldExpr(field))
			continue
		}
		if v, ok := val.(int); ok && v == 1 {
			query.Columns = append(query.Columns, FieldExpr(field))
			continue
		}

		// Field reference: {alias: "$field"}
		if fieldRef, ok := val.(string); ok {
			if strings.HasPrefix(fieldRef, "$") {
				query.SelectColumns = append(query.SelectColumns, models.SelectColumn{
					ExpressionObj: FieldExpr(strings.TrimPrefix(fieldRef, "$")),
					Alias:         field,
				})
			} else {
				query.SelectColumns = append(query.SelectColumns, models.SelectColumn{
					ExpressionObj: LiteralExpr(fieldRef),
					Alias:         field,
				})
			}
			continue
		}

		// Expression: {alias: {$operator: ...}}
		if exprMap, ok := val.(map[string]interface{}); ok {
			expr := convertComplexExpression(exprMap)
			if expr != nil {
				query.SelectColumns = append(query.SelectColumns, models.SelectColumn{
					ExpressionObj: expr,
					Alias:         field,
				})
			}
		}
	}
}

func convertMongoGroup(query *models.Query, group map[string]interface{}) {
	// _id → GROUP BY
	if groupID := group["_id"]; groupID != nil {
		switch id := groupID.(type) {
		case string:
			if strings.HasPrefix(id, "$") {
				query.GroupBy = append(query.GroupBy, FieldExpr(strings.TrimPrefix(id, "$")))
			}
		case map[string]interface{}:
			for _, v := range id {
				if fieldStr, ok := v.(string); ok && strings.HasPrefix(fieldStr, "$") {
					query.GroupBy = append(query.GroupBy, FieldExpr(strings.TrimPrefix(fieldStr, "$")))
				}
			}
		}
	}

	// Aggregate functions
	for key, val := range group {
		if key == "_id" {
			continue
		}
		if aggMap, ok := val.(map[string]interface{}); ok {
			for aggOp, field := range aggMap {
				oqlOp := mongoAggregateToOQL(aggOp)
				
				// Detect COUNT pattern: {"$sum": 1}
				if aggOp == "$sum" {
					if numVal, ok := field.(float64); ok && numVal == 1 {
						oqlOp = "COUNT"
					}
				}
				
				query.Operation = oqlOp

				fieldStr := valueToString(field)
				if strings.HasPrefix(fieldStr, "$") {
					fieldStr = strings.TrimPrefix(fieldStr, "$")
				}
				if fieldStr == "" || fieldStr == "1" {
					fieldStr = "*"
				}

				query.Aggregate = &models.Aggregation{
					Function:  models.AggregateFunc(oqlOp),
					FieldExpr: FieldExpr(fieldStr),
				}
				return
			}
		}
	}
}

// ============================================================================
// DDL: COLLECTION OPERATIONS
// ============================================================================

func convertMongoCollMod(collection string, doc map[string]interface{}) (*models.Query, error) {
	// Check if it's a view modification
	if _, hasViewOn := doc["viewOn"]; hasViewOn {
		return &models.Query{
			Operation: "ALTER VIEW",
			ViewName:  collection,
		}, nil
	}

	return &models.Query{
		Operation: "ALTER TABLE",
		Entity:    TableToEntity(collection),
	}, nil
}

func convertMongoCreateIndex(collection string, doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "CREATE INDEX",
		Entity:    TableToEntity(collection),
	}

	if indexes, ok := doc["indexes"].([]interface{}); ok && len(indexes) > 0 {
		if idx, ok := indexes[0].(map[string]interface{}); ok {
			if name, ok := idx["name"].(string); ok {
				query.NewName = name
			}
			if key, ok := idx["key"].(map[string]interface{}); ok {
				for fieldName := range key {
					query.Fields = append(query.Fields, models.Field{
						NameExpr: FieldExpr(fieldName),
					})
				}
			}
		}
	}

	return query, nil
}

func convertMongoDropIndex(collection string, doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "DROP INDEX",
		Entity:    TableToEntity(collection),
	}

	if indexName, ok := doc["index"].(string); ok {
		query.NewName = indexName
	}

	return query, nil
}

func convertMongoCreateView(viewName string, doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "CREATE VIEW",
		ViewName:  viewName,
	}

	if viewOn, ok := doc["viewOn"].(string); ok {
		query.Entity = TableToEntity(viewOn)
	}

	return query, nil
}

// ============================================================================
// TCL: TRANSACTION OPERATIONS
// ============================================================================

func convertMongoStartTransaction(opts map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation:   "BEGIN",
		Transaction: &models.Transaction{Operation: "BEGIN"},
	}

	if readConcern, ok := opts["readConcern"].(map[string]interface{}); ok {
		if level, ok := readConcern["level"].(string); ok {
			query.Operation = "SET TRANSACTION"
			query.Transaction.Operation = "SET TRANSACTION"
			query.Transaction.IsolationLevel = level
		}
	}

	if readPreference, ok := opts["readPreference"].(map[string]interface{}); ok {
		if mode, ok := readPreference["mode"].(string); ok && mode == "primary" {
			query.Transaction.ReadOnly = false
		}
	}

	return query, nil
}

// ============================================================================
// DCL: USER MANAGEMENT
// ============================================================================

func convertMongoCreateUser(userName string, doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "CREATE USER",
		Permission: &models.Permission{
			Operation: "CREATE USER",
			UserName:  userName,
		},
	}

	if pwd, ok := doc["pwd"].(string); ok {
		query.Permission.Password = pwd
	}

	if roles, ok := doc["roles"].([]interface{}); ok {
		query.Permission.Roles = extractRoles(roles)
	}

	return query, nil
}

func convertMongoUpdateUser(userName string, doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "ALTER USER",
		Permission: &models.Permission{
			Operation: "ALTER USER",
			UserName:  userName,
		},
	}

	if pwd, ok := doc["pwd"].(string); ok {
		query.Permission.Password = pwd
	}

	if roles, ok := doc["roles"].([]interface{}); ok {
		query.Permission.Roles = extractRoles(roles)
	}

	return query, nil
}

// ============================================================================
// DCL: ROLE MANAGEMENT
// ============================================================================

func convertMongoCreateRole(roleName string, doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "CREATE ROLE",
		Permission: &models.Permission{
			Operation: "CREATE ROLE",
			RoleName:  roleName,
		},
	}

	if roles, ok := doc["roles"].([]interface{}); ok {
		query.Permission.Roles = extractRoles(roles)
	}

	return query, nil
}

func convertMongoGrantRoles(doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "ASSIGN ROLE",
		Permission: &models.Permission{
			Operation: "ASSIGN ROLE",
		},
	}

	if userName, ok := doc["grantRolesToUser"].(string); ok {
		query.Permission.UserName = userName
	}

	if roles, ok := doc["roles"].([]interface{}); ok {
		query.Permission.Roles = extractRoles(roles)
	}

	return query, nil
}

func convertMongoRevokeRoles(doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "REVOKE ROLE",
		Permission: &models.Permission{
			Operation: "REVOKE ROLE",
		},
	}

	if userName, ok := doc["revokeRolesFromUser"].(string); ok {
		query.Permission.UserName = userName
	}

	if roles, ok := doc["roles"].([]interface{}); ok {
		query.Permission.Roles = extractRoles(roles)
	}

	return query, nil
}

func convertMongoGrantPrivileges(doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "GRANT",
		Permission: &models.Permission{
			Operation: "GRANT",
		},
	}

	if roleName, ok := doc["grantPrivilegesToRole"].(string); ok {
		query.Permission.RoleName = roleName
	}

	if privileges, ok := doc["privileges"].([]interface{}); ok {
		query.Permission.Permissions = extractPrivileges(privileges)
	}

	return query, nil
}

func convertMongoRevokePrivileges(doc map[string]interface{}) (*models.Query, error) {
	query := &models.Query{
		Operation: "REVOKE",
		Permission: &models.Permission{
			Operation: "REVOKE",
		},
	}

	if roleName, ok := doc["revokePrivilegesFromRole"].(string); ok {
		query.Permission.RoleName = roleName
	}

	if privileges, ok := doc["privileges"].([]interface{}); ok {
		query.Permission.Permissions = extractPrivileges(privileges)
	}

	return query, nil
}

// ============================================================================
// FILTER CONVERSION - ALL OPERATORS
// ============================================================================

func convertMongoFilter(filter map[string]interface{}) ([]models.Condition, error) {
	var conditions []models.Condition
	isFirst := true

	for field, value := range filter {
		// Handle $and
		if field == "$and" {
			if arr, ok := value.([]interface{}); ok {
				for _, item := range arr {
					if m, ok := item.(map[string]interface{}); ok {
						subConds, err := convertMongoFilter(m)
						if err != nil {
							return nil, err
						}
						for i := range subConds {
							if !isFirst {
								subConds[i].Logic = "AND"
							}
							isFirst = false
						}
						conditions = append(conditions, subConds...)
					}
				}
			}
			continue
		}

		// Handle $or
		if field == "$or" {
			if arr, ok := value.([]interface{}); ok {
				for _, item := range arr {
					if m, ok := item.(map[string]interface{}); ok {
						subConds, err := convertMongoFilter(m)
						if err != nil {
							return nil, err
						}
						for i := range subConds {
							if !isFirst {
								subConds[i].Logic = "OR"
							}
							isFirst = false
						}
						conditions = append(conditions, subConds...)
					}
				}
			}
			continue
		}

		// Handle $nor
		if field == "$nor" {
			if arr, ok := value.([]interface{}); ok {
				for _, item := range arr {
					if m, ok := item.(map[string]interface{}); ok {
						subConds, err := convertMongoFilter(m)
						if err != nil {
							return nil, err
						}
						for i := range subConds {
							subConds[i].Operator = negateOperatorOQL(subConds[i].Operator)
							if !isFirst {
								subConds[i].Logic = "AND"
							}
							isFirst = false
						}
						conditions = append(conditions, subConds...)
					}
				}
			}
			continue
		}

		cond, err := convertFieldCondition(field, value, isFirst)
		if err != nil {
			return nil, err
		}
		if cond != nil {
			conditions = append(conditions, *cond)
			isFirst = false
		}
	}

	return conditions, nil
}

func convertFieldCondition(field string, value interface{}, isFirst bool) (*models.Condition, error) {
	cond := &models.Condition{
		FieldExpr: FieldExpr(field),
	}
	if !isFirst {
		cond.Logic = "AND"
	}

	switch v := value.(type) {
	case map[string]interface{}:
		return convertOperatorCondition(cond, v)
	case nil:
		cond.Operator = "IS_NULL"
	default:
		cond.Operator = "="
		cond.ValueExpr = LiteralExpr(valueToString(v))
	}

	return cond, nil
}

func convertOperatorCondition(cond *models.Condition, ops map[string]interface{}) (*models.Condition, error) {
	// Check for BETWEEN pattern first: {$gte: x, $lte: y}
	gteVal, hasGte := ops["$gte"]
	lteVal, hasLte := ops["$lte"]
	if hasGte && hasLte {
		cond.Operator = "BETWEEN"
		cond.ValueExpr = LiteralExpr(valueToString(gteVal))
		cond.Value2Expr = LiteralExpr(valueToString(lteVal))
		return cond, nil
	}

	// Check for NOT_BETWEEN pattern: {$lt: x, $gt: y} (value < x OR value > y)
	ltVal, hasLt := ops["$lt"]
	gtVal, hasGt := ops["$gt"]
	if hasLt && hasGt {
		cond.Operator = "NOT_BETWEEN"
		cond.ValueExpr = LiteralExpr(valueToString(gtVal))
		cond.Value2Expr = LiteralExpr(valueToString(ltVal))
		return cond, nil
	}

	// Process single operators
	for mongoOp, val := range ops {
		switch mongoOp {
		case "$eq":
			cond.Operator = "="
			cond.ValueExpr = LiteralExpr(valueToString(val))

		case "$ne":
			cond.Operator = "!="
			cond.ValueExpr = LiteralExpr(valueToString(val))

		case "$gt":
			cond.Operator = ">"
			cond.ValueExpr = LiteralExpr(valueToString(val))

		case "$gte":
			cond.Operator = ">="
			cond.ValueExpr = LiteralExpr(valueToString(val))

		case "$lt":
			cond.Operator = "<"
			cond.ValueExpr = LiteralExpr(valueToString(val))

		case "$lte":
			cond.Operator = "<="
			cond.ValueExpr = LiteralExpr(valueToString(val))

		case "$in":
			cond.Operator = "IN"
			if arr, ok := val.([]interface{}); ok {
				for _, item := range arr {
					cond.ValuesExpr = append(cond.ValuesExpr, LiteralExpr(valueToString(item)))
				}
			}

		case "$nin":
			cond.Operator = "NOT_IN"
			if arr, ok := val.([]interface{}); ok {
				for _, item := range arr {
					cond.ValuesExpr = append(cond.ValuesExpr, LiteralExpr(valueToString(item)))
				}
			}

		case "$regex":
			pattern := valueToString(val)
			options := ""
			if opt, ok := ops["$options"].(string); ok {
				options = opt
			}
			if strings.Contains(options, "i") {
				cond.Operator = "ILIKE"
			} else {
				cond.Operator = "LIKE"
			}
			cond.ValueExpr = LiteralExpr(mongoRegexToLike(pattern))

		case "$not":
			if innerOps, ok := val.(map[string]interface{}); ok {
				innerCond, err := convertOperatorCondition(cond, innerOps)
				if err != nil {
					return nil, err
				}
				innerCond.Operator = negateOperatorOQL(innerCond.Operator)
				return innerCond, nil
			}

		case "$exists":
			if exists, ok := val.(bool); ok {
				if exists {
					cond.Operator = "IS_NOT_NULL"
				} else {
					cond.Operator = "IS_NULL"
				}
			}

		case "$type":
			// Type checking - map to special handling
			cond.Operator = "TYPE"
			cond.ValueExpr = LiteralExpr(valueToString(val))

		case "$all":
			// Array contains all elements
			cond.Operator = "CONTAINS_ALL"
			if arr, ok := val.([]interface{}); ok {
				for _, item := range arr {
					cond.ValuesExpr = append(cond.ValuesExpr, LiteralExpr(valueToString(item)))
				}
			}

		case "$elemMatch":
			// Array element matching
			cond.Operator = "ELEM_MATCH"
			if elemOps, ok := val.(map[string]interface{}); ok {
				subConds, _ := convertMongoFilter(elemOps)
				cond.Nested = subConds
			}

		case "$size":
			// Array size
			cond.Operator = "ARRAY_SIZE"
			cond.ValueExpr = LiteralExpr(valueToString(val))

		case "$options":
			// Already handled with $regex
			continue

		default:
			// Unknown operator - use as-is
			cond.Operator = GetOQLOperator(mongoOp, "MongoDB")
			cond.ValueExpr = LiteralExpr(valueToString(val))
		}

		return cond, nil
	}

	return cond, nil
}

// ============================================================================
// UPDATE OPERATORS
// ============================================================================

func convertMongoUpdate(update map[string]interface{}) []models.Field {
	var fields []models.Field

	// $set - direct field assignment
	if set, ok := update["$set"].(map[string]interface{}); ok {
		for _, name := range sortedKeys(set) {
			fields = append(fields, models.Field{
				NameExpr:  FieldExpr(name),
				ValueExpr: LiteralExpr(valueToString(set[name])),
			})
		}
	}

	// $unset - remove fields (set to null)
	if unset, ok := update["$unset"].(map[string]interface{}); ok {
		for _, name := range sortedKeys(unset) {
			fields = append(fields, models.Field{
				NameExpr:  FieldExpr(name),
				ValueExpr: LiteralExpr("NULL"),
			})
		}
	}

	// $inc - increment
	if inc, ok := update["$inc"].(map[string]interface{}); ok {
		for _, name := range sortedKeys(inc) {
			fields = append(fields, models.Field{
				NameExpr:  FieldExpr(name),
				ValueExpr: BinaryExpr(FieldExpr(name), "+", LiteralExpr(valueToString(inc[name]))),
			})
		}
	}

	// $mul - multiply
	if mul, ok := update["$mul"].(map[string]interface{}); ok {
		for _, name := range sortedKeys(mul) {
			fields = append(fields, models.Field{
				NameExpr:  FieldExpr(name),
				ValueExpr: BinaryExpr(FieldExpr(name), "*", LiteralExpr(valueToString(mul[name]))),
			})
		}
	}

	// $min - set to minimum
	if min, ok := update["$min"].(map[string]interface{}); ok {
		for _, name := range sortedKeys(min) {
			fields = append(fields, models.Field{
				NameExpr:  FieldExpr(name),
				ValueExpr: FunctionExpr("MIN", FieldExpr(name), LiteralExpr(valueToString(min[name]))),
			})
		}
	}

	// $max - set to maximum
	if max, ok := update["$max"].(map[string]interface{}); ok {
		for _, name := range sortedKeys(max) {
			fields = append(fields, models.Field{
				NameExpr:  FieldExpr(name),
				ValueExpr: FunctionExpr("MAX", FieldExpr(name), LiteralExpr(valueToString(max[name]))),
			})
		}
	}

	// $rename - rename field
	if rename, ok := update["$rename"].(map[string]interface{}); ok {
		for _, oldName := range sortedKeys(rename) {
			fields = append(fields, models.Field{
				NameExpr:    FieldExpr(oldName),
				ValueExpr:   FieldExpr(valueToString(rename[oldName])),
				Constraints: []string{"RENAME"},
			})
		}
	}

	// $push - array append
	if push, ok := update["$push"].(map[string]interface{}); ok {
		for _, name := range sortedKeys(push) {
			fields = append(fields, models.Field{
				NameExpr:  FieldExpr(name),
				ValueExpr: FunctionExpr("ARRAY_APPEND", FieldExpr(name), LiteralExpr(valueToString(push[name]))),
			})
		}
	}

	// $pull - array remove
	if pull, ok := update["$pull"].(map[string]interface{}); ok {
		for _, name := range sortedKeys(pull) {
			fields = append(fields, models.Field{
				NameExpr:  FieldExpr(name),
				ValueExpr: FunctionExpr("ARRAY_REMOVE", FieldExpr(name), LiteralExpr(valueToString(pull[name]))),
			})
		}
	}

	// $addToSet - array add unique
	if addToSet, ok := update["$addToSet"].(map[string]interface{}); ok {
		for _, name := range sortedKeys(addToSet) {
			fields = append(fields, models.Field{
				NameExpr:  FieldExpr(name),
				ValueExpr: FunctionExpr("ARRAY_ADD_UNIQUE", FieldExpr(name), LiteralExpr(valueToString(addToSet[name]))),
			})
		}
	}

	// $pop - array pop
	if pop, ok := update["$pop"].(map[string]interface{}); ok {
		for _, name := range sortedKeys(pop) {
			value := pop[name]
			dir := "LAST"
			if v, ok := value.(float64); ok && v < 0 {
				dir = "FIRST"
			}
			fields = append(fields, models.Field{
				NameExpr:  FieldExpr(name),
				ValueExpr: FunctionExpr("ARRAY_POP", FieldExpr(name), LiteralExpr(dir)),
			})
		}
	}

	return fields
}

// ============================================================================
// DOCUMENT / PROJECTION / SORT CONVERSION
// ============================================================================

func convertMongoDocument(doc map[string]interface{}) []models.Field {
	// Sort keys for deterministic order
	keys := make([]string, 0, len(doc))
	for k := range doc {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var fields []models.Field
	for _, name := range keys {
		fields = append(fields, models.Field{
			NameExpr:  FieldExpr(name),
			ValueExpr: LiteralExpr(valueToString(doc[name])),
		})
	}
	return fields
}

func convertMongoSort(sortDoc map[string]interface{}) []models.OrderBy {
	// Sort keys for deterministic order
	keys := make([]string, 0, len(sortDoc))
	for k := range sortDoc {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var orderBy []models.OrderBy
	for _, field := range keys {
		dir := sortDoc[field]
		direction := models.Asc
		if d, ok := dir.(float64); ok && d < 0 {
			direction = models.Desc
		}
		if d, ok := dir.(int); ok && d < 0 {
			direction = models.Desc
		}
		orderBy = append(orderBy, models.OrderBy{
			FieldExpr: FieldExpr(field),
			Direction: direction,
		})
	}
	return orderBy
}

// sortedKeys returns map keys in sorted order for deterministic output
func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ============================================================================
// LOOKUP → JOIN
// ============================================================================

func convertMongoLookup(lookup map[string]interface{}) *models.Join {
	from, _ := lookup["from"].(string)
	localField, _ := lookup["localField"].(string)
	foreignField, _ := lookup["foreignField"].(string)

	if from == "" {
		return nil
	}

	return &models.Join{
		Type:      models.LeftJoin,
		Table:     TableToEntity(from),
		LeftExpr:  FieldExpr(localField),
		RightExpr: FieldExpr(foreignField),
	}
}

// ============================================================================
// HELPERS
// ============================================================================

func mongoAggregateToOQL(mongoOp string) string {
	switch mongoOp {
	case "$sum":
		return "SUM"
	case "$avg":
		return "AVG"
	case "$min":
		return "MIN"
	case "$max":
		return "MAX"
	case "$count":
		return "COUNT"
	case "$first":
		return "GET"
	case "$last":
		return "GET"
	default:
		return strings.ToUpper(strings.TrimPrefix(mongoOp, "$"))
	}
}

func negateOperatorOQL(op string) string {
	switch op {
	case "=":
		return "!="
	case "!=":
		return "="
	case ">":
		return "<="
	case ">=":
		return "<"
	case "<":
		return ">="
	case "<=":
		return ">"
	case "IN":
		return "NOT_IN"
	case "NOT_IN":
		return "IN"
	case "LIKE":
		return "NOT_LIKE"
	case "NOT_LIKE":
		return "LIKE"
	case "ILIKE":
		return "NOT_ILIKE"
	case "NOT_ILIKE":
		return "ILIKE"
	case "IS_NULL":
		return "IS_NOT_NULL"
	case "IS_NOT_NULL":
		return "IS_NULL"
	case "BETWEEN":
		return "NOT_BETWEEN"
	case "NOT_BETWEEN":
		return "BETWEEN"
	default:
		return "NOT_" + op
	}
}

func mongoRegexToLike(pattern string) string {
	// Convert basic regex to LIKE pattern
	// ^ → start (implicit in LIKE)
	// $ → end (implicit in LIKE)
	// .* → %
	// Only convert . to _ when it's a single char wildcard (not part of .*)
	result := pattern
	result = strings.TrimPrefix(result, "^")
	result = strings.TrimSuffix(result, "$")
	// Replace .* with % first (before handling single .)
	result = strings.ReplaceAll(result, ".*", "%")
	// Don't convert literal dots - only regex . wildcard would be alone
	// In most cases, dots in patterns like "gmail.com" are literal
	return result
}

func extractRoles(roles []interface{}) []string {
	var result []string
	for _, r := range roles {
		switch role := r.(type) {
		case string:
			result = append(result, role)
		case map[string]interface{}:
			if roleName, ok := role["role"].(string); ok {
				result = append(result, roleName)
			}
		}
	}
	return result
}

func extractPrivileges(privileges []interface{}) []string {
	var result []string
	for _, p := range privileges {
		if privMap, ok := p.(map[string]interface{}); ok {
			if actions, ok := privMap["actions"].([]interface{}); ok {
				for _, action := range actions {
					if actionStr, ok := action.(string); ok {
						result = append(result, strings.ToUpper(actionStr))
					}
				}
			}
		}
	}
	return result
}

func valueToString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		if val == float64(int(val)) {
			return fmt.Sprintf("%d", int(val))
		}
		return fmt.Sprintf("%v", val)
	case int:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("%v", val)
	}
}