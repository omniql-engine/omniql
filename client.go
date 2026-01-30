// client.go

package oql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/omniql-engine/omniql/engine/translator"
	redisbuilders "github.com/omniql-engine/omniql/engine/builders/redis"
	pb "github.com/omniql-engine/omniql/utilities/proto"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// ============================================
// CLIENT STRUCT
// ============================================

// Client wraps a database connection with OmniQL
type Client struct {
	sqlDB    *sql.DB
	mongoDB  *mongo.Database
	redisDB  *redis.Client
	dbType   string
	tenantID string
	ctx      context.Context
}

// ============================================
// CONSTRUCTORS
// ============================================

// WrapSQL wraps a SQL database connection (PostgreSQL or MySQL)
func WrapSQL(db *sql.DB, dbType string) *Client {
	if dbType != "PostgreSQL" && dbType != "MySQL" {
		dbType = "PostgreSQL"
	}
	return &Client{
		sqlDB:  db,
		dbType: dbType,
		ctx:    context.Background(),
	}
}

// WrapMongo wraps a MongoDB database connection
func WrapMongo(db *mongo.Database) *Client {
	return &Client{
		mongoDB: db,
		dbType:  "MongoDB",
		ctx:     context.Background(),
	}
}

// WrapRedis wraps a Redis client connection
func WrapRedis(rdb *redis.Client, tenantID string) *Client {
	return &Client{
		redisDB:  rdb,
		dbType:   "Redis",
		tenantID: tenantID,
		ctx:      context.Background(),
	}
}

// ============================================
// CONFIGURATION
// ============================================

// SetTenant sets the tenant ID for multi-tenant queries
func (c *Client) SetTenant(tenantID string) {
	c.tenantID = tenantID
}

// SetContext sets the context for database operations
func (c *Client) SetContext(ctx context.Context) {
	c.ctx = ctx
}

// ============================================
// QUERY METHOD
// ============================================

// Query executes an OmniQL or native query and returns results
func (c *Client) Query(input string) ([]map[string]any, error) {
	switch c.dbType {
	case "PostgreSQL", "MySQL":
		return c.querySQL(input)
	case "MongoDB":
		return c.queryMongo(input)
	case "Redis":
		return c.queryRedis(input)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", c.dbType)
	}
}

// ============================================
// SQL IMPLEMENTATION (PostgreSQL, MySQL)
// ============================================

func (c *Client) querySQL(input string) ([]map[string]any, error) {
	query, isOQL, err := Parse(input)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	if !isOQL {
		return nil, fmt.Errorf("OmniQL syntax required: queries must start with ':'")
	}

	result, err := translator.Translate(query, c.dbType, c.tenantID)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}
	sqlString := result.GetRelational().Sql

	upperSQL := strings.ToUpper(strings.TrimSpace(sqlString))

	if strings.HasPrefix(upperSQL, "SELECT") || strings.HasPrefix(upperSQL, "WITH") {
		rows, err := c.sqlDB.QueryContext(c.ctx, sqlString)
		if err != nil {
			return nil, fmt.Errorf("query error: %w", err)
		}
		defer rows.Close()
		return rowsToMaps(rows)
	}

	execResult, err := c.sqlDB.ExecContext(c.ctx, sqlString)
	if err != nil {
		return nil, fmt.Errorf("exec error: %w", err)
	}

	rowsAffected, _ := execResult.RowsAffected()
	lastInsertID, _ := execResult.LastInsertId()

	return []map[string]any{{
		"rows_affected": rowsAffected,
		"inserted_id":   lastInsertID,
	}}, nil
}

// ============================================
// MONGODB IMPLEMENTATION
// ============================================

func (c *Client) queryMongo(input string) ([]map[string]any, error) {
	query, isOQL, err := Parse(input)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	if !isOQL {
		return nil, fmt.Errorf("native MongoDB queries not supported, use OmniQL syntax")
	}

	result, err := translator.Translate(query, "MongoDB", c.tenantID)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}

	docQuery := result.GetDocument()
	collection := c.mongoDB.Collection(docQuery.Collection)
	operation := strings.ToLower(docQuery.Operation)

	switch operation {
	case "find":
		return c.mongoFind(collection, docQuery.Query)
	case "insertone":
		return c.mongoInsert(collection, docQuery.Fields)
	case "updateone":
		return c.mongoUpdate(collection, docQuery.Query, docQuery.Fields)
	case "deleteone":
		return c.mongoDelete(collection, docQuery.Query)
	case "count":
		return c.mongoCount(collection, docQuery.Query)
	default:
		return nil, fmt.Errorf("unsupported MongoDB operation: %s", operation)
	}
}

func (c *Client) mongoFind(coll *mongo.Collection, queryStr string) ([]map[string]any, error) {
	var filter bson.M
	if queryStr != "" && queryStr != "{}" {
		if err := json.Unmarshal([]byte(queryStr), &filter); err != nil {
			filter = bson.M{}
		}
	}

	cursor, err := coll.Find(c.ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("find error: %w", err)
	}
	defer cursor.Close(c.ctx)

	var results []map[string]any
	for cursor.Next(c.ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		results = append(results, bsonToMap(doc))
	}

	return results, nil
}

func (c *Client) mongoInsert(coll *mongo.Collection, fields []*pb.QueryField) ([]map[string]any, error) {
	doc := bson.M{}
	for _, f := range fields {
		if f.NameExpr != nil && f.ValueExpr != nil {
			doc[f.NameExpr.Value] = f.ValueExpr.Value
		}
	}

	result, err := coll.InsertOne(c.ctx, doc)
	if err != nil {
		return nil, fmt.Errorf("insert error: %w", err)
	}

	return []map[string]any{{
		"inserted_id":   result.InsertedID,
		"rows_affected": 1,
	}}, nil
}

func (c *Client) mongoUpdate(coll *mongo.Collection, queryStr string, fields []*pb.QueryField) ([]map[string]any, error) {
	var filter bson.M
	if queryStr != "" {
		json.Unmarshal([]byte(queryStr), &filter)
	}

	updateDoc := bson.M{}
	for _, f := range fields {
		if f.NameExpr != nil && f.ValueExpr != nil {
			updateDoc[f.NameExpr.Value] = f.ValueExpr.Value
		}
	}
	update := bson.M{"$set": updateDoc}

	result, err := coll.UpdateOne(c.ctx, filter, update)
	if err != nil {
		return nil, fmt.Errorf("update error: %w", err)
	}

	return []map[string]any{{
		"rows_affected": result.ModifiedCount,
	}}, nil
}

func (c *Client) mongoDelete(coll *mongo.Collection, queryStr string) ([]map[string]any, error) {
	var filter bson.M
	if queryStr != "" {
		json.Unmarshal([]byte(queryStr), &filter)
	}

	result, err := coll.DeleteOne(c.ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("delete error: %w", err)
	}

	return []map[string]any{{
		"rows_affected": result.DeletedCount,
	}}, nil
}

func (c *Client) mongoCount(coll *mongo.Collection, queryStr string) ([]map[string]any, error) {
	var filter bson.M
	if queryStr != "" {
		json.Unmarshal([]byte(queryStr), &filter)
	}

	count, err := coll.CountDocuments(c.ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("count error: %w", err)
	}

	return []map[string]any{{
		"count": count,
	}}, nil
}

// ============================================
// REDIS IMPLEMENTATION
// ============================================

func (c *Client) queryRedis(input string) ([]map[string]any, error) {
	query, isOQL, err := Parse(input)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	if !isOQL {
		return nil, fmt.Errorf("native Redis queries not supported, use OmniQL syntax")
	}

	result, err := translator.Translate(query, "Redis", c.tenantID)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}

	kvQuery := result.GetKeyValue()
	command := strings.ToUpper(kvQuery.Command)

	switch command {
	case "HGETALL":
		return c.redisGet(kvQuery)
	case "HMSET":
		return c.redisCreate(kvQuery)
	case "HSET":
		return c.redisUpdate(kvQuery)
	case "DEL":
		return c.redisDelete(kvQuery)
	case "COUNT":
		return c.redisCount(kvQuery)
	case "SUM", "AVG", "MIN", "MAX":
		return c.redisAggregate(kvQuery, command)
	case "MULTI":
		return c.redisMulti()
	case "EXEC":
		return c.redisExec()
	case "DISCARD":
		return c.redisDiscard()
	default:
		return nil, fmt.Errorf("unsupported Redis command: %s", command)
	}
}

func (c *Client) redisGet(kvQuery *pb.KeyValueQuery) ([]map[string]any, error) {
	key := kvQuery.Key
	conditions := kvQuery.Conditions
	limit := int(kvQuery.Limit)
	offset := int(kvQuery.Offset)

	// Direct key lookup (no wildcard)
	if !strings.Contains(key, "*") {
		hash, err := c.redisDB.HGetAll(c.ctx, key).Result()
		if err != nil {
			return nil, fmt.Errorf("hgetall error: %w", err)
		}
		if len(hash) == 0 {
			return []map[string]any{}, nil
		}
		return []map[string]any{stringMapToAnyMap(hash)}, nil
	}

	// Pattern scan + filter
	var results []map[string]any
	var cursor uint64
	skipped := 0

	for {
		keys, nextCursor, err := c.redisDB.Scan(c.ctx, cursor, key, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}

		for _, k := range keys {
			hash, err := c.redisDB.HGetAll(c.ctx, k).Result()
			if err != nil || len(hash) == 0 {
				continue
			}

			// Apply conditions filter
			if len(conditions) > 0 {
				if !redisbuilders.MatchesConditions(hash, conditions) {
					continue
				}
			}

			// Handle offset
			if skipped < offset {
				skipped++
				continue
			}

			results = append(results, stringMapToAnyMap(hash))

			// Handle limit
			if limit > 0 && len(results) >= limit {
				return results, nil
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return results, nil
}

func (c *Client) redisCreate(kvQuery *pb.KeyValueQuery) ([]map[string]any, error) {
	key := kvQuery.Key
	args := kvQuery.Args

	// Convert args to field-value pairs
	fieldValues := make([]interface{}, len(args))
	for i, arg := range args {
		fieldValues[i] = arg
	}

	err := c.redisDB.HMSet(c.ctx, key, fieldValues...).Err()
	if err != nil {
		return nil, fmt.Errorf("hmset error: %w", err)
	}

	return []map[string]any{{
		"inserted_id":   key,
		"rows_affected": 1,
	}}, nil
}

func (c *Client) redisUpdate(kvQuery *pb.KeyValueQuery) ([]map[string]any, error) {
	key := kvQuery.Key
	args := kvQuery.Args

	// Convert args to field-value pairs
	for i := 0; i < len(args)-1; i += 2 {
		err := c.redisDB.HSet(c.ctx, key, args[i], args[i+1]).Err()
		if err != nil {
			return nil, fmt.Errorf("hset error: %w", err)
		}
	}

	return []map[string]any{{
		"rows_affected": 1,
	}}, nil
}

func (c *Client) redisDelete(kvQuery *pb.KeyValueQuery) ([]map[string]any, error) {
	key := kvQuery.Key

	// Handle pattern delete
	if strings.Contains(key, "*") {
		keys, err := c.redisDB.Keys(c.ctx, key).Result()
		if err != nil {
			return nil, fmt.Errorf("keys error: %w", err)
		}
		if len(keys) == 0 {
			return []map[string]any{{"rows_affected": 0}}, nil
		}
		deleted, err := c.redisDB.Del(c.ctx, keys...).Result()
		if err != nil {
			return nil, fmt.Errorf("del error: %w", err)
		}
		return []map[string]any{{"rows_affected": deleted}}, nil
	}

	// Single key delete
	deleted, err := c.redisDB.Del(c.ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("del error: %w", err)
	}

	return []map[string]any{{
		"rows_affected": deleted,
	}}, nil
}

func (c *Client) redisCount(kvQuery *pb.KeyValueQuery) ([]map[string]any, error) {
	key := kvQuery.Key
	conditions := kvQuery.Conditions

	keys, err := c.redisDB.Keys(c.ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("keys error: %w", err)
	}

	if len(conditions) == 0 {
		return []map[string]any{{"count": len(keys)}}, nil
	}

	// Count with filter
	count := 0
	for _, k := range keys {
		hash, err := c.redisDB.HGetAll(c.ctx, k).Result()
		if err != nil || len(hash) == 0 {
			continue
		}
		if redisbuilders.MatchesConditions(hash, conditions) {
			count++
		}
	}

	return []map[string]any{{"count": count}}, nil
}

func (c *Client) redisAggregate(kvQuery *pb.KeyValueQuery, operation string) ([]map[string]any, error) {
	key := kvQuery.Key
	conditions := kvQuery.Conditions
	args := kvQuery.Args

	// Get field to aggregate
	field := ""
	if len(args) > 0 {
		field = args[0]
	}

	keys, err := c.redisDB.Keys(c.ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("keys error: %w", err)
	}

	var values []float64
	for _, k := range keys {
		hash, err := c.redisDB.HGetAll(c.ctx, k).Result()
		if err != nil || len(hash) == 0 {
			continue
		}
		if len(conditions) > 0 && !redisbuilders.MatchesConditions(hash, conditions) {
			continue
		}
		if val, ok := hash[field]; ok {
			if num, err := strconv.ParseFloat(val, 64); err == nil {
				values = append(values, num)
			}
		}
	}

	if len(values) == 0 {
		return []map[string]any{{strings.ToLower(operation): 0}}, nil
	}

	var result float64
	switch operation {
	case "SUM":
		for _, v := range values {
			result += v
		}
	case "AVG":
		for _, v := range values {
			result += v
		}
		result /= float64(len(values))
	case "MIN":
		result = values[0]
		for _, v := range values[1:] {
			if v < result {
				result = v
			}
		}
	case "MAX":
		result = values[0]
		for _, v := range values[1:] {
			if v > result {
				result = v
			}
		}
	}

	return []map[string]any{{strings.ToLower(operation): result}}, nil
}

func (c *Client) redisMulti() ([]map[string]any, error) {
	return []map[string]any{{"status": "transaction_started"}}, nil
}

func (c *Client) redisExec() ([]map[string]any, error) {
	return []map[string]any{{"status": "transaction_committed"}}, nil
}

func (c *Client) redisDiscard() ([]map[string]any, error) {
	return []map[string]any{{"status": "transaction_discarded"}}, nil
}

// ============================================
// HELPERS
// ============================================

func rowsToMaps(rows *sql.Rows) ([]map[string]any, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]any

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]any)
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	return results, rows.Err()
}

func stringMapToAnyMap(m map[string]string) map[string]any {
	result := make(map[string]any, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

func bsonToMap(doc bson.M) map[string]any {
	result := make(map[string]any, len(doc))
	for k, v := range doc {
		result[k] = v
	}
	return result
}
