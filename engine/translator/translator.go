package translator

import (
	"fmt"                        
                      
	"github.com/omniql-engine/omniql/mapping"          
	"github.com/omniql-engine/omniql/engine/models"        
	pb "github.com/omniql-engine/omniql/utilities/proto" 
)

// Translate routes query to appropriate database translator and wraps in UniversalQuery
func Translate(query *models.Query, dbType string, tenantID string) (*pb.UniversalQuery, error) {
	// Validate database type using mapping
	if !mapping.IsSupportedDatabase(dbType) {
		return nil, fmt.Errorf("unsupported database type: %s (supported: %v)", dbType, mapping.SupportedDatabases)
	}

	switch dbType {
	case "PostgreSQL":
		return translateRelational(query, tenantID, TranslatePostgreSQL, "PostgreSQL")
	
	case "MySQL":
		return translateRelational(query, tenantID, TranslateMySQL, "MySQL")
	
	case "SQLite":
		return translateRelational(query, tenantID, TranslateSQLite, "SQLite")
		
	case "MongoDB":
		return translateDocument(query, tenantID, TranslateMongoDB, "MongoDB")
	
	case "Redis":
		return translateKeyValue(query, tenantID, TranslateRedis, "Redis")
		
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}
}

// translateRelational - Helper to reduce duplication for SQL databases
func translateRelational(
	query *models.Query, 
	tenantID string,
	translator func(*models.Query, string) (*pb.RelationalQuery, error),
	dbName string,
) (*pb.UniversalQuery, error) {
	relQuery, err := translator(query, tenantID)
	if err != nil {
		return nil, err
	}
	
	return &pb.UniversalQuery{
		QueryType: &pb.UniversalQuery_Relational{
			Relational: relQuery,
		},
	}, nil
}

// translateDocument - Helper for MongoDB
func translateDocument(
	query *models.Query,
	tenantID string,
	translator func(*models.Query, string) (*pb.DocumentQuery, error),
	dbName string,
) (*pb.UniversalQuery, error) {
	docQuery, err := translator(query, tenantID)
	if err != nil {
		return nil, err
	}
	
	
	return &pb.UniversalQuery{
		QueryType: &pb.UniversalQuery_Document{
			Document: docQuery,
		},
	}, nil
}

// translateKeyValue - Helper for Redis (NEW)
func translateKeyValue(
	query *models.Query,
	tenantID string,
	translator func(*models.Query, string) (*pb.KeyValueQuery, error),
	dbName string,
) (*pb.UniversalQuery, error) {
	kvQuery, err := translator(query, tenantID)
	if err != nil {
		return nil, err
	}
	
	
	return &pb.UniversalQuery{
		QueryType: &pb.UniversalQuery_KeyValue{
			KeyValue: kvQuery,
		},
	}, nil
}