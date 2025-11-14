package parser

import (
	"fmt"
	"strings"
	"github.com/omniql-engine/omniql/engine/models"
)

// dclParsers maps DCL operations to their parser functions
var dclParsers = map[string]func([]string) (*models.Query, error){
	"GRANT":       ParseGrant,
	"REVOKE":      ParseRevoke,
	"CREATE ROLE": ParseCreateRole,
	"DROP ROLE":   ParseDropRole,
	"ASSIGN ROLE": ParseAssignRole,
	"REVOKE ROLE": ParseRevokeRole,
	"CREATE USER": ParseCreateUser,
	"DROP USER":   ParseDropUser,
	"ALTER USER":  ParseAlterUser,
}

// parseDCL routes DCL operations to specific parsers using function map
func parseDCL(operation string, parts []string) (*models.Query, error) {
	parser, exists := dclParsers[operation]
	if !exists {
		return nil, fmt.Errorf("unknown DCL operation: %s", operation)
	}
	return parser(parts)
}

// ============================================================================
// PERMISSION OPERATIONS
// ============================================================================

// ParseGrant handles: GRANT permissions ON table TO target
// Examples:
// - GRANT READ ON User TO tenant_abc
// - GRANT READ, WRITE ON Project TO role_editor
// - GRANT ALL ON User TO tenant_xyz
func ParseGrant(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "GRANT",
		Permission: &models.Permission{
			Operation: "GRANT",
		},
	}
	
	// Find ON keyword
	onIndex := findKeyword(parts, "ON")
	if onIndex == -1 {
		return nil, fmt.Errorf("GRANT requires ON clause")
	}
	
	// Find TO keyword
	toIndex := findKeyword(parts, "TO")
	if toIndex == -1 {
		return nil, fmt.Errorf("GRANT requires TO clause")
	}
	
	entityIndex := getEntityIndex(query.Operation)
	
	// Permissions are between operation and ON
	if onIndex <= entityIndex {
		return nil, fmt.Errorf("GRANT requires permissions")
	}
	
	permissionsStr := strings.Join(parts[entityIndex:onIndex], " ")
	// Split by comma and trim spaces
	permissionsList := strings.Split(permissionsStr, ",")
	var permissions []string
	for _, perm := range permissionsList {
		permissions = append(permissions, strings.TrimSpace(perm))
	}
	query.Permission.Permissions = permissions
	
	// Table name is between ON and TO
	if toIndex-onIndex < 2 {
		return nil, fmt.Errorf("GRANT ON requires table name")
	}
	query.Entity = parts[onIndex+1]
	
	// Target is after TO
	if toIndex+1 >= len(parts) {
		return nil, fmt.Errorf("GRANT TO requires target (tenant_id or role_name)")
	}
	query.Permission.Target = parts[toIndex+1]
	
	return query, nil
}

// ParseRevoke handles: REVOKE permissions ON table FROM target
// Examples:
// - REVOKE WRITE ON User FROM tenant_abc
// - REVOKE ALL ON Project FROM role_viewer
func ParseRevoke(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "REVOKE",
		Permission: &models.Permission{
			Operation: "REVOKE",
		},
	}
	
	// Find ON keyword
	onIndex := findKeyword(parts, "ON")
	if onIndex == -1 {
		return nil, fmt.Errorf("REVOKE requires ON clause")
	}
	
	// Find FROM keyword
	fromIndex := findKeyword(parts, "FROM")
	if fromIndex == -1 {
		return nil, fmt.Errorf("REVOKE requires FROM clause")
	}
	
	entityIndex := getEntityIndex(query.Operation)
	
	// Permissions are between operation and ON
	if onIndex <= entityIndex {
		return nil, fmt.Errorf("REVOKE requires permissions")
	}
	
	permissionsStr := strings.Join(parts[entityIndex:onIndex], " ")
	permissionsList := strings.Split(permissionsStr, ",")
	var permissions []string
	for _, perm := range permissionsList {
		permissions = append(permissions, strings.TrimSpace(perm))
	}
	query.Permission.Permissions = permissions
	
	// Table name is between ON and FROM
	if fromIndex-onIndex < 2 {
		return nil, fmt.Errorf("REVOKE ON requires table name")
	}
	query.Entity = parts[onIndex+1]
	
	// Target is after FROM
	if fromIndex+1 >= len(parts) {
		return nil, fmt.Errorf("REVOKE FROM requires target")
	}
	query.Permission.Target = parts[fromIndex+1]
	
	return query, nil
}

// ============================================================================
// ROLE OPERATIONS
// ============================================================================

// ParseCreateRole handles: CREATE ROLE role_name [WITH permissions]
// Examples:
// - CREATE ROLE editor
// - CREATE ROLE viewer WITH READ
// - CREATE ROLE admin WITH READ, WRITE, DELETE
func ParseCreateRole(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "CREATE ROLE",
		Permission: &models.Permission{
			Operation: "CREATE ROLE",
		},
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("CREATE ROLE requires role name")
	}
	
	query.Permission.RoleName = parts[entityIndex]
	
	// Check for optional WITH clause (permissions)
	withIndex := findKeyword(parts, "WITH")
	if withIndex != -1 && withIndex+1 < len(parts) {
		permissionsStr := strings.Join(parts[withIndex+1:], " ")
		permissionsList := strings.Split(permissionsStr, ",")
		var permissions []string
		for _, perm := range permissionsList {
			permissions = append(permissions, strings.TrimSpace(perm))
		}
		query.Permission.Permissions = permissions
	}
	
	return query, nil
}

// ParseDropRole handles: DROP ROLE role_name
func ParseDropRole(parts []string) (*models.Query, error) {
	entityIndex := getEntityIndex("DROP ROLE")
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("DROP ROLE requires role name")
	}
	
	return &models.Query{
		Operation: "DROP ROLE",
		Permission: &models.Permission{
			Operation: "DROP ROLE",
			RoleName:  parts[entityIndex],
		},
	}, nil
}

// ParseAssignRole handles: ASSIGN ROLE role_name TO target
// Format: ASSIGN ROLE rolename TO username
// Examples:
// - ASSIGN ROLE editor TO tenant_abc
// - ASSIGN ROLE viewer TO user_john
func ParseAssignRole(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "ASSIGN ROLE",
		Permission: &models.Permission{
			Operation: "ASSIGN ROLE",
		},
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("ASSIGN ROLE requires role name")
	}
	
	toIndex := findKeyword(parts, "TO")
	if toIndex == -1 || toIndex+1 >= len(parts) {
		return nil, fmt.Errorf("ASSIGN ROLE requires TO clause")
	}
	
	query.Permission.RoleName = parts[entityIndex]
	query.Permission.Target = parts[toIndex+1]
	query.Permission.UserName = parts[toIndex+1]
	
	return query, nil
}

// ParseRevokeRole handles: REVOKE ROLE role_name FROM target
// Format: REVOKE ROLE rolename FROM username
func ParseRevokeRole(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "REVOKE ROLE",
		Permission: &models.Permission{
			Operation: "REVOKE ROLE",
		},
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("REVOKE ROLE requires role name")
	}
	
	fromIndex := findKeyword(parts, "FROM")
	if fromIndex == -1 || fromIndex+1 >= len(parts) {
		return nil, fmt.Errorf("REVOKE ROLE requires FROM clause")
	}
	
	query.Permission.RoleName = parts[entityIndex]
	query.Permission.Target = parts[fromIndex+1]
	query.Permission.UserName = parts[fromIndex+1]
	
	return query, nil
}

// ============================================================================
// USER OPERATIONS
// ============================================================================

// ParseCreateUser handles: CREATE USER username WITH PASSWORD password [ROLES role1,role2]
// Examples:
// - CREATE USER john WITH PASSWORD secret123
// - CREATE USER admin WITH PASSWORD admin123 ROLES admin,editor
func ParseCreateUser(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "CREATE USER",
		Permission: &models.Permission{
			Operation: "CREATE USER",
		},
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("CREATE USER requires username")
	}
	
	query.Permission.UserName = parts[entityIndex]
	
	// Find WITH keyword
	withIndex := findKeyword(parts, "WITH")
	if withIndex == -1 {
		return nil, fmt.Errorf("CREATE USER requires WITH PASSWORD clause")
	}
	
	// Find PASSWORD keyword
	passwordIndex := findKeyword(parts, "PASSWORD")
	if passwordIndex == -1 || passwordIndex+1 >= len(parts) {
		return nil, fmt.Errorf("CREATE USER requires PASSWORD")
	}
	
	// Find ROLES keyword (optional)
	rolesIndex := findKeyword(parts, "ROLES")
	
	// Password is between PASSWORD and ROLES (or end)
	endIndex := len(parts)
	if rolesIndex != -1 {
		endIndex = rolesIndex
	}
	
	if passwordIndex+1 >= endIndex {
		return nil, fmt.Errorf("CREATE USER PASSWORD requires value")
	}
	
	query.Permission.Password = parts[passwordIndex+1]
	
	// Parse roles if specified
	if rolesIndex != -1 && rolesIndex+1 < len(parts) {
		rolesStr := strings.Join(parts[rolesIndex+1:], " ")
		rolesList := strings.Split(rolesStr, ",")
		var roles []string
		for _, role := range rolesList {
			roles = append(roles, strings.TrimSpace(role))
		}
		query.Permission.Roles = roles
	}
	
	return query, nil
}

// ParseDropUser handles: DROP USER username
func ParseDropUser(parts []string) (*models.Query, error) {
	entityIndex := getEntityIndex("DROP USER")
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("DROP USER requires username")
	}
	
	return &models.Query{
		Operation: "DROP USER",
		Permission: &models.Permission{
			Operation: "DROP USER",
			UserName:  parts[entityIndex],
		},
	}, nil
}

// ParseAlterUser handles: ALTER USER username WITH PASSWORD new_password [ROLES role1,role2]
// Format: ALTER USER username WITH PASSWORD password (consistent with CREATE USER)
// Also supports: ALTER USER username SET PASSWORD password (alternative syntax)
// Examples:
// - ALTER USER john WITH PASSWORD newpass123
// - ALTER USER admin SET PASSWORD admin456 ROLES admin
func ParseAlterUser(parts []string) (*models.Query, error) {
	query := &models.Query{
		Operation: "ALTER USER",
		Permission: &models.Permission{
			Operation: "ALTER USER",
		},
	}
	
	entityIndex := getEntityIndex(query.Operation)
	if len(parts) < entityIndex+1 {
		return nil, fmt.Errorf("ALTER USER requires username")
	}
	
	query.Permission.UserName = parts[entityIndex]
	
	// Support both WITH and SET keywords for consistency
	withIndex := findKeyword(parts, "WITH")
	setIndex := findKeyword(parts, "SET")
	
	keywordIndex := -1
	if withIndex != -1 {
		keywordIndex = withIndex
	} else if setIndex != -1 {
		keywordIndex = setIndex
	}
	
	if keywordIndex == -1 {
		return nil, fmt.Errorf("ALTER USER requires WITH or SET clause")
	}
	
	// Find PASSWORD keyword
	passwordIndex := findKeyword(parts, "PASSWORD")
	if passwordIndex != -1 && passwordIndex+1 < len(parts) {
		// Find ROLES keyword (optional)
		rolesIndex := findKeyword(parts, "ROLES")
		
		endIndex := len(parts)
		if rolesIndex != -1 {
			endIndex = rolesIndex
		}
		
		if passwordIndex+1 < endIndex {
			query.Permission.Password = parts[passwordIndex+1]
		}
		
		// Parse roles if specified
		if rolesIndex != -1 && rolesIndex+1 < len(parts) {
			rolesStr := strings.Join(parts[rolesIndex+1:], " ")
			rolesList := strings.Split(rolesStr, ",")
			var roles []string
			for _, role := range rolesList {
				roles = append(roles, strings.TrimSpace(role))
			}
			query.Permission.Roles = roles
		}
	}
	
	return query, nil
}