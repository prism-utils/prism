package authz

// Action names HTTP routes authorized by RBAC.
type Action string

// HTTP actions guarded by RBAC middleware.
const (
	ActionQuery  Action = "query"
	ActionIngest Action = "ingest"
	ActionEnsure Action = "ensure"
	ActionStats  Action = "stats"
)

// Role names fixed permission sets in the policy file.
type Role string

// Fixed RBAC roles.
const (
	RoleReader Role = "reader"
	RoleWriter Role = "writer"
	RoleAdmin  Role = "admin"
)

var roleActions = map[Role]map[Action]struct{}{
	RoleReader: {ActionQuery: {}},
	RoleWriter: {ActionIngest: {}},
	RoleAdmin: {
		ActionQuery:  {},
		ActionIngest: {},
		ActionEnsure: {},
		ActionStats:  {},
	},
}

func roleAllows(role Role, action Action) bool {
	actions, ok := roleActions[role]
	if !ok {
		return false
	}
	_, ok = actions[action]
	return ok
}

func parseRole(s string) (Role, bool) {
	switch Role(s) {
	case RoleReader, RoleWriter, RoleAdmin:
		return Role(s), true
	default:
		return "", false
	}
}
