package persistence

import "fmt"
import "strings"

const (
	VirtualDialectPostgres = "postgres"
	VirtualDialectSQLite   = "sqlite"
)

// VirtualFieldExpr returns a SQL snippet for the given dialect to access a JSON/JSONB field.
// When asJSON is false, text extraction is used (suitable for comparisons/order-by).
// When asJSON is true, the raw JSON value is returned.
func VirtualFieldExpr(dialect, sourceField, key string, asJSON bool) string {
	switch strings.ToLower(dialect) {
	case VirtualDialectSQLite:
		// json_extract(metadata, '$.key')
		return fmt.Sprintf("json_extract(%s, '$.%s')", sourceField, key)
	case VirtualDialectPostgres:
		fallthrough
	default:
		if asJSON {
			// metadata->'key'
			return fmt.Sprintf("%s->'%s'", sourceField, key)
		}
		// metadata->>'key'
		return fmt.Sprintf("%s->>'%s'", sourceField, key)
	}
}
