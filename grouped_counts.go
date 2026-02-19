package persistence

import "github.com/uptrace/bun"

// GroupCount is a deterministic grouped-count row shape.
type GroupCount struct {
	Key   string `bun:"group_key"`
	Count int64  `bun:"group_count"`
}

// NewGroupedCountQuery builds a grouped count query ordered by group key.
//
// The query uses bun.Ident placeholders to avoid string-concatenated SQL.
func NewGroupedCountQuery(db bun.IDB, model any, groupColumn string) *bun.SelectQuery {
	return db.NewSelect().
		Model(model).
		ColumnExpr("? AS group_key", bun.Ident(groupColumn)).
		ColumnExpr("COUNT(*) AS group_count").
		GroupExpr("?", bun.Ident(groupColumn)).
		OrderExpr("? ASC", bun.Ident(groupColumn))
}
