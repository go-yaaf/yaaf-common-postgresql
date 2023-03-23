// Postgresql SQL query helper to construct SQL queries
//

package postgresql

import (
	"fmt"
	"github.com/lib/pq"
	"strings"

	"github.com/go-yaaf/yaaf-common/database"
)

// region Query helper Methods -----------------------------------------------------------------------------------------

// Build postgres SQL statement with sql arguments based on the query data
func (s *postgresDatabaseQuery) buildStatement(keys ...string) (SQL string, args []any) {

	args = make([]any, 0)

	// Build the SQL select
	tblName := tableName(s.factory().TABLE(), keys...)

	// Build the WHERE clause
	where, args := s.buildCriteria()
	order := s.buildOrder()
	limit := s.buildLimit()

	SQL = fmt.Sprintf(`SELECT id, data FROM "%s" %s %s %s`, tblName, where, order, limit)
	return
}

// Build postgres SQL count statement with sql arguments based on the query data
func (s *postgresDatabaseQuery) buildCountStatement(keys ...string) (SQL string, args []any) {

	args = make([]any, 0)

	// Build the SQL select
	tblName := tableName(s.factory().TABLE(), keys...)

	// Build the WHERE clause
	where, args := s.buildCriteria()

	SQL = fmt.Sprintf(`SELECT count(*) as cnt FROM "%s" %s`, tblName, where)
	return
}

// Build postgres SQL statement with sql arguments based on the query data
func (s *postgresDatabaseQuery) buildIdStatement(keys ...string) (SQL string, args []any) {

	args = make([]any, 0)

	// Build the SQL select
	tblName := tableName(s.factory().TABLE(), keys...)

	// Build the WHERE clause
	where, args := s.buildCriteria()
	order := s.buildOrder()
	limit := s.buildLimit()

	SQL = fmt.Sprintf(`SELECT id FROM "%s" %s %s %s`, tblName, where, order, limit)
	return
}

// Build postgres SQL statement with sql arguments based on the query data
func (s *postgresDatabaseQuery) buildCriteria() (where string, args []any) {
	parts := make([]string, 0, 0)
	varIndex := 1

	// Initialize match all (AND) conditions
	for _, list := range s.allFilters {
		for _, fq := range list {
			part, partArgs := s.buildFilter(fq, varIndex)
			if len(part) > 0 {
				parts = append(parts, part)
				if partArgs != nil {
					args = append(args, partArgs...)
					varIndex += len(partArgs)
				}
			}
		}
	}

	// Initialize match any (OR) conditions
	for _, list := range s.anyFilters {
		orParts := make([]string, 0, 0)
		for _, fq := range list {
			part, partArgs := s.buildFilter(fq, varIndex)
			if len(part) > 0 {
				orParts = append(orParts, part)
				if partArgs != nil {
					args = append(args, partArgs...)
					varIndex += len(partArgs)
				}
			}
		}

		if len(orParts) > 0 {
			orConditions := fmt.Sprintf("(%s)", strings.Join(orParts, " OR "))
			parts = append(parts, orConditions)
		}
	}

	if len(parts) > 0 {
		where = fmt.Sprintf("WHERE %s", strings.Join(parts, " AND "))
	}

	return
}

// Build order clause based on the query data
func (s *postgresDatabaseQuery) buildOrder() string {

	l := len(s.ascOrders) + len(s.descOrders)
	if l == 0 {
		return ""
	}
	fields := make([]string, 0, l)
	for _, field := range s.ascOrders {
		fields = append(fields, fmt.Sprintf("data->>'%s' ASC", field))
	}
	for _, field := range s.descOrders {
		fields = append(fields, fmt.Sprintf("data->>'%s' DESC", field))
	}

	order := fmt.Sprintf("ORDER BY %s", strings.Join(fields, " , "))
	return order
}

// Build limit clause for pagination
func (s *postgresDatabaseQuery) buildLimit() string {
	// Calculate limit and offset from page number and page size (limit)
	var offset int
	if s.limit > 0 {
		if s.page < 2 {
			offset = 0
			return fmt.Sprintf(`LIMIT %d`, s.limit)
		} else {
			offset = (s.page - 1) * s.limit
			return fmt.Sprintf(`LIMIT %d OFFSET %d`, s.limit, offset)
		}
	} else {
		return ""
	}
}

// Build query filter
func (s *postgresDatabaseQuery) buildFilter(qf database.QueryFilter, varIndex int) (sqlPart string, args []any) {

	// Ignore empty values
	if len(qf.GetValues()) == 0 {
		return "", nil
	}

	// Determine the field name and extract operator
	fieldName := qf.GetField()
	if qf.GetField() != "id" {
		fieldName = fmt.Sprintf("data->>'%s'", qf.GetField())
	}

	switch qf.GetOperator() {
	case database.Eq:
		return fmt.Sprintf("(%s = $%d)", fieldName, varIndex), qf.GetValues()
	case database.Neq:
		return fmt.Sprintf("(%s != $%d)", fieldName, varIndex), qf.GetValues()
	case database.Gt:
		return fmt.Sprintf("(%s > $%d)", fieldName, varIndex), qf.GetValues()
	case database.Gte:
		return fmt.Sprintf("(%s >= $%d)", fieldName, varIndex), qf.GetValues()
	case database.Lt:
		return fmt.Sprintf("(%s < $%d)", fieldName, varIndex), qf.GetValues()
	case database.Lte:
		return fmt.Sprintf("(%s <= $%d)", fieldName, varIndex), qf.GetValues()
	case database.Like:
		return s.buildFilterLike(fieldName, qf, varIndex)
	case database.In:
		return s.buildFilterIn(fieldName, qf, varIndex)
	case database.NotIn:
		return s.buildFilterNotIn(fieldName, qf, varIndex)
	case database.Between:
		return fmt.Sprintf("(%s BETWEEN $%d AND $%d)", fieldName, varIndex, varIndex+1), qf.GetValues()
	case database.Contains:
		return fmt.Sprintf("((%s)::jsonb @> $%d)", fieldName, varIndex), qf.GetValues()
	default:
		return fmt.Sprintf("(%s = $%d)", fieldName, varIndex), qf.GetValues()
	}
}

// Build LIKE query filter
func (s *postgresDatabaseQuery) buildFilterLike(fieldName string, qf database.QueryFilter, varIndex int) (sqlPart string, args []any) {

	args = make([]any, 0)
	parts := make([]string, 0)

	for _, value := range qf.GetValues() {
		str := parseWildcards(fmt.Sprintf("%v", value))
		parts = append(parts, fmt.Sprintf("(lower(%s) LIKE lower($%d))", fieldName, varIndex))
		args = append(args, str)
		varIndex++
	}
	sqlPart = fmt.Sprintf("(%s)", strings.Join(parts, " OR "))
	return
}

// Handle special characters: * ?
func parseWildcards(value string) string {
	if strings.Contains(value, "*") {
		return strings.Replace(value, "*", "%", -1)
	} else if strings.Contains(value, "%") {
		return value
	} else {
		return fmt.Sprintf("%%%s%%", value)
	}
}

// Build IN query filter
func (s *postgresDatabaseQuery) buildFilterIn(fieldName string, qf database.QueryFilter, varIndex int) (sqlPart string, args []any) {

	// For string values, use IN clause
	values := qf.GetValues()
	if _, ok := values[0].(string); ok {
		list := createStringsList(values)
		return fmt.Sprintf("(%s IN (%s))", fieldName, list), nil
	} else {
		return fmt.Sprintf("(%s = ANY($%d))", fieldName, varIndex), []any{pq.Array(qf.GetValues())}
	}
}

// Build NOT IN query filter
func (s *postgresDatabaseQuery) buildFilterNotIn(fieldName string, qf database.QueryFilter, varIndex int) (sqlPart string, args []any) {

	// For string values, use IN clause
	values := qf.GetValues()
	if _, ok := values[0].(string); ok {
		list := createStringsList(values)
		return fmt.Sprintf("(%s NOT IN (%s))", fieldName, list), nil
	} else {
		return fmt.Sprintf("NOT (%s = ANY ($%d))", fieldName, varIndex), []any{pq.Array(qf.GetValues())}
	}
}

// Build filter for strings
func createStringsList(values []any) string {
	list := make([]string, 0)
	for _, v := range values {
		list = append(list, fmt.Sprintf("'%v'", v))
	}
	return strings.Join(list, ",")
}

// endregion
