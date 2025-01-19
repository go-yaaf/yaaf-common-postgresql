// Postgresql SQL query helper to construct SQL queries
//

package postgresql

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/entity"
)

// region Query helper Methods -----------------------------------------------------------------------------------------

// Build postgres SQL statement with sql arguments based on the query data
func (s *postgresDatabaseQuery) buildStatement(keys ...string) (SQL string, args []any) {

	args = make([]any, 0)
	s.keys = make([]string, 0)
	s.keys = append(s.keys, keys...)

	// Build the SQL select
	tblName := tableName(s.factory().TABLE(), keys...)

	// Build the WHERE clause
	where, args := s.buildCriteria(0)
	order := s.buildOrder()
	limit := s.buildLimit()

	SQL = fmt.Sprintf(`SELECT id, data FROM "%s" %s %s %s`, tblName, where, order, limit)
	return
}

// Build postgres SQL count statement with sql arguments based on the query data
// supported aggregations: count, sum, avg, min, max
func (s *postgresDatabaseQuery) buildCountStatement(field, function string, keys ...string) (SQL string, args []any) {

	args = make([]any, 0)

	// Build the SQL select
	tblName := tableName(s.factory().TABLE(), keys...)

	// Build the WHERE clause
	where, args := s.buildCriteria(0)

	aggr := "*"
	if function != "count" {
		aggr = fmt.Sprintf("(data->>'%s')::FLOAT", field)
	}
	SQL = fmt.Sprintf(`SELECT %s(%s) as aggr FROM "%s" %s`, function, aggr, tblName, where)
	return
}

// Build postgres SQL statement with sql arguments based on the query data
func (s *postgresDatabaseQuery) buildIdStatement(keys ...string) (SQL string, args []any) {

	args = make([]any, 0)

	// Build the SQL select
	tblName := tableName(s.factory().TABLE(), keys...)

	// Build the WHERE clause
	where, args := s.buildCriteria(0)
	order := s.buildOrder()
	limit := s.buildLimit()

	SQL = fmt.Sprintf(`SELECT id FROM "%s" %s %s %s`, tblName, where, order, limit)
	return
}

// Build postgres SQL statement with sql arguments based on the query data
func (s *postgresDatabaseQuery) buildCriteria(startFrom int) (where string, args []any) {
	parts := make([]string, 0, 0)
	varIndex := 1
	if startFrom > 0 {
		varIndex = startFrom
	}

	// If range is defined, add it to the filters
	if len(s.rangeField) > 0 {
		rangeFilter := []database.QueryFilter{database.F(s.rangeField).Between(s.rangeFrom, s.rangeTo)}
		s.allFilters = append(s.allFilters, rangeFilter)
	}

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
		fields = append(fields, fmt.Sprintf(" %s ASC", s.getCastField(field.(string), database.Eq)))
	}

	for _, field := range s.descOrders {
		fields = append(fields, fmt.Sprintf(" %s DESC", s.getCastField(field.(string), database.Eq)))
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

	// handle IN sub-query
	if qf.GetOperator() == database.InSQ {
		return s.buildSubQueryFilter(qf, varIndex, true)
	}

	// handle NOT IN sub-query
	if qf.GetOperator() == database.NotInSQ {
		return s.buildSubQueryFilter(qf, varIndex, false)
	}

	values := qf.GetValues()

	// Ignore empty values
	if qf.GetOperator() != database.Empty {
		if len(values) == 0 {
			return "", nil
		}
	}

	// Determine the field name and extract operator
	fieldName := qf.GetField()
	if fieldName != "id" {
		fieldName = s.getCastField(fieldName, qf.GetOperator())
	}
	// Sanitize boolean values ( pgx-specific behaviour, expects to get it as string )

	switch qf.GetOperator() {
	case database.Eq:
		return fmt.Sprintf("(%s = $%d)", fieldName, varIndex), values
	case database.Neq:
		return fmt.Sprintf("(%s != $%d)", fieldName, varIndex), values
	case database.Gt:
		return fmt.Sprintf("(%s > $%d)", fieldName, varIndex), values
	case database.Gte:
		return fmt.Sprintf("(%s >= $%d)", fieldName, varIndex), values
	case database.Lt:
		return fmt.Sprintf("(%s < $%d)", fieldName, varIndex), values
	case database.Lte:
		return fmt.Sprintf("(%s <= $%d)", fieldName, varIndex), values
	case database.Like:
		return s.buildFilterLike(fieldName, qf, varIndex)
	case database.In:
		return s.buildFilterIn(fieldName, qf, varIndex)
	case database.NotIn:
		return s.buildFilterNotIn(fieldName, qf, varIndex)
	case database.Between:
		return fmt.Sprintf("(%s BETWEEN $%d AND $%d)", fieldName, varIndex, varIndex+1), values
	case database.Contains:
		arr := toArray(values)
		return fmt.Sprintf("%s @> '[%s]'", fieldName, arr), nil
	case database.Empty:
		return fmt.Sprintf("((%s = '') IS NOT FALSE)", fieldName), values
	default:
		return fmt.Sprintf("(%s = $%d)", fieldName, varIndex), values
	}
}

func (s *postgresDatabaseQuery) buildSubQueryFilter(qf database.QueryFilter, varIndex int, in bool) (sqlPart string, args []any) {

	fieldName := qf.GetField()
	if fieldName != "id" {
		fieldName = s.getCastField(fieldName, qf.GetOperator())
	}

	subQuery, ok := qf.GetSubQuery().(*postgresDatabaseQuery)
	if !ok {
		return "", nil
	}

	tblName := tableName(subQuery.factory().TABLE(), s.keys...)

	where, subQueryArgs := subQuery.buildCriteria(varIndex)
	sqField := qf.GetSubQueryField()
	if sqField != "id" {
		sqField = fmt.Sprintf("data->>'%s'", sqField)
	}

	operator := "NOT IN"
	if in {
		operator = "IN"
	}

	sqTableName := tblName
	SQL := fmt.Sprintf(`SELECT %s FROM "%s" %s`, sqField, sqTableName, where)
	return fmt.Sprintf("(%s %s (%s))", fieldName, operator, SQL), subQueryArgs
}

func toArray(values []any) string {
	result := make([]string, 0)
	for _, v := range values {
		if val, ok := v.(string); ok {
			result = append(result, fmt.Sprintf("\"%v\"", val))
		} else {
			result = append(result, fmt.Sprintf("%v", v))
		}

	}
	return strings.Join(result, ", ")
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

	// If value is of type array, convert each item to an array
	list := make([]any, 0)

	for _, val := range qf.GetValues() {
		if reflect.TypeOf(val).Kind() == reflect.Slice {
			items := s.convertAnyArray(val)
			for _, item := range items {
				list = append(list, item)
			}
		} else {
			list = append(list, val)
		}
	}
	return fmt.Sprintf("(%s = ANY($%d))", fieldName, varIndex), []any{list}
}

// Build NOT IN query filter
func (s *postgresDatabaseQuery) buildFilterNotIn(fieldName string, qf database.QueryFilter, varIndex int) (sqlPart string, args []any) {

	// If value is of type array, convert each item to an array
	list := make([]any, 0)

	for _, val := range qf.GetValues() {
		if reflect.TypeOf(val).Kind() == reflect.Slice {
			items := s.convertAnyArray(val)
			for _, item := range items {
				list = append(list, item)
			}
		} else {
			list = append(list, val)
		}
	}
	return fmt.Sprintf("NOT (%s = ANY ($%d))", fieldName, varIndex), []any{list}
}

// Build the cast
func (s *postgresDatabaseQuery) getCastField(fieldName string, operator database.QueryOperator) (result string) {

	//check if field's name is in map of "data" fields
	//if it is not, treat it as a native column name
	//this is introduced with aim to use native indecies for large
	//datasets of >1M records.
	_, ok := s.filedNameToType[strings.ToLower(fieldName)]
	if !ok {
		return fieldName
	}

	// Convert to Postgres Jsonb query
	if operator == database.Contains {
		return fmt.Sprintf("(data->'%s')", fieldName)
	}

	dataField := fmt.Sprintf("data->>'%s'", fieldName)
	if strings.Contains(fieldName, ".") {
		fields := strings.ReplaceAll(fieldName, ".", ",")
		dataField = fmt.Sprintf("data#>>'{%s}'", fields)
	}

	fieldTypeAsString, ok := s.filedNameToType[strings.ToLower(fieldName)]
	if !ok {
		return dataField
	}
	switch fieldTypeAsString {
	case "byte":
		return fmt.Sprintf("(%s)::SMALLINT", dataField)
	case "uint8":
		return fmt.Sprintf("(%s)::SMALLINT", dataField)
	case "int":
		return fmt.Sprintf("(%s)::BIGINT", dataField)
	case "uint":
		return fmt.Sprintf("(%s)::BIGINT", dataField)
	case "int32":
		return fmt.Sprintf("(%s)::BIGINT", dataField)
	case "int64":
		return fmt.Sprintf("(%s)::BIGINT", dataField)
	case "entity.Timestamp":
		return fmt.Sprintf("(%s)::BIGINT", dataField)
	case "float32":
		return fmt.Sprintf("(%s)::FLOAT", dataField)
	case "float64":
		return fmt.Sprintf("(%s)::FLOAT", dataField)
	case "bool":
		return fmt.Sprintf("(%s)::BOOLEAN", dataField)
	default:
		return fmt.Sprintf("%s", dataField)
	}
}

// endregion

func (s *postgresDatabaseQuery) convertAnyArray(value any) (result []any) {

	switch v := value.(type) {
	case []any:
		for _, item := range v {
			result = append(result, item)
		}
	case []int:
		for _, item := range v {
			result = append(result, item)
		}
	case []uint:
		for _, item := range v {
			result = append(result, item)
		}
	case []int32:
		for _, item := range v {
			result = append(result, item)
		}
	case []int64:
		for _, item := range v {
			result = append(result, item)
		}
	case []uint32:
		for _, item := range v {
			result = append(result, item)
		}
	case []uint64:
		for _, item := range v {
			result = append(result, item)
		}
	case []string:
		for _, item := range v {
			result = append(result, item)
		}
	case []float32:
		for _, item := range v {
			result = append(result, item)
		}
	case []float64:
		for _, item := range v {
			result = append(result, item)
		}
	case []bool:
		for _, item := range v {
			result = append(result, item)
		}
	default:
		result = append(result, value)
	}
	return result
}

func entityFieldsToTypesMap(ef entity.EntityFactory) map[string]string {

	v := ef()

	fieldsMap := map[string]string{
		"createdon": "int64",
		"updatedon": "int64",
		"id":        "string",
	}

	val := reflect.ValueOf(v)

	// We're only interested in structs
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil
	}

	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		fieldsMap[strings.ToLower(field.Name)] = field.Type.String()
	}
	return fieldsMap
}
