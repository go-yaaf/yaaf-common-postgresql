// SQLite SQL query helper to construct SQL queries
//

package sqlitedb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/entity"
)

// isSafeFieldName validates that a field / identifier contains only characters
// that are legal in an entity field path: letters, digits, underscore, dot
// (nested JSON path) and the array marker []. Any other character (quote,
// space, parenthesis, semicolon, comment marker, ...) indicates an attempt to
// break out of the surrounding SQL and is rejected.
//
// Field names are the one part of a query that cannot be passed as a bind
// parameter, so every place that interpolates a field name into SQL must gate
// it through this function.
func isSafeFieldName(name string) bool {
	if name == "" || len(name) > 256 {
		return false
	}
	for _, r := range name {
		if !(r == '_' || r == '.' || r == '[' || r == ']' ||
			(r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')) {
			return false
		}
	}
	return true
}

// region Query helper Methods -----------------------------------------------------------------------------------------

// Build SQLite SQL statement with sql arguments based on the query data
func (s *sqliteDatabaseQuery) buildStatement(keys ...string) (SQL string, args []any) {

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

// Build SQLite SQL count statement with sql arguments based on the query data
// supported aggregations: count, sum, avg, min, max
func (s *sqliteDatabaseQuery) buildCountStatement(field, function string, keys ...string) (SQL string, args []any) {

	args = make([]any, 0)

	// Build the SQL select
	tblName := tableName(s.factory().TABLE(), keys...)

	// Build the WHERE clause
	where, args := s.buildCriteria(0)

	aggr := "*"
	if function != "count" {
		aggr = fmt.Sprintf("CAST(data->>'%s' AS REAL)", field)
	}
	SQL = fmt.Sprintf(`SELECT %s(%s) as aggr FROM "%s" %s`, function, aggr, tblName, where)
	return
}

// Build SQLite SQL statement with sql arguments based on the query data
func (s *sqliteDatabaseQuery) buildIdStatement(keys ...string) (SQL string, args []any) {

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

// Build SQLite SQL statement with sql arguments based on the query data
func (s *sqliteDatabaseQuery) buildCriteria(startFrom int) (where string, args []any) {
	parts := make([]string, 0, 0)
	varIndex := 1
	if startFrom > 0 {
		varIndex = startFrom
	}

	// If range is defined, add it to the filters
	if len(s.rangeField) > 0 {
		if (s.rangeFrom != 0) || (s.rangeTo != 0) {
			rangeFilter := []database.QueryFilter{database.F(s.rangeField).Between(s.rangeFrom, s.rangeTo)}
			s.allFilters = append(s.allFilters, rangeFilter)
		}
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
func (s *sqliteDatabaseQuery) buildOrder() string {

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
func (s *sqliteDatabaseQuery) buildLimit() string {
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
func (s *sqliteDatabaseQuery) buildFilter(qf database.QueryFilter, varIndex int) (sqlPart string, args []any) {

	// handle IN sub-query
	if qf.GetOperator() == database.InSQ {
		return s.buildSubQueryFilter(qf, varIndex, true)
	}

	// handle NOT IN sub-query
	if qf.GetOperator() == database.NotInSQ {
		return s.buildSubQueryFilter(qf, varIndex, false)
	}

	values := qf.GetValues()

	// Ignore empty values for operators Empty | True | False
	if len(values) == 0 {
		if qf.GetOperator() != database.Empty &&
			qf.GetOperator() != database.True &&
			qf.GetOperator() != database.False {
			return "", nil
		}
	}

	// Determine the field name and extract operator
	rawFieldName := qf.GetField()
	fieldName := rawFieldName
	if fieldName != "id" {
		fieldName = s.getCastField(fieldName, qf.GetOperator())
	}

	// Handle JSONB array LIKE/NOT LIKE
	if (qf.GetOperator() == database.Like || qf.GetOperator() == database.NotLike) && strings.Contains(rawFieldName, "[]") {
		return s.buildFilterArrayLike(rawFieldName, qf, varIndex)
	}

	// Sanitize boolean values ( pgx-specific behavior, expects to get it as string )

	switch qf.GetOperator() {
	case database.Eq:
		return fmt.Sprintf("(%s = ?%d)", fieldName, varIndex), values
	case database.Neq:
		return fmt.Sprintf("(%s != ?%d)", fieldName, varIndex), values
	case database.Gt:
		return fmt.Sprintf("(%s > ?%d)", fieldName, varIndex), values
	case database.Gte:
		return fmt.Sprintf("(%s >= ?%d)", fieldName, varIndex), values
	case database.Lt:
		return fmt.Sprintf("(%s < ?%d)", fieldName, varIndex), values
	case database.Lte:
		return fmt.Sprintf("(%s <= ?%d)", fieldName, varIndex), values
	case database.Like:
		return s.buildFilterLike(fieldName, qf, varIndex)
	case database.NotLike:
		return s.buildFilterNotLike(fieldName, qf, varIndex)
	case database.In:
		return s.buildFilterIn(fieldName, qf, varIndex)
	case database.NotIn:
		return s.buildFilterNotIn(fieldName, qf, varIndex)
	case database.Between:
		return fmt.Sprintf("(%s BETWEEN ?%d AND ?%d)", fieldName, varIndex, varIndex+1), values
	case database.Contains:
		// SQLite has no jsonb @> operator: assert every requested element is
		// present in the field's JSON array.
		if jsonArr, jErr := json.Marshal(values); jErr == nil {
			return fmt.Sprintf("(NOT EXISTS (SELECT 1 FROM json_each(?%d) _n WHERE _n.value NOT IN (SELECT value FROM json_each(%s))))", varIndex, fieldName), []any{string(jsonArr)}
		}
		return "", nil
	case database.NotContains:
		if jsonArr, jErr := json.Marshal(values); jErr == nil {
			return fmt.Sprintf("(EXISTS (SELECT 1 FROM json_each(?%d) _n WHERE _n.value NOT IN (SELECT value FROM json_each(%s))))", varIndex, fieldName), []any{string(jsonArr)}
		}
		return "", nil
	case database.Empty:
		return fmt.Sprintf("(%s IS NULL OR %s = '')", fieldName, fieldName), nil
	case database.True:
		return fmt.Sprintf("((%s) = 1)", fieldName), nil
	case database.False:
		return fmt.Sprintf("((%s) = 0)", fieldName), nil
	case database.WithFlag:
		return fmt.Sprintf("((%s & ?%d) = ?%d)", fieldName, varIndex, varIndex), values
	case database.WithNoFlag:
		return fmt.Sprintf("((%s & ?%d) <> ?%d)", fieldName, varIndex, varIndex), values
	default:
		return fmt.Sprintf("(%s = ?%d)", fieldName, varIndex), values
	}
}

func (s *sqliteDatabaseQuery) buildSubQueryFilter(qf database.QueryFilter, varIndex int, in bool) (sqlPart string, args []any) {

	fieldName := qf.GetField()
	if fieldName != "id" {
		fieldName = s.getCastField(fieldName, qf.GetOperator())
	}

	subQuery, ok := qf.GetSubQuery().(*sqliteDatabaseQuery)
	if !ok {
		return "", nil
	}

	tblName := tableName(subQuery.factory().TABLE(), s.keys...)

	where, subQueryArgs := subQuery.buildCriteria(varIndex)
	sqField := qf.GetSubQueryField()
	if sqField != "id" {
		if !isSafeFieldName(sqField) {
			return "", nil
		}
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

func (s *sqliteDatabaseQuery) buildFilterArrayLike(fieldName string, qf database.QueryFilter, varIndex int) (sqlPart string, args []any) {
	args = make([]any, 0)
	parts := make([]string, 0)

	// fieldName is like "simCards[].ip"
	pathParts := strings.Split(fieldName, "[]")
	if len(pathParts) < 2 {
		return "", nil // Should not happen
	}
	arrayField := pathParts[0]
	innerField := strings.TrimPrefix(pathParts[1], ".")

	// Both parts are interpolated as identifiers - reject anything unsafe.
	if !isSafeFieldName(arrayField) || !isSafeFieldName(innerField) {
		return "", nil
	}

	for _, value := range qf.GetValues() {
		str := parseWildcards(fmt.Sprintf("%v", value))
		op := "LIKE"
		exists := "EXISTS"
		if qf.GetOperator() == database.NotLike {
			exists = "NOT EXISTS"
		}

		parts = append(parts, fmt.Sprintf("(%s (SELECT 1 FROM json_each(data->'%s') AS elem WHERE lower(elem.value->>'%s') %s lower(?%d)))", exists, arrayField, innerField, op, varIndex))
		args = append(args, str)
		varIndex++
	}
	sqlPart = fmt.Sprintf("(%s)", strings.Join(parts, " OR "))
	return
}

// Build LIKE query filter
func (s *sqliteDatabaseQuery) buildFilterLike(fieldName string, qf database.QueryFilter, varIndex int) (sqlPart string, args []any) {

	args = make([]any, 0)
	parts := make([]string, 0)

	for _, value := range qf.GetValues() {
		str := parseWildcards(fmt.Sprintf("%v", value))
		parts = append(parts, fmt.Sprintf("(lower(%s) LIKE lower(?%d))", fieldName, varIndex))
		args = append(args, str)
		varIndex++
	}
	sqlPart = fmt.Sprintf("(%s)", strings.Join(parts, " OR "))
	return
}

// Build NOT LIKE query filter
func (s *sqliteDatabaseQuery) buildFilterNotLike(fieldName string, qf database.QueryFilter, varIndex int) (sqlPart string, args []any) {

	args = make([]any, 0)
	parts := make([]string, 0)

	for _, value := range qf.GetValues() {
		str := parseWildcards(fmt.Sprintf("%v", value))
		parts = append(parts, fmt.Sprintf("(lower(%s) NOT LIKE lower(?%d))", fieldName, varIndex))
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
func (s *sqliteDatabaseQuery) buildFilterIn(fieldName string, qf database.QueryFilter, varIndex int) (sqlPart string, args []any) {

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

	// SQLite cannot bind a slice or use "= ANY(array)"; pass the values as a
	// JSON array and expand it with json_each.
	jsonList, err := json.Marshal(list)
	if err != nil {
		return "", nil
	}
	return fmt.Sprintf("(%s IN (SELECT value FROM json_each(?%d)))", fieldName, varIndex), []any{string(jsonList)}
}

// Build NOT IN query filter
func (s *sqliteDatabaseQuery) buildFilterNotIn(fieldName string, qf database.QueryFilter, varIndex int) (sqlPart string, args []any) {

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
	jsonList, err := json.Marshal(list)
	if err != nil {
		return "", nil
	}
	return fmt.Sprintf("(%s NOT IN (SELECT value FROM json_each(?%d)))", fieldName, varIndex), []any{string(jsonList)}
}

// Build the cast
func (s *sqliteDatabaseQuery) getCastField(fieldName string, operator database.QueryOperator) (result string) {

	// Defense-in-depth: a field name cannot be a bind parameter, so it is
	// interpolated into the SQL. Never allow an unsafe identifier through -
	// fall back to a constant literal that can neither match nor break out of
	// the statement (an always-false predicate / constant ORDER BY term).
	if !isSafeFieldName(fieldName) {
		return "'__invalid_field__'"
	}

	// Check if field's name is in map of "data" fields
	// if it is not, treat it as a native column name
	// this is introduced with aim to use native indices for large
	// datasets of >1M records.
	_, ok := s.filedNameToType[fieldName]
	if !ok {
		return fieldName
	}

	// Convert to SQLite Jsonb query
	if operator == database.Contains {
		return fmt.Sprintf("(data->'%s')", fieldName)
	}

	dataField := fmt.Sprintf("data->>'%s'", fieldName)
	if strings.Contains(fieldName, ".") {
		dataField = fmt.Sprintf("data->>'$.%s'", fieldName)
	}

	fieldTypeAsString, ok := s.filedNameToType[fieldName]
	if !ok {
		return dataField
	}
	switch fieldTypeAsString {
	case "byte", "uint8", "int", "uint", "int32", "int64", "entity.Timestamp", "bool":
		return fmt.Sprintf("CAST(%s AS INTEGER)", dataField)
	case "float32", "float64":
		return fmt.Sprintf("CAST(%s AS REAL)", dataField)
	default:
		return dataField
	}
}

// endregion

func (s *sqliteDatabaseQuery) convertAnyArray(value any) (result []any) {

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
	fieldsMap := make(map[string]string)
	typ := reflect.TypeOf(v)

	extractFields(fieldsMap, typ, "")
	return fieldsMap
}

func extractFields(fieldsMap map[string]string, t reflect.Type, prefix string) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		ft := field.Type

		// Get json tag or fallback to field name
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" {
			jsonTag = field.Name
		}

		// Compose name with prefix
		fieldName := jsonTag
		if prefix != "" {
			fieldName = prefix + "." + fieldName
		}

		// Check if embedded (anonymous) field
		if field.Anonymous && ft.Kind() == reflect.Struct {
			extractFields(fieldsMap, ft, prefix) // Inherit fields without prefixing field name
			continue
		}

		fieldsMap[fieldName] = ft.String()

		// Recursively handle struct fields
		switch ft.Kind() {
		case reflect.Struct:
			extractFields(fieldsMap, ft, fieldName)
		case reflect.Ptr:
			if ft.Elem().Kind() == reflect.Struct {
				extractFields(fieldsMap, ft.Elem(), fieldName)
			}
		case reflect.Slice, reflect.Array:
			elem := ft.Elem()
			if elem.Kind() == reflect.Struct {
				extractFields(fieldsMap, elem, fieldName+"[]")
			}
		default:
			continue
		}
	}
}
