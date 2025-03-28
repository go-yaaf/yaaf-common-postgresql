// Postgresql database implementation of IQuery
//

package postgresql

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"strconv"
	"strings"
	"time"

	"github.com/go-yaaf/yaaf-common/database"
	. "github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/utils/collections"
)

var functions = []string{"count", "avg", "sum", "min", "max"}

// region postgres query internal structure ----------------------------------------------------------------------------

type postgresDatabaseQuery struct {
	db              *PostgresDatabase        // A reference to the underlying IDatabase
	factory         EntityFactory            // The entity factory method
	allFilters      [][]database.QueryFilter // List of lists of AND filters
	anyFilters      [][]database.QueryFilter // List of lists of OR filters
	ascOrders       []any                    // List of fields for ASC order
	descOrders      []any                    // List of fields for DESC order
	callbacks       []func(in Entity) Entity // List of entity transformation callback functions
	page            int                      // Page number (for pagination)
	limit           int                      // Page size: how many results in a page (for pagination)
	rangeField      string                   // Field name for range filter (must be timestamp field)
	rangeFrom       Timestamp                // Start timestamp for range filter
	rangeTo         Timestamp                // End timestamp for range filter
	filedNameToType map[string]string        // Holds map of Entity's field s names to their types
	keys            []string                 // partition keys
}

// endregion

// region Query Construction Methods -----------------------------------------------------------------------------------

// Apply adds a callback to apply on each result entity in the query
func (s *postgresDatabaseQuery) Apply(cb func(in Entity) Entity) database.IQuery {
	if cb != nil {
		s.callbacks = append(s.callbacks, cb)
	}
	return s
}

// Filter Add single field filter
func (s *postgresDatabaseQuery) Filter(filter database.QueryFilter) database.IQuery {
	if filter.IsActive() {
		s.allFilters = append(s.allFilters, []database.QueryFilter{filter})
	}
	return s
}

// Range add time frame filter on specific time field
func (s *postgresDatabaseQuery) Range(field string, from Timestamp, to Timestamp) database.IQuery {
	s.rangeField = field
	s.rangeFrom = from
	s.rangeTo = to
	return s
}

// MatchAll Add list of filters, all of them should be satisfied (AND)
func (s *postgresDatabaseQuery) MatchAll(filters ...database.QueryFilter) database.IQuery {
	list := make([]database.QueryFilter, 0)
	for _, filter := range filters {
		if filter.IsActive() {
			list = append(list, filter)
		}
	}
	s.allFilters = append(s.allFilters, list)
	return s
}

// MatchAny Add list of filters, any of them should be satisfied (OR)
func (s *postgresDatabaseQuery) MatchAny(filters ...database.QueryFilter) database.IQuery {
	list := make([]database.QueryFilter, 0)
	for _, filter := range filters {
		if filter.IsActive() == true {
			list = append(list, filter)
		}
	}
	s.anyFilters = append(s.anyFilters, list)
	return s
}

// Sort Add sort order by field,  expects sort parameter in the following form: field_name (Ascending) or field_name- (Descending)
func (s *postgresDatabaseQuery) Sort(sort string) database.IQuery {
	if sort == "" {
		return s
	}

	// as a default, order will be ASC
	if strings.HasSuffix(sort, "-") {
		s.descOrders = append(s.descOrders, sort[0:len(sort)-1])
	} else if strings.HasSuffix(sort, "+") {
		s.ascOrders = append(s.ascOrders, sort[0:len(sort)-1])
	} else {
		s.ascOrders = append(s.ascOrders, sort)
	}
	return s
}

// Limit Set page size limit (for pagination)
func (s *postgresDatabaseQuery) Limit(limit int) database.IQuery {
	s.limit = limit
	return s
}

// Page Set requested page number (used for pagination)
func (s *postgresDatabaseQuery) Page(page int) database.IQuery {
	s.page = page
	return s
}

// endregion

// region QueryBuilder Execution Methods -------------------------------------------------------------------------------

// List Execute a query to get list of entities by IDs (the criteria is ignored)
func (s *postgresDatabaseQuery) List(entityIDs []string, keys ...string) (out []Entity, err error) {

	result, err := s.db.List(s.factory, entityIDs, keys...)
	if err != nil {
		return nil, err
	}

	// Apply filters
	for _, entity := range result {
		transformed := s.processCallbacks(entity)
		if transformed != nil {
			out = append(out, transformed)
		}
	}
	return
}

// Find Execute query based on the criteria, order and pagination
// On each record, after the marshaling the result shall be transformed via the query callback chain
func (s *postgresDatabaseQuery) Find(keys ...string) (out []Entity, total int64, err error) {

	var rows pgx.Rows

	sqlState, args := s.buildStatement(keys...)

	if rows, err = s.db.poolDb.Query(context.Background(), sqlState, args...); err != nil {
		return
	}

	// Scan row by row and fetch entities
	var entity Entity

	defer rows.Close()

	for rows.Next() {

		jsonDoc := JsonDoc{}
		if err = rows.Scan(&jsonDoc.Id, &jsonDoc.Data); err != nil {
			return
		}

		if entity, err = s.unMarshal(&jsonDoc, nil); err != nil {
			return
		}
		transformed := s.processCallbacks(entity)
		if transformed != nil {
			out = append(out, transformed)
		}
	}
	// Get the rows count
	total, err = s.Count(keys...)
	return
}

// Select is similar to find but with ability to retrieve specific fields
func (s *postgresDatabaseQuery) Select(fields ...string) ([]Json, error) {

	// Build the SQL select
	tblName := tableName(s.factory().TABLE())

	// Build the WHERE clause
	where, args := s.buildCriteria(0)
	order := s.buildOrder()
	limit := s.buildLimit()

	SQL := fmt.Sprintf(`SELECT id FROM "%s" %s %s %s`, tblName, where, order, limit)

	if len(fields) > 0 {
		fieldArr := make([]string, 0)
		for i, field := range fields {
			if strings.Contains(field, " as") {
				fieldArr = append(fieldArr, field)
			} else {
				fieldArr = append(fieldArr, fmt.Sprintf("%s as field%d", field, i))
			}
		}
		selectFields := strings.Join(fieldArr, ",")
		SQL = fmt.Sprintf(`SELECT %s FROM "%s" %s %s %s`, selectFields, tblName, where, order, limit)
	}

	rows, err := s.db.poolDb.Query(context.Background(), SQL, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]Json, 0)
	for {
		if !rows.Next() {
			break
		}

		cols := rows.FieldDescriptions()
		values := make([]any, len(cols))
		for i := range cols {
			values[i] = new(string)
		}

		if err = rows.Scan(values...); err != nil {
			return nil, err
		}

		entry := Json{}
		for i, col := range cols {
			entry[col.Name] = values[i]
		}
		result = append(result, entry)
	}

	return result, nil
}

// Count Execute query based on the criteria, order and pagination
// returns only the count of matching rows
func (s *postgresDatabaseQuery) Count(keys ...string) (total int64, err error) {

	var rows pgx.Rows

	SQL, args := s.buildCountStatement("", "count", keys...)

	if rows, err = s.db.poolDb.Query(context.Background(), SQL, args...); err != nil {
		return
	}

	defer rows.Close()
	if rows.Next() {
		err = rows.Scan(&total)
	}
	return
}

// Aggregation Execute the query based on the criteria, order and pagination and return the provided aggregation function on the field
// supported functions: count ,avg, sum, min, max
func (s *postgresDatabaseQuery) Aggregation(field string, function database.AggFunc, keys ...string) (value float64, err error) {

	if !collections.Include(functions, string(function)) {
		return 0, fmt.Errorf("function %s not supported", function)
	}
	SQL, args := s.buildCountStatement(field, string(function), keys...)

	rows, err := s.db.poolDb.Query(context.Background(), SQL, args...)

	if err != nil {
		return 0, err
	}

	if rows.Next() {
		err = rows.Scan(&value)
	}

	rows.Close()
	return
}

// GroupCount Execute the query based on the criteria, grouped by field and return count per group
func (s *postgresDatabaseQuery) GroupCount(field string, keys ...string) (map[any]int64, int64, error) {

	result := make(map[any]int64)

	// Build the group count statement
	tblName := tableName(s.factory().TABLE(), keys...)
	args := make([]any, 0)
	where, args := s.buildCriteria(0)
	SQL := fmt.Sprintf(`SELECT count(*) cnt , data->>'%s' grp FROM "%s" %s GROUP BY grp`, field, tblName, where)

	rows, err := s.db.poolDb.Query(context.Background(), SQL, args...)

	if err != nil {
		return result, 0, err
	}

	var count, total int64
	var group any

	for rows.Next() {
		if er := rows.Scan(&count, &group); er == nil {
			// If group is a number, add it as a number
			str := fmt.Sprintf("%v", group)
			if gNum, cErr := strconv.Atoi(str); cErr != nil {
				result[group] = count
			} else {
				result[gNum] = count
			}
			total += count
		}
	}

	rows.Close()
	return result, total, nil
}

// GroupAggregation Execute the query based on the criteria, order and pagination and return the aggregated value per group
// the data point is a calculation of the provided function on the selected field, each data point includes the number of documents and the calculated value
// the total is the sum of all calculated values in all the buckets
// supported functions: count : avg, sum, min, max
func (s *postgresDatabaseQuery) GroupAggregation(field string, function database.AggFunc, keys ...string) (map[any]Tuple[int64, float64], float64, error) {

	if !collections.Include(functions, string(function)) {
		return nil, 0, fmt.Errorf("function %s not supported", function)
	}
	result := make(map[any]Tuple[int64, float64])
	total := float64(0)

	// Build the group count statement
	tblName := tableName(s.factory().TABLE(), keys...)
	args := make([]any, 0)
	where, args := s.buildCriteria(0)

	aggr := "*"
	if function != "count" {
		aggr = fmt.Sprintf("(data->>'%s')::FLOAT", field)
	}
	SQL := fmt.Sprintf(`SELECT %s(%s) cnt , data->>'%s' grp FROM "%s" %s GROUP BY grp`, function, aggr, field, tblName, where)

	rows, err := s.db.poolDb.Query(context.Background(), SQL, args...)

	if err != nil {
		return result, total, err
	}

	var count float64
	var group any

	for rows.Next() {
		if er := rows.Scan(&count, &group); er == nil {
			// If group is a number, add it as a number
			str := fmt.Sprintf("%v", group)
			if gNum, cErr := strconv.Atoi(str); cErr != nil {
				result[group] = Tuple[int64, float64]{Key: int64(count), Value: count}
			} else {
				result[gNum] = Tuple[int64, float64]{Key: int64(count), Value: count}
			}
			total += count
		}
	}
	rows.Close()
	return result, total, nil
}

// Histogram returns a time series data points based on the time field, supported intervals: Minute, Hour, Day, week, month
// the data point is a calculation of the provided function on the selected field, each data point includes the number of documents and the calculated value
// the total is the sum of all calculated values in all the buckets
// supported functions: count : avg, sum, min, max
func (s *postgresDatabaseQuery) Histogram(field string, function database.AggFunc, timeField string, interval time.Duration, keys ...string) (map[Timestamp]Tuple[int64, float64], float64, error) {

	if !collections.Include(functions, string(function)) {
		return nil, 0, fmt.Errorf("function %s not supported", function)
	}
	result := make(map[Timestamp]Tuple[int64, float64])

	// Build the group count statement
	tblName := tableName(s.factory().TABLE(), keys...)
	args := make([]any, 0)
	where, args := s.buildCriteria(0)

	// calculate date part
	dp := s.calculateDatePart(interval)

	aggr := "*"
	if function != "count" {
		aggr = fmt.Sprintf("(data->>'%s')::FLOAT", field)
	}

	SQL := fmt.Sprintf(
		`SELECT %s(%s) cnt, 
				date_trunc('%s', to_timestamp((data->>'%s')::bigint / 1000)) dp 
				FROM "%s" %s GROUP BY dp ORDER BY dp`, function, aggr, dp, timeField, tblName, where)

	rows, err := s.db.poolDb.Query(context.Background(), SQL, args...)
	if err != nil {
		return nil, 0, err
	}

	var count, total float64
	var ts Timestamp
	var rTime time.Time

	for rows.Next() {
		if er := rows.Scan(&count, &rTime); er == nil {
			ts = Timestamp(rTime.Unix() * 1000)
			result[ts] = Tuple[int64, float64]{Key: int64(count), Value: count}
			total += count
		}
	}
	rows.Close()
	return result, total, nil
}

// Histogram2D returns a two-dimensional time series data points based on the time field, supported intervals: Minute, Hour, Day, week, month
// the data point is a calculation of the provided function on the selected field
// supported functions: count : avg, sum, min, max
func (s *postgresDatabaseQuery) Histogram2D(field string, function database.AggFunc, dim, timeField string, interval time.Duration, keys ...string) (map[Timestamp]map[any]Tuple[int64, float64], float64, error) {
	if !collections.Include(functions, string(function)) {
		return nil, 0, fmt.Errorf("function %s not supported", function)
	}
	result := make(map[Timestamp]map[any]Tuple[int64, float64])

	// Build the group count statement
	tblName := tableName(s.factory().TABLE(), keys...)
	args := make([]any, 0)
	where, args := s.buildCriteria(0)

	// calculate date part
	dp := s.calculateDatePart(interval)

	aggr := "*"
	if function != "count" {
		aggr = fmt.Sprintf("(data->>'%s')::FLOAT", field)
	}

	SQL := fmt.Sprintf(
		`SELECT %s(%s) cnt, (data->>'%s') dim,
				date_trunc('%s', to_timestamp((data->>'%s')::bigint / 1000)) dp 
				FROM "%s" %s GROUP BY dp, dim ORDER BY dp`, function, aggr, dim, dp, timeField, tblName, where)

	rows, err := s.db.poolDb.Query(context.Background(), SQL, args...)
	if err != nil {
		return nil, 0, err
	}

	var count, total float64
	var dimVal int
	var ts Timestamp
	var rTime time.Time

	for rows.Next() {
		if er := rows.Scan(&count, &dimVal, &rTime); er == nil {
			ts = Timestamp(rTime.Unix() * 1000)

			if _, ok := result[ts]; !ok {
				result[ts] = make(map[any]Tuple[int64, float64])
			}
			result[ts][dimVal] = Tuple[int64, float64]{Key: int64(count), Value: count}
			total += count
		}
	}
	rows.Close()
	return result, total, nil
}

// FindSingle Execute query based on the where criteria to get a single (the first) result
// After the marshaling the result shall be transformed via the query callback chain
func (s *postgresDatabaseQuery) FindSingle(keys ...string) (entity Entity, err error) {

	s.limit = 1
	sqlState, args := s.buildStatement(keys...)
	row := s.db.poolDb.QueryRow(context.Background(), sqlState, args...)

	jsonDoc := JsonDoc{}
	if err = row.Scan(&jsonDoc.Id, &jsonDoc.Data); err != nil {
		return
	}

	if entity, err = s.unMarshal(&jsonDoc, nil); err != nil {
		return
	}
	entity = s.processCallbacks(entity)
	return
}

// GetMap Execute query based on the criteria, order and pagination and return the results as a map of id->Entity
func (s *postgresDatabaseQuery) GetMap(keys ...string) (out map[string]Entity, err error) {

	var rows pgx.Rows

	out = make(map[string]Entity)

	SQL, args := s.buildStatement(keys...)

	if rows, err = s.db.poolDb.Query(context.Background(), SQL, args...); err != nil {
		return
	}

	// Scan row by row and fetch entities
	var entity Entity

	for rows.Next() {
		jsonDoc := JsonDoc{}
		if err := rows.Scan(&jsonDoc.Id, &jsonDoc.Data); err != nil {
			return nil, err
		}
		if entity, err = s.unMarshal(&jsonDoc, nil); err != nil {
			return
		}
		transformed := s.processCallbacks(entity)
		if transformed != nil {
			out[transformed.ID()] = transformed
		}
	}

	rows.Close()
	return
}

// GetIDs Execute query based on the where criteria, order and pagination and return the results as a list of Ids
func (s *postgresDatabaseQuery) GetIDs(keys ...string) (out []string, err error) {

	var rows pgx.Rows

	out = make([]string, 0)

	SQL, args := s.buildIdStatement(keys...)

	if rows, err = s.db.poolDb.Query(context.Background(), SQL, args...); err != nil {
		return
	}

	// Scan row by row and fetch ID
	for rows.Next() {
		id := ""
		if er := rows.Scan(&id); er == nil {
			out = append(out, id)
		}
	}

	rows.Close()
	return
}

// Delete Execute delete command based on the where criteria
func (s *postgresDatabaseQuery) Delete(keys ...string) (total int64, err error) {

	tblName := tableName(s.factory().TABLE(), keys...)
	where, args := s.buildCriteria(0)
	limit := s.buildLimit()

	// Build the SQL
	SQL := fmt.Sprintf(`DELETE FROM "%s" %s %s`, tblName, where, limit)

	if res, ser := s.db.poolDb.Exec(context.Background(), SQL, args...); err != nil {
		return 0, ser
	} else {
		return res.RowsAffected(), nil
	}
}

// SetField Update single field of all the documents meeting the criteria in a single transaction
func (s *postgresDatabaseQuery) SetField(field string, value any, keys ...string) (total int64, err error) {
	fields := make(map[string]any)
	fields[field] = value
	return s.SetFields(fields, keys...)
}

// SetFields Update multiple fields of all the documents meeting the criteria in a single transaction
func (s *postgresDatabaseQuery) SetFields(fields map[string]any, keys ...string) (total int64, err error) {

	allArgs := make([]any, 0)

	entity := s.factory()
	tblName := tableName(entity.TABLE(), keys...)

	parts := make([]string, 0)
	i := 1
	for f, v := range fields {
		part := fmt.Sprintf(`"%s": $%d`, f, i)
		allArgs = append(allArgs, v)
		parts = append(parts, part)
	}

	fieldsList := strings.Join(parts, ",")

	// Build the WHERE clause
	where, args := s.buildCriteria(0)

	allArgs = append(allArgs, args)
	SQL := fmt.Sprintf(`UPDATE "%s" SET data = data || '{%s}' %s`, tblName, fieldsList, where)

	if res, er := s.db.poolDb.Exec(context.Background(), SQL, allArgs...); er != nil {
		return 0, err
	} else {
		return res.RowsAffected(), nil
	}
}

// endregion

// region Query ToString Methods ---------------------------------------------------------------------------------------

// ToString Get the string representation of the query
func (s *postgresDatabaseQuery) ToString() string {
	// Create Json representing the internal builder
	if bytes, err := Marshal(s); err != nil {
		return err.Error()
	} else {
		return string(bytes)
	}
}

// endregion

// region Query Internal Methods ---------------------------------------------------------------------------------------

// Unmarshal database Json document to Entity
func (s *postgresDatabaseQuery) unMarshal(jsonDoc *JsonDoc, errIn error) (Entity, error) {
	if errIn != nil {
		return nil, errIn
	}

	entity := s.factory()
	if err := Unmarshal([]byte(jsonDoc.Data), &entity); err != nil {
		return nil, err
	} else {
		return entity, nil
	}
}

// Transform the entity through the chain of callbacks
func (s *postgresDatabaseQuery) processCallbacks(in Entity) (out Entity) {
	if len(s.callbacks) == 0 {
		out = in
		return
	}

	tmp := in
	for _, cb := range s.callbacks {
		out = cb(tmp)
		if out == nil {
			return nil
		} else {
			tmp = out
		}
	}
	return
}

// Calculate postgres specific date part from time Duration
func (s *postgresDatabaseQuery) calculateDatePart(interval time.Duration) string {

	// calculate date part
	dp := "minute"
	switch interval {
	case time.Minute:
		dp = "minute"
	case time.Hour:
		dp = "hour"
	case time.Hour * 24:
		dp = "day"
	case time.Hour * 24 * 7:
		dp = "week"
	case time.Hour * 24 * 30:
		dp = "month"
	}
	return dp
}

// endregion
