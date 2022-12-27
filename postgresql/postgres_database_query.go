// Copyright 2022. Motty Cohen
//
// Postgresql database implementation of IQuery
//

package postgresql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-yaaf/yaaf-common/database"
	. "github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/logger"
)

// region postgres query internal structure ----------------------------------------------------------------------------

type postgresDatabaseQuery struct {
	db         *PostgresDatabase
	factory    EntityFactory
	allFilters [][]database.QueryFilter
	anyFilters [][]database.QueryFilter
	ascOrders  []string
	descOrders []string
	callbacks  []func(in Entity) Entity
	page       int
	limit      int
}

// endregion

// region Query Construction Methods -----------------------------------------------------------------------------------

/**
 * Add callback to apply on each result entity in the query
 */
func (s *postgresDatabaseQuery) Apply(cb func(in Entity) Entity) database.IQuery {
	if cb != nil {
		s.callbacks = append(s.callbacks, cb)
	}
	return s
}

/**
 * Add single field filter
 */
func (s *postgresDatabaseQuery) Filter(filter database.QueryFilter) database.IQuery {
	if filter.IsActive() {
		s.allFilters = append(s.allFilters, []database.QueryFilter{filter})
	}
	return s
}

/**
 * Add list of filters, all of them should be satisfied (AND)
 */
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

/**
 * Add list of filters, any of them should be satisfied (OR)
 */
func (s *postgresDatabaseQuery) MatchAny(filters ...database.QueryFilter) database.IQuery {
	list := make([]database.QueryFilter, 0)
	for _, filter := range filters {
		if filter.IsActive() == true {
			list = append(list, filter)
		}
	}
	s.anyFilters = append(s.allFilters, list)
	return s
}

/**
 * Add sort order by field,  expects sort parameter in the following form: field_name (Ascending) or field_name- (Descending)
 */
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

/**
 * Set page size limit (for pagination)
 */
func (s *postgresDatabaseQuery) Limit(limit int) database.IQuery {
	s.limit = limit
	return s
}

/**
 * Set requested page number (used for pagination)
 */
func (s *postgresDatabaseQuery) Page(page int) database.IQuery {
	s.page = page
	return s
}

// endregion

// region QueryBuilder Execution Methods -------------------------------------------------------------------------------

/**
 * Execute a query to get list of entities by IDs (the criteria is ignored)
 */
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

/**
 * Execute query based on the criteria, order and pagination
 * On each record, after the marshaling the result shall be transformed via the query callback chain
 */
func (s *postgresDatabaseQuery) Find(keys ...string) (out []Entity, total int64, err error) {

	sql, args := s.buildStatement(keys...)
	logger.Debug(sql)

	statement, e := s.db.pgDb.Prepare(sql)
	if e != nil {
		return nil, 0, e
	}

	defer func() {
		if statement != nil {
			_ = statement.Close()
		}
	}()

	// Execute the query
	rows, fe := statement.Query(args)
	if fe != nil {
		if rows != nil {
			_ = rows.Close()
		}
		return nil, 0, e
	}

	// Scan row by row and fetch entities
	var entity Entity

	for rows.Next() {
		if entity, fe = s.unMarshal(s.scanRow(rows)); fe != nil {
			return
		}
		transformed := s.processCallbacks(entity)
		if transformed != nil {
			out = append(out, transformed)
		}
	}

	_ = rows.Close()

	// Get the rows count
	total, err = s.Count(keys...)
	return
}

/**
 * Execute query based on the criteria, order and pagination
 * Return only the count of matching rows
 */
func (s *postgresDatabaseQuery) Count(keys ...string) (total int64, err error) {

	sql, args := s.buildCountStatement(keys...)
	logger.Debug(sql)

	statement, e := s.db.pgDb.Prepare(sql)
	if e != nil {
		return 0, e
	}

	defer func() {
		if statement != nil {
			_ = statement.Close()
		}
	}()

	// Execute the query
	rows, fe := statement.Query(args)
	if fe != nil {
		if rows != nil {
			_ = rows.Close()
		}
		return 0, e
	}

	if rows.Next() {
		err = rows.Scan(&total)
	}

	_ = rows.Close()
	return
}

/**
 * Execute query based on the where criteria to get a single (the first) result
 * After the marshaling the result shall be transformed via the query callback chain
 */
func (s *postgresDatabaseQuery) FindSingle(keys ...string) (entity Entity, err error) {

	s.limit = 1
	if list, _, fe := s.Find(keys...); fe != nil {
		return nil, fe
	} else {
		if len(list) == 0 {
			return nil, fmt.Errorf("not found")
		} else {
			return list[0], nil
		}
	}
}

/**
 * Execute query based on the criteria, order and pagination and return the results as a map of id->Entity
 */
func (s *postgresDatabaseQuery) GetMap(keys ...string) (out map[string]Entity, err error) {

	out = make(map[string]Entity)

	sql, args := s.buildStatement(keys...)
	logger.Debug(sql)

	statement, e := s.db.pgDb.Prepare(sql)
	if e != nil {
		return nil, e
	}

	defer func() {
		if statement != nil {
			_ = statement.Close()
		}
	}()

	// Execute the query
	rows, fe := statement.Query(args)
	if fe != nil {
		if rows != nil {
			_ = rows.Close()
		}
		return nil, e
	}

	// Scan row by row and fetch entities
	var entity Entity

	for rows.Next() {
		if entity, fe = s.unMarshal(s.scanRow(rows)); fe != nil {
			return
		}
		transformed := s.processCallbacks(entity)
		if transformed != nil {
			out[transformed.ID()] = transformed
		}
	}

	_ = rows.Close()
	return
}

/**
 * Execute query based on the where criteria, order and pagination and return the results as a list of Ids
 */
func (s *postgresDatabaseQuery) GetIds(keys ...string) (out []string, err error) {

	out = make([]string, 0)

	sql, args := s.buildIdStatement(keys...)
	logger.Debug(sql)

	statement, e := s.db.pgDb.Prepare(sql)
	if e != nil {
		return nil, e
	}

	defer func() {
		if statement != nil {
			_ = statement.Close()
		}
	}()

	// Execute the query
	rows, fe := statement.Query(args)
	if fe != nil {
		if rows != nil {
			_ = rows.Close()
		}
		return nil, e
	}

	// Scan row by row and fetch ID
	for rows.Next() {
		id := ""
		if er := rows.Scan(&id); er == nil {
			out = append(out, id)
		}
	}
	_ = rows.Close()
	return
}

/**
 * Execute delete command based on the where criteria
 */
func (s *postgresDatabaseQuery) Delete(keys ...string) (total int64, err error) {

	tblName := tableName(s.factory().TABLE(), keys...)
	where, args := s.buildCriteria()
	limit := s.buildLimit()

	whereClause := fmt.Sprintf("WHERE %s", where)
	if len(where) == 0 {
		whereClause = ""
	}

	// Build the SQL
	SQL := fmt.Sprintf(`DELETE FROM "%s" WHERE %s %s`, tblName, whereClause, limit)
	logger.Debug(SQL)

	if res, ser := s.db.pgDb.Exec(SQL, args...); err != nil {
		return 0, ser
	} else {
		if rows, er := res.RowsAffected(); er != nil {
			return 0, er
		} else {
			return rows, nil
		}
	}
}

/**
 * Update single field of all the documents meeting the criteria in a single transaction
 */
func (s *postgresDatabaseQuery) SetField(field string, value any, keys ...string) (total int64, err error) {
	fields := make(map[string]any)
	fields[field] = value
	return s.SetFields(fields, keys...)
}

/**
 * Update multiple fields of all the documents meeting the criteria in a single transaction
 */
func (s *postgresDatabaseQuery) SetFields(fields map[string]any, keys ...string) (total int64, err error) {

	//args, where := s.buildCriteria()

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
	where, args := s.buildCriteria()
	whereClause := fmt.Sprintf("WHERE %s", where)
	if len(where) == 0 {
		whereClause = ""
	}

	allArgs = append(allArgs, args)
	SQL := fmt.Sprintf(`UPDATE "%s" SET data = data || '{%s}' %s`, tblName, fieldsList, whereClause)
	logger.Debug(SQL)

	if res, er := s.db.pgDb.Exec(SQL, allArgs...); er != nil {
		return 0, err
	} else {
		if rows, ser := res.RowsAffected(); ser != nil {
			return 0, ser
		} else {
			return rows, nil
		}
	}
}

// endregion

// region Query ToString Methods ---------------------------------------------------------------------------------------
/**
 * Get the string representation of the query
 */
func (s *postgresDatabaseQuery) ToString() string {
	// Create Json representing the internal builder
	if bytes, err := json.Marshal(s); err != nil {
		return err.Error()
	} else {
		return string(bytes)
	}
}

// endregion

// region Query Internal Methods ---------------------------------------------------------------------------------------

/**
 * Scan single database row into Json document
 */
func (s *postgresDatabaseQuery) scanRow(rows *sql.Rows) (*JsonDoc, error) {

	jsonDoc := JsonDoc{}
	if err := rows.Scan(&jsonDoc.Id, &jsonDoc.Data); err != nil {
		return nil, err
	} else {
		return &jsonDoc, nil
	}
}

/**
 * Unmarshal database Json document to Entity
 */
func (s *postgresDatabaseQuery) unMarshal(jsonDoc *JsonDoc, errIn error) (Entity, error) {
	if errIn != nil {
		return nil, errIn
	}

	entity := s.factory()
	if err := json.Unmarshal([]byte(jsonDoc.Data), &entity); err != nil {
		return nil, err
	} else {
		return entity, nil
	}
}

/**
 * Transform the entity through the chain of callbacks
 */
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

// endregion
