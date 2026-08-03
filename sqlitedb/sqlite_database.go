// SQLiteDatabase object database implementations of IDatabase interface
//

package sqlitedb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/go-yaaf/yaaf-common/database"
	. "github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/logger"
	"github.com/go-yaaf/yaaf-common/messaging"
)

// region Database store definitions -----------------------------------------------------------------------------------

type SQLiteDatabase struct {
	bus messaging.IMessageBus
	uri string
	db  *sql.DB
}

const (
	sqlInsert      = `INSERT INTO "%s" (id, data) VALUES (?1, ?2)`
	sqlUpdate      = `UPDATE "%s" SET data = ?2 WHERE id = ?1`
	sqlUpsert      = `INSERT INTO "%s" (id, data) VALUES (?1, ?2) ON CONFLICT (id) DO UPDATE SET data = excluded.data`
	sqlDelete      = `DELETE FROM "%s" WHERE id = ?1`
	sqlBulkDelete  = `DELETE FROM "%s" WHERE id IN (SELECT value FROM json_each(?1))`
	ddlDropTable   = `DROP TABLE IF EXISTS "%s"`
	ddlCreateTable = `CREATE TABLE IF NOT EXISTS "%s" (id TEXT PRIMARY KEY NOT NULL, data TEXT NOT NULL default '{}')`
	ddlCreateIndex = `CREATE INDEX IF NOT EXISTS %s_%s_idx ON "%s" ((data->>'%s'))`
	ddlPurgeTable  = `DELETE FROM "%s"`
)

// endregion

// region Factory method for Database store ----------------------------------------------------------------------------

// NewSQLiteStore factory method for datastore
// param: URI - represents the database connection string in the format of: sqlitedb://user:password@host:port/database_name?application_name
// return: IDatabase instance, error
func NewSQLiteStore(URI string) (dbs database.IDatastore, err error) {
	return createSQLiteDatabase(URI)
}

// NewSQLiteDatabase factory method for database
// param: URI - represents the database connection string in the format of: sqlitedb://user:password@host:port/database_name?application_name
// return: IDatabase instance, error
func NewSQLiteDatabase(URI string) (dbs database.IDatabase, err error) {
	return createSQLiteDatabase(URI)
}

// NewSQLiteDatabaseWithMessageBus factory method for database with injected message bus
// param: URI - represents the database connection string in the format of: sqlitedb://user:password@host:port/database_name?application_name
// return: IDatabase instance, error
func NewSQLiteDatabaseWithMessageBus(URI string, bus messaging.IMessageBus) (dbs database.IDatabase, err error) {
	var db *SQLiteDatabase
	if db, err = createSQLiteDatabase(URI); err != nil {
		return
	}
	db.bus = bus
	return db, nil
}

func createSQLiteDatabase(dbUri string) (*SQLiteDatabase, error) {

	if db, err := sql.Open("sqlite", dbUri); err != nil {
		return nil, err
	} else {
		return &SQLiteDatabase{db: db, uri: dbUri}, nil
	}
}

// Ping Test database connectivity
//
// param: retries - how many retries are required (max 10)
// param: intervalInSeconds - time interval (in seconds) between retries (max 60)
func (dbs *SQLiteDatabase) Ping(retries uint, intervalInSeconds uint) error {

	if retries > 10 {
		retries = 10
	}

	if intervalInSeconds > 60 {
		intervalInSeconds = 60
	}

	for try := 1; try <= int(retries); try++ {
		err := dbs.db.Ping()
		if err == nil {
			return nil
		}

		// In case of failure, sleep and try again after 10 seconds
		logger.Debug("ping to database failed try %d of 5", try)

		// time.Second
		duration := time.Second * time.Duration(intervalInSeconds)
		time.Sleep(duration)
	}
	return fmt.Errorf("could not establish database connection")
}

// Close DB and free resources
func (dbs *SQLiteDatabase) Close() error {
	return dbs.db.Close()
}

// CloneDatabase Returns a clone (copy) of the database instance
func (dbs *SQLiteDatabase) CloneDatabase() (database.IDatabase, error) {
	return NewSQLiteDatabaseWithMessageBus(dbs.uri, dbs.bus)
}

// CloneDatastore Returns a clone (copy) of the database instance
func (dbs *SQLiteDatabase) CloneDatastore() (database.IDatastore, error) {
	return NewSQLiteStore(dbs.uri)
}

// Resolve table name from entity class name and shard keys
func tableName(table string, keys ...string) (tblName string) {

	tblName = table

	// If the {key} exists, replace it with keys
	if strings.Contains(tblName, "{key}") {
		if len(keys) == 0 {
			idx := strings.Index(tblName, "{key}")
			prefix := tblName[:idx-1]
			if strings.HasSuffix(prefix, "_") {
				return prefix[:len(prefix)-len("_")]
			}
			if strings.HasSuffix(prefix, "-") {
				return prefix[:len(prefix)-len("-")]
			}
			return prefix
		} else {
			return strings.ReplaceAll(table, "{key}", keys[0])
		}
	}

	// for any other use case, remove placeholders
	if strings.Contains(tblName, "-{") {
		idx := strings.Index(tblName, "-{")
		return tblName[:idx]
	}

	// If keys are not provided, remove placeholders
	if len(keys) == 0 {
		if strings.Contains(tblName, "{") {
			idx := strings.Index(tblName, "{")
			return tblName[:idx-1]
		} else {
			return tblName
		}
	}
	return
}

//endregion

//endregion

// region Database basic CRUD methods ----------------------------------------------------------------------------------

// Get a single entity by ID
//
// param: factory - Entity factory
// param: entityID - Entity id
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: Entity, error
func (dbs *SQLiteDatabase) Get(factory EntityFactory, entityID string, keys ...string) (result Entity, err error) {

	var (
		rows *sql.Rows
	)

	result = factory()

	defer func() {
		if err != nil {
			if result != nil {
				result = nil
			}
		}
	}()

	if entityID == "" {
		return nil, fmt.Errorf("empty entity id passed to Get operation")
	}

	SQL := fmt.Sprintf(`SELECT id, data FROM "%s" WHERE id = ?1`, tableName(result.TABLE(), keys...))

	if rows, err = dbs.db.Query(SQL, entityID); err != nil {
		return nil, err
	}

	// Connection is released to pool only after rows is closed.
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		return nil, fmt.Errorf("no row fetched for id: %s", entityID)
	}

	jsonDoc := JsonDoc{}
	if err = rows.Scan(&jsonDoc.Id, &jsonDoc.Data); err != nil {
		return nil, err
	}

	if err = Unmarshal([]byte(jsonDoc.Data), &result); err != nil {
		return nil, err
	}

	return
}

// Exists Check if entity exists by ID
//
// param: factory - Entity factory
// param: entityID - Entity id
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: bool, error
func (dbs *SQLiteDatabase) Exists(factory EntityFactory, entityID string, keys ...string) (result bool, err error) {

	var rows *sql.Rows

	SQL := fmt.Sprintf(`SELECT id FROM "%s" WHERE id = ?1`, tableName(factory().TABLE(), keys...))

	if rows, err = dbs.db.Query(SQL, entityID); err != nil {
		return false, err
	}
	result = rows.Next()
	_ = rows.Close()
	return result, nil
}

// List Get list of entities by IDs
//
// param: factory - Entity factory
// param: entityIDs - List of Entity IDs
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: []Entity, error
func (dbs *SQLiteDatabase) List(factory EntityFactory, entityIDs []string, keys ...string) (list []Entity, err error) {

	var (
		rows *sql.Rows
	)

	list = make([]Entity, 0)

	// For empty list of ids, return empty list
	if len(entityIDs) == 0 {
		return list, nil
	}

	table := tableName(factory().TABLE(), keys...)
	SQL := fmt.Sprintf(`SELECT id, data FROM "%s" WHERE id IN (SELECT value FROM json_each(?1))`, table)
	jsonIDs, err := json.Marshal(entityIDs)
	if err != nil {
		return nil, err
	}
	if rows, err = dbs.db.Query(SQL, string(jsonIDs)); err != nil {
		return
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		jsonDoc := JsonDoc{}
		if err = rows.Scan(&jsonDoc.Id, &jsonDoc.Data); err != nil {
			return
		}
		entity := factory()
		if err = Unmarshal([]byte(jsonDoc.Data), &entity); err == nil {
			list = append(list, entity)
		}
	}
	return
}

// Insert new entity
//
// param: entity - The entity to insert
// return: Inserted Entity, error
func (dbs *SQLiteDatabase) Insert(entity Entity) (Entity, error) {
	var (
		result sql.Result
	)

	tblName := tableName(entity.TABLE(), entity.KEY())

	SQL := fmt.Sprintf(sqlInsert, tblName)
	data, err := Marshal(entity)
	if err != nil {
		return nil, err
	}

	result, err = dbs.db.Exec(SQL, entity.ID(), data)
	if err != nil {
		return nil, err
	}

	num, err := result.RowsAffected()
	if err != nil {
		return nil, err
	} else if num == 0 {
		return nil, fmt.Errorf("no row affected by insert operation")
	}

	// Publish the change
	dbs.publishChange(AddEntity, entity)
	return entity, nil
}

// Update existing entity
//
// param: entity - The entity to update
// return: Updated Entity, error
func (dbs *SQLiteDatabase) Update(entity Entity) (Entity, error) {
	var (
		result sql.Result
	)

	tblName := tableName(entity.TABLE(), entity.KEY())

	SQL := fmt.Sprintf(sqlUpdate, tblName)
	data, err := Marshal(entity)
	if err != nil {
		return nil, err
	}

	result, err = dbs.db.Exec(SQL, entity.ID(), data)
	if err != nil {
		return nil, err
	}

	num, err := result.RowsAffected()
	if err != nil {
		return nil, err
	} else if num == 0 {
		return nil, fmt.Errorf("no row affected by update operation")
	}

	// Publish the change
	dbs.publishChange(UpdateEntity, entity)
	return entity, nil
}

// Upsert Update entity or insert it if it does not exist
//
// param: entity - The entity to update
// return: Updated Entity, error
func (dbs *SQLiteDatabase) Upsert(entity Entity) (Entity, error) {
	var (
		result sql.Result
	)

	tblName := tableName(entity.TABLE(), entity.KEY())

	SQL := fmt.Sprintf(sqlUpsert, tblName)
	data, err := Marshal(entity)
	if err != nil {
		return nil, err
	}

	result, err = dbs.db.Exec(SQL, entity.ID(), data)
	if err != nil {
		return nil, err
	}

	num, err := result.RowsAffected()
	if err != nil {
		return nil, err
	} else if num == 0 {
		return nil, fmt.Errorf("no row affected by update operation")
	}

	// Publish the change
	dbs.publishChange(UpdateEntity, entity)
	return entity, nil
}

// Delete entity
//
// param: factory - Entity factory
// param: entityID - Entity ID to delete
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: error
func (dbs *SQLiteDatabase) Delete(factory EntityFactory, entityID string, keys ...string) (err error) {

	entity := factory()

	// Get entity
	deleted, er := dbs.Get(factory, entityID, keys...)
	if er != nil {
		return er
	}

	tblName := tableName(entity.TABLE(), keys...)
	SQL := fmt.Sprintf(sqlDelete, tblName)
	if _, err = dbs.db.Exec(SQL, entityID); err != nil {
		return
	}
	// Publish the change
	dbs.publishChange(DeleteEntity, deleted)
	return
}

//endregion

// region Database bulk CRUD methods -----------------------------------------------------------------------------------

// BulkInsert Insert multiple entities to database in a single transaction (all must be of the same type)
//
// param: entities - List of entities to insert
// return: Number of inserted entities, error
func (dbs *SQLiteDatabase) BulkInsert(entities []Entity) (affected int64, err error) {

	if len(entities) == 0 {
		return 0, nil
	}

	// Get the table
	table := tableName(entities[0].TABLE(), entities[0].KEY())
	valueStrings := make([]string, 0, len(entities))
	valueArgs := make([]any, 0, len(entities)*2)
	i := 0
	for _, entity := range entities {
		valueStrings = append(valueStrings, fmt.Sprintf("(?%d, ?%d)", i*2+1, i*2+2))
		valueArgs = append(valueArgs, entity.ID())
		bytes, _ := Marshal(entity)
		valueArgs = append(valueArgs, string(bytes))
		i++
	}
	SQL := fmt.Sprintf(`INSERT INTO "%s" (id, data) VALUES %s`, table, strings.Join(valueStrings, ","))

	if _, err = dbs.db.Exec(SQL, valueArgs...); err != nil {
		return
	}

	// Publish the change
	for _, entity := range entities {
		dbs.publishChange(AddEntity, entity)
	}
	return
}

// BulkUpdate Update multiple entities to database in a single transaction (all must be of the same type)
//
// param: entities - List of entities to update
// return: Number of updated entities, error
func (dbs *SQLiteDatabase) BulkUpdate(entities []Entity) (affected int64, err error) {

	if len(entities) == 0 {
		return 0, nil
	}

	var tx *sql.Tx

	// Start transaction
	if tx, err = dbs.db.Begin(); err != nil {
		return
	}

	// Loop over entities and update each entity within the transaction scope
	for _, entity := range entities {
		table := tableName(entity.TABLE(), entity.KEY())
		SQL := fmt.Sprintf(sqlUpdate, table)
		data, _ := Marshal(entity)
		if _, err = dbs.db.Exec(SQL, entity.ID(), data); err != nil {
			return 0, tx.Rollback()
		}
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return
	} else {
		affected = int64(len(entities))
	}

	// Publish the changes
	for _, entity := range entities {
		dbs.publishChange(UpdateEntity, entity)
	}
	return
}

// BulkUpsert Upsert multiple entities to database in a single transaction (all must be of the same type)
//
// param: entities - List of entities to upsert
// return: Number of updated entities, error
func (dbs *SQLiteDatabase) BulkUpsert(entities []Entity) (affected int64, err error) {

	if len(entities) == 0 {
		return 0, nil
	}

	var tx *sql.Tx

	// Start transaction
	if tx, err = dbs.db.Begin(); err != nil {
		return
	}

	// Loop over entities and update each entity within the transaction scope
	for _, entity := range entities {
		table := tableName(entity.TABLE(), entity.KEY())
		SQL := fmt.Sprintf(sqlUpsert, table)
		data, _ := Marshal(entity)
		if _, err = dbs.db.Exec(SQL, entity.ID(), data); err != nil {
			return 0, tx.Rollback()
		}
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return
	} else {
		affected = int64(len(entities))
	}

	// Publish the changes
	for _, entity := range entities {
		dbs.publishChange(UpdateEntity, entity)
	}
	return
}

// BulkDelete Delete multiple entities from the database in a single transaction (all must be of the same type)
//
// param: factory - Entity factory
// param: entityIDs - List of entities IDs to delete
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: Number of deleted entities, error
func (dbs *SQLiteDatabase) BulkDelete(factory EntityFactory, entityIDs []string, keys ...string) (affected int64, err error) {
	var (
		result sql.Result
		entity = factory()
	)

	if len(entityIDs) == 0 {
		return 0, nil
	}

	tblName := tableName(entity.TABLE(), keys...)

	// Get the list of deleted entities (for notification)
	deleted, e := dbs.List(factory, entityIDs, keys...)
	if e != nil {
		return 0, e
	}

	SQL := fmt.Sprintf(sqlBulkDelete, tblName)

	// Convert array of entityIDs to its json string
	jsonBytes, err := json.Marshal(entityIDs)
	if err != nil {
		return 0, err
	}
	jsonString := string(jsonBytes)

	if result, err = dbs.db.Exec(SQL, jsonString); err != nil {
		return
	}

	if affected, err = result.RowsAffected(); err != nil {
		return
	} else if affected == 0 {
		return 0, fmt.Errorf("no row affected when executing delete operation")
	}

	// Publish the change to the cache
	for _, ent := range deleted {
		dbs.publishChange(DeleteEntity, ent)
	}
	return
}

//endregion

// region Database set field methods -----------------------------------------------------------------------------------

// SetField Update a single field of the document in a single transaction
//
// param: factory - Entity factory
// param: entityID - The entity ID to update the field
// param: field - The field name to update
// param: value - The field value to update
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: error
func (dbs *SQLiteDatabase) SetField(factory EntityFactory, entityID string, field string, value any, keys ...string) (err error) {

	entity := factory()
	tblName := tableName(entity.TABLE(), keys...)

	// Field names cannot be bind parameters, so validate as a safe identifier.
	if !isSafeFieldName(field) {
		return fmt.Errorf("invalid field name: %q", field)
	}

	// JSON-encode the value so it becomes a valid, escaped jsonb literal, then
	// pass it (and the entity id) as bind parameters - never interpolate them.
	jsonVal, jErr := json.Marshal(value)
	if jErr != nil {
		return fmt.Errorf("failed to encode field value: %w", jErr)
	}

	SQL := fmt.Sprintf(`UPDATE "%s" SET data = json_set(data, '$.%s', json(?1)) WHERE id = ?2`, tblName, field)

	if _, err = dbs.db.Exec(SQL, string(jsonVal), entityID); err != nil {
		return
	}

	// Get the updated entity and publish the change
	if updated, fer := dbs.Get(factory, entityID, keys...); fer == nil {
		dbs.publishChange(UpdateEntity, updated)
	}
	return
}

// SetFields Update some fields of the document in a single transaction
//
// param: factory - Entity factory
// param: entityID - The entity ID to update the field
// param: fields - A map of field-value pairs to update
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: error
func (dbs *SQLiteDatabase) SetFields(factory EntityFactory, entityID string, fields map[string]any, keys ...string) (err error) {

	for f, v := range fields {
		if er := dbs.SetField(factory, entityID, f, v, keys...); er != nil {
			return er
		}
	}
	return nil
}

// BulkSetFields Update specific field of multiple entities in a single transaction (eliminates the need to fetch - change - update)
//
// param: factory - Entity factory
// param: field - The field name to update
// param: values - The map of entity Id to field value
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: Number of updated entities, error
func (dbs *SQLiteDatabase) BulkSetFields(factory EntityFactory, field string, values map[string]any, keys ...string) (affected int64, error error) {

	if len(values) == 0 {
		return 0, nil
	}

	// Field name is interpolated into the UPDATE - validate as a safe identifier.
	if !isSafeFieldName(field) {
		return 0, fmt.Errorf("invalid field name: %q", field)
	}

	// Determine the type of the field
	sqlType := dbs.getSqlType(values)

	// Create temp table to map entity to field id
	tmpTable := fmt.Sprintf("ch%d", time.Now().UnixMilli())
	createTmp := fmt.Sprintf("CREATE TEMP TABLE %s (id TEXT PRIMARY KEY NOT NULL, val %s)", tmpTable, sqlType)
	if _, err := dbs.db.Exec(createTmp); err != nil {
		return 0, err
	}

	// Bulk Insert values
	valueStrings := make([]string, 0, len(values))
	valueArgs := make([]any, 0, len(values)*2)
	i := 0
	for id, val := range values {
		valueStrings = append(valueStrings, fmt.Sprintf("(?%d, ?%d)", i*2+1, i*2+2))
		valueArgs = append(valueArgs, id)
		valueArgs = append(valueArgs, val)
		i++
	}
	SQL := fmt.Sprintf(`INSERT INTO "%s" (id, val) VALUES %s`, tmpTable, strings.Join(valueStrings, ","))

	if _, err := dbs.db.Exec(SQL, valueArgs...); err != nil {
		return 0, err
	}

	// Create bulk update statement
	entity := factory()
	tblName := tableName(entity.TABLE(), keys...)

	SQL = fmt.Sprintf(`UPDATE "%s" SET data = json_set(data, '$.%s', %s.val) FROM %s WHERE %s.id = "%s".id`, tblName, field, tmpTable, tmpTable, tmpTable, tblName)

	// Drop the temp table
	defer func() {
		DROP := fmt.Sprintf("DROP TABLE %s", tmpTable)
		_, _ = dbs.db.Exec(DROP)
	}()

	// Execute update
	if result, err := dbs.db.Exec(SQL); err != nil {
		return 0, err
	} else {
		return result.RowsAffected()
	}
}

// Get the SQL type of the value
func (dbs *SQLiteDatabase) getSqlType(values map[string]any) string {

	typeName := "string"
	for _, v := range values {
		typeName = fmt.Sprintf("%T", v)
		break
	}
	if strings.HasPrefix(typeName, "string") {
		return "TEXT"
	}
	if strings.HasPrefix(typeName, "float") {
		return "REAL"
	}
	if strings.HasPrefix(typeName, "bool") {
		return "BOOLEAN"
	}

	// For all other types (numbers, timestamp, enums) return INTEGER
	return "INTEGER"
}

//endregion

// region Database Query methods ---------------------------------------------------------------------------------------

// Query Helper method to construct query
//
// param: factory - Entity factory
// return: Query object
func (dbs *SQLiteDatabase) Query(factory EntityFactory) database.IQuery {
	return &sqliteDatabaseQuery{
		db:              dbs,
		factory:         factory,
		filedNameToType: entityFieldsToTypesMap(factory),
	}
}

func (dbs *SQLiteDatabase) AdvancedQuery(EntityFactory) database.IAdvancedQuery {
	panic("SQLiteDatabase: IAdvancedQuery interface is not implemented/supported ")
}

//endregion

// region Database DDL methods -----------------------------------------------------------------------------------------

// ExecuteDDL create table and indexes
//
// param: ddl - The ddl parameter is a map of strings (table names) to array of strings (list of fields to index)
// return: error
func (dbs *SQLiteDatabase) ExecuteDDL(ddl map[string][]string) (err error) {

	for table, fields := range ddl {
		SQL := fmt.Sprintf(ddlCreateTable, table)
		if _, err = dbs.db.Exec(SQL); err != nil {
			logger.Error("%s error: %s", SQL, err.Error())
			return
		}
		for _, field := range fields {
			SQL = fmt.Sprintf(ddlCreateIndex, table, field, table, field)
			if _, err = dbs.db.Exec(SQL); err != nil {
				logger.Error("%s error: %s", SQL, err.Error())
				return
			}
		}
	}
	return nil
}

// ExecuteSQL Execute SQL command
//
// param: sql - The SQL command to execute
// param: args - Statement arguments
// return: Number of affected records, error
func (dbs *SQLiteDatabase) ExecuteSQL(sql string, args ...any) (int64, error) {

	if result, err := dbs.db.Exec(sql, args...); err != nil {
		logger.Error("%s error: %s", sql, err.Error())
		return 0, err
	} else {
		return result.RowsAffected()
	}
}

// ExecuteQuery Execute native SQL query
func (dbs *SQLiteDatabase) ExecuteQuery(source string, sql string, args ...any) ([]Json, error) {

	rows, err := dbs.db.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	result := make([]Json, 0)
	for {
		if !rows.Next() {
			break
		}

		cols, er := rows.ColumnTypes()
		if er != nil {
			continue
		}

		columnsData := make([]any, len(cols))
		columnPointers := make([]any, len(cols))
		for i := range columnsData {
			columnPointers[i] = &columnsData[i]
		}

		if err = rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		entry := Json{}
		for i, col := range cols {
			val := columnPointers[i].(*any)
			entry[col.Name()] = *val
		}
		result = append(result, entry)
	}

	return result, nil
}

// DropTable Drop table and indexes
//
// param: table - Table name to drop
// return: error
func (dbs *SQLiteDatabase) DropTable(table string) (err error) {
	SQL := fmt.Sprintf(ddlDropTable, table)
	if _, err = dbs.db.Exec(SQL); err != nil {
		logger.Error("%s error: %s", SQL, err.Error())
	}
	return
}

// PurgeTable Fast delete table content (truncate)
//
// param: table - Table name to purge
// return: error
func (dbs *SQLiteDatabase) PurgeTable(table string) (err error) {
	SQL := fmt.Sprintf(ddlPurgeTable, table)
	if _, err = dbs.db.Exec(SQL); err != nil {
		logger.Error("%s error: %s", SQL, err.Error())
	}
	return
}

//endregion

// region PRIVATE SECTION ----------------------------------------------------------------------------------------------

// publishChange Publish entity change to the message bus:
//
//	Topic: 		ENTITY_<accountId> or ENTITY_system
//	Payload:		The entity
//	OpCode:		1=Add, 2=Update, 3=Delete
//	Addressee:		The entity table name
//	SessionId:		The shard key
//
// param: action - The action on the entity
// param: entity - The changed entity
func (dbs *SQLiteDatabase) publishChange(action EntityAction, entity Entity) {

	if dbs.bus == nil || entity == nil {
		return
	}

	// Set topic in the format of: ENTITY-{Table}-{Key}
	topic := fmt.Sprintf("%s-%s-%s", messaging.EntityMessageTopic, entity.TABLE(), entity.KEY())
	addressee := reflect.TypeOf(entity).String()
	idx := strings.LastIndex(addressee, ".")
	addressee = addressee[idx+1:]

	if dbs.bus != nil {
		msg := messaging.EntityMessage{
			BaseMessage: messaging.BaseMessage{
				MsgTopic:     topic,
				MsgOpCode:    int(action),
				MsgAddressee: addressee,
				MsgSessionId: entity.ID(),
			},
			MsgPayload: entity,
		}
		if err := dbs.bus.Publish(&msg); err != nil {
			logger.Warn("error publishing change: %s", err.Error())
		}
	}
}

// endregion

// region Datastore  methods -------------------------------------------------------------------------------------------

// IndexExists tests if index exists
func (dbs *SQLiteDatabase) IndexExists(indexName string) (exists bool) {
	// TODO: Add implementation
	return false
}

// CreateIndex creates an index (without mapping)
func (dbs *SQLiteDatabase) CreateIndex(indexName string) (name string, err error) {
	// TODO: Add implementation
	return indexName, fmt.Errorf("not implemented")
}

// CreateEntityIndex creates an index of entity and add entity field mapping
func (dbs *SQLiteDatabase) CreateEntityIndex(factory EntityFactory, key string) (name string, err error) {
	// TODO: Add implementation
	return key, fmt.Errorf("not implemented")
}

// ListIndices returns a list of all indices matching the pattern
func (dbs *SQLiteDatabase) ListIndices(pattern string) (map[string]int, error) {
	// TODO: Add implementation
	return nil, fmt.Errorf("not implemented")
}

// DropIndex drops an index
func (dbs *SQLiteDatabase) DropIndex(indexName string) (ack bool, err error) {
	// TODO: Add implementation
	return false, fmt.Errorf("not implemented")
}

// endregion
