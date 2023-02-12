// Postgresql object database implementations of IDatabase interface
//

package postgresql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/go-yaaf/yaaf-common/database"
	. "github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/logger"
)

// region Database store definitions -----------------------------------------------------------------------------------

type PostgresDatabase struct {
	pgDb *sql.DB
}

const (
	sqlInsert             = `INSERT INTO "%s" (id, data) VALUES ($1, $2)`
	sqlUpdate             = `UPDATE "%s" SET data = $2 WHERE id = $1`
	sqlUpsert             = `INSERT INTO "%s" (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = $2`
	sqlDelete             = `DELETE FROM "%s" WHERE id = $1`
	sqlBulkDelete         = `DELETE FROM "%s" WHERE id = ANY($1)`
	sqlUpdateField        = `UPDATE "%s" SET data = jsonb_set(data, '{%s}', '"%s"', true) WHERE id = '%s'`
	sqlUpdateNumericField = `UPDATE "%s" SET data = jsonb_set(data, '{%s}', '%d', true) WHERE id = '%s'`
	ddlDropTable          = `DROP TABLE IF EXISTS "%s" CASCADE`
	ddlCreateTable        = `CREATE TABLE IF NOT EXISTS "%s" (id character varying PRIMARY KEY NOT NULL, data jsonb NOT NULL default '{}')`
	ddlCreateIndex        = `CREATE INDEX IF NOT EXISTS %s_%s_idx ON "%s" USING BTREE ((data->>'%s'))`
	ddlPurgeTable         = `TRUNCATE "%s" RESTART IDENTITY CASCADE`
)

// endregion

// region Factory method for Database store ----------------------------------------------------------------------------

// NewPostgresStore factory method for datastore
//
// param: URI - represents the database connection string in the format of: postgresql://user:password@host:port/database_name?application_name
// return: IDatabase instance, error
func NewPostgresStore(URI string) (dbs database.IDatastore, err error) {

	var (
		driver, connStr string
		db              *sql.DB
	)

	// Ensure driver name
	if driver, connStr, err = convertConnectionString(URI); err != nil {
		return nil, err
	}

	// Open connection
	if db, err = sql.Open(driver, connStr); err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	} else {
		dbs = &PostgresDatabase{pgDb: db}
	}
	return
}

// NewPostgresDatabase factory method for database
//
// param: URI - represents the database connection string in the format of: postgresql://user:password@host:port/database_name?application_name
// return: IDatabase instance, error
func NewPostgresDatabase(URI string) (dbs database.IDatabase, err error) {

	var (
		driver, connStr string
		db              *sql.DB
	)

	// Ensure driver name
	if driver, connStr, err = convertConnectionString(URI); err != nil {
		return nil, err
	}

	// Open connection
	if db, err = sql.Open(driver, connStr); err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	} else {
		dbs = &PostgresDatabase{pgDb: db}
	}

	return
}

// convertConnectionString Convert URI style connection to DB connection string in the format of:
// postgres://user:password@host:port/database_name
// param: dbUri - The database URI
// return: driver name, connection string, error
func convertConnectionString(dbUri string) (driver string, connStr string, err error) {
	uri, err := url.Parse(strings.TrimSpace(dbUri))
	if err != nil {
		return "", "", fmt.Errorf("URI: %s parsing failed: %s", dbUri, err.Error())
	}

	usr := uri.User.Username()
	pwd, _ := uri.User.Password()

	driver = strings.ToLower(uri.Scheme)
	if driver == "postgresql" {
		driver = "postgres"
	}

	if driver != "postgres" {
		return "", "", fmt.Errorf("schema for postgresql database must be: postgres")
	}

	dbName := strings.TrimPrefix(uri.Path, "/") // Remove slash
	host, port, err := net.SplitHostPort(uri.Host)
	if err != nil {
		return "", "", fmt.Errorf("URI: %s host:port parsing failed: %s", dbUri, err.Error())
	}

	// Get the app name
	appName := ""
	params := uri.Query()
	if _, ok := params["application_name"]; ok {
		appName = params["application_name"][0]
	} else if _, ok := params["ApplicationName"]; ok {
		appName = params["ApplicationName"][0]
	}

	connStr = fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=%s application_name=%s",
		host, port, dbName, usr, pwd, "disable", appName)

	return driver, connStr, nil
}

// Ping Test database connectivity
//
// param: retries - how many retries are required (max 10)
// param: intervalInSeconds - time interval (in seconds) between retries (max 60)
func (dbs *PostgresDatabase) Ping(retries uint, intervalInSeconds uint) error {

	if retries > 10 {
		retries = 10
	}

	if intervalInSeconds > 60 {
		intervalInSeconds = 60
	}

	for try := 1; try <= int(retries); try++ {
		err := dbs.pgDb.Ping()
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
func (dbs *PostgresDatabase) Close() error {
	return dbs.pgDb.Close()
}

// Resolve table name from entity class name and shard keys
func tableName(table string, keys ...string) (tblName string) {

	tblName = table

	if len(keys) == 0 {
		return tblName
	}

	// replace accountId placeholder with the first key
	tblName = strings.Replace(tblName, "{{accountId}}", "{{0}}", -1)

	for idx, key := range keys {
		placeHolder := fmt.Sprintf("{{%d}}", idx)
		tblName = strings.Replace(tblName, placeHolder, key, -1)
	}

	// Replace templates: {{year}}
	tblName = strings.Replace(tblName, "{{year}}", time.Now().Format("2006"), -1)

	// Replace templates: {{month}}
	tblName = strings.Replace(tblName, "{{month}}", time.Now().Format("01"), -1)

	// TODO: Replace templates: {{week}}

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
func (dbs *PostgresDatabase) Get(factory EntityFactory, entityID string, keys ...string) (result Entity, err error) {

	var (
		rows *sql.Rows
		fe   error
	)

	result = factory()

	defer func() {
		if fe != nil {
			if result != nil {
				result = nil
			}
		}
	}()

	if entityID == "" {
		return nil, fmt.Errorf("empty entity id passed to Get operation")
	}

	SQL := fmt.Sprintf(`SELECT id, data FROM "%s" WHERE id = $1`, tableName(result.TABLE(), keys...))
	logger.Debug(SQL)

	if rows, err = dbs.pgDb.Query(SQL, entityID); err != nil {
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

	if err = json.Unmarshal([]byte(jsonDoc.Data), &result); err != nil {
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
func (dbs *PostgresDatabase) Exists(factory EntityFactory, entityID string, keys ...string) (result bool, err error) {

	SQL := fmt.Sprintf(`SELECT id FROM "%s" WHERE id = $1`, tableName(factory().TABLE(), keys...))
	logger.Debug(SQL)

	if rows, err := dbs.pgDb.Query(SQL, entityID); err != nil {
		return false, err
	} else {
		result = rows.Next()
		_ = rows.Close()
		return result, nil
	}
}

// List Get list of entities by IDs
//
// param: factory - Entity factory
// param: entityIDs - List of Entity IDs
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: []Entity, error
func (dbs *PostgresDatabase) List(factory EntityFactory, entityIDs []string, keys ...string) (list []Entity, err error) {

	var (
		rows *sql.Rows
	)

	list = make([]Entity, 0)

	// For empty list of ids, return empty list
	if len(entityIDs) == 0 {
		return list, nil
	}

	table := tableName(factory().TABLE(), keys...)
	SQL := fmt.Sprintf(`SELECT id, data FROM "%s" WHERE id = ANY($1)`, table)
	logger.Debug(SQL)

	if rows, err = dbs.pgDb.Query(SQL, pq.Array(entityIDs)); err != nil {
		return
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		jsonDoc := JsonDoc{}
		if err = rows.Scan(&jsonDoc.Id, &jsonDoc.Data); err != nil {
			return
		} else {
			entity := factory()
			if err = json.Unmarshal([]byte(jsonDoc.Data), &entity); err == nil {
				list = append(list, entity)
			}
		}
	}
	return
}

// Insert new entity
//
// param: entity - The entity to insert
// return: Inserted Entity, error
func (dbs *PostgresDatabase) Insert(entity Entity) (added Entity, err error) {
	var (
		result sql.Result
		data   []byte
	)

	tblName := tableName(entity.TABLE(), entity.KEY())

	SQL := fmt.Sprintf(sqlInsert, tblName)
	logger.Debug(SQL)

	if data, err = json.Marshal(entity); err != nil {
		return
	}

	if result, err = dbs.pgDb.Exec(SQL, entity.ID(), data); err != nil {
		return
	}

	if affected, err := result.RowsAffected(); err != nil {
		return nil, err
	} else if affected == 0 {
		err = fmt.Errorf("no row affected when inserting new entity")
	}
	added = entity

	// Publish the change
	dbs.publishChange(AddEntity, added)
	return
}

// Update existing entity
//
// param: entity - The entity to update
// return: Updated Entity, error
func (dbs *PostgresDatabase) Update(entity Entity) (updated Entity, err error) {

	var (
		result sql.Result
		data   []byte
	)

	tblName := tableName(entity.TABLE(), entity.KEY())
	SQL := fmt.Sprintf(sqlUpdate, tblName)
	logger.Debug(SQL)

	if data, err = json.Marshal(entity); err != nil {
		return
	}

	if result, err = dbs.pgDb.Exec(SQL, entity.ID(), data); err != nil {
		return
	}

	var affected int64
	if affected, err = result.RowsAffected(); err != nil {
		return
	} else if affected == 0 {
		return nil, fmt.Errorf("no row affected when executing update operation")
	}
	updated = entity

	// Publish the change
	dbs.publishChange(UpdateEntity, entity)
	return
}

// Upsert Update entity or insert it if it does not exist
//
// param: entity - The entity to update
// return: Updated Entity, error
func (dbs *PostgresDatabase) Upsert(entity Entity) (updated Entity, err error) {
	var (
		result sql.Result
		data   []byte
	)

	tblName := tableName(entity.TABLE(), entity.KEY())
	SQL := fmt.Sprintf(sqlUpsert, tblName)
	logger.Debug(SQL)

	if data, err = json.Marshal(entity); err != nil {
		return
	}

	if result, err = dbs.pgDb.Exec(SQL, entity.ID(), data); err != nil {
		return
	}

	var affected int64
	if affected, err = result.RowsAffected(); err != nil {
		return
	} else if affected == 0 {
		return nil, fmt.Errorf("no row affected when executing upsert operation")
	}
	updated = entity

	// Publish the change
	dbs.publishChange(UpdateEntity, entity)
	return
}

// Delete entity
//
// param: factory - Entity factory
// param: entityID - Entity ID to delete
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: error
func (dbs *PostgresDatabase) Delete(factory EntityFactory, entityID string, keys ...string) (err error) {
	var (
		affected int64
		result   sql.Result
	)
	entity := factory()

	// Get entity
	deleted, er := dbs.Get(factory, entityID, keys...)
	if er != nil {
		return er
	}

	tblName := tableName(entity.TABLE(), keys...)
	SQL := fmt.Sprintf(sqlDelete, tblName)
	logger.Debug(SQL)

	if result, err = dbs.pgDb.Exec(SQL, entityID); err != nil {
		return
	}

	if affected, err = result.RowsAffected(); err != nil {
		return
	} else if affected == 0 {
		return fmt.Errorf("no row affected when executing delete operation")
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
func (dbs *PostgresDatabase) BulkInsert(entities []Entity) (affected int64, err error) {

	if len(entities) == 0 {
		return 0, nil
	}

	// Get the table
	table := tableName(entities[0].TABLE(), entities[0].KEY())
	valueStrings := make([]string, 0, len(entities))
	valueArgs := make([]any, 0, len(entities)*2)
	i := 0
	for _, entity := range entities {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2))
		valueArgs = append(valueArgs, entity.ID())
		bytes, _ := json.Marshal(entity)
		valueArgs = append(valueArgs, string(bytes))
		i++
	}
	SQL := fmt.Sprintf(`INSERT INTO "%s" (id, data) VALUES %s`, table, strings.Join(valueStrings, ","))
	logger.Debug(SQL)

	var (
		result sql.Result
	)
	if result, err = dbs.pgDb.Exec(SQL, valueArgs...); err != nil {
		return
	}

	if affected, err = result.RowsAffected(); err != nil {
		return
	} else if affected == 0 {
		return affected, fmt.Errorf("no row affected when executing bulk insert operation")
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
func (dbs *PostgresDatabase) BulkUpdate(entities []Entity) (affected int64, err error) {

	if len(entities) == 0 {
		return 0, nil
	}

	var (
		tx *sql.Tx
	)

	// Start transaction
	if tx, err = dbs.pgDb.Begin(); err != nil {
		return
	}

	// Loop over entities and update each entity within the transaction scope
	for _, entity := range entities {
		table := tableName(entity.TABLE(), entity.KEY())
		SQL := fmt.Sprintf(sqlUpdate, table)
		data, _ := json.Marshal(entity)
		if _, err = dbs.pgDb.Exec(SQL, entity.ID(), data); err != nil {
			_ = tx.Rollback()
			return 0, err
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
func (dbs *PostgresDatabase) BulkUpsert(entities []Entity) (affected int64, err error) {

	if len(entities) == 0 {
		return 0, nil
	}

	var (
		tx *sql.Tx
	)

	// Start transaction
	if tx, err = dbs.pgDb.Begin(); err != nil {
		return
	}

	// Loop over entities and update each entity within the transaction scope
	for _, entity := range entities {
		table := tableName(entity.TABLE(), entity.KEY())
		SQL := fmt.Sprintf(sqlUpsert, table)
		data, _ := json.Marshal(entity)
		if _, err = dbs.pgDb.Exec(SQL, entity.ID(), data); err != nil {
			_ = tx.Rollback()
			return 0, err
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
func (dbs *PostgresDatabase) BulkDelete(factory EntityFactory, entityIDs []string, keys ...string) (affected int64, err error) {
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
	logger.Debug(SQL)

	if result, err = dbs.pgDb.Exec(SQL, pq.Array(entityIDs)); err != nil {
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
func (dbs *PostgresDatabase) SetField(factory EntityFactory, entityID string, field string, value any, keys ...string) (err error) {

	fields := make(map[string]any)
	fields[field] = value
	return dbs.SetFields(factory, entityID, fields, keys...)
}

// SetFields Update some fields of the document in a single transaction
//
// param: factory - Entity factory
// param: entityID - The entity ID to update the field
// param: fields - A map of field-value pairs to update
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: error
func (dbs *PostgresDatabase) SetFields(factory EntityFactory, entityID string, fields map[string]any, keys ...string) (err error) {

	entity := factory()

	tblName := tableName(entity.TABLE(), keys...)

	// Build list of SQL fields and args
	fieldsStrings := make([]string, 0, len(fields))
	fieldsArgs := make([]any, 0, len(fields))
	i := 1
	for f, v := range fields {
		fieldsStrings = append(fieldsStrings, fmt.Sprintf(`"%s": $%d`, f, i))
		fieldsArgs = append(fieldsArgs, v)
		i++
	}

	fieldsList := strings.Join(fieldsStrings, ",")
	SQL := fmt.Sprintf(`UPDATE "%s" SET data = data || '{%s}' WHERE id = $%d`, tblName, fieldsList, i)
	logger.Debug(SQL)

	// append entityID
	fieldsArgs = append(fieldsArgs, entityID)
	if _, err = dbs.pgDb.Exec(SQL, fieldsArgs...); err != nil {
		return
	}

	// Get the updated entity and publish the change
	if updated, fer := dbs.Get(factory, entityID, keys...); fer == nil {
		dbs.publishChange(UpdateEntity, updated)
	}
	return
}

//endregion

// region Database Query methods ---------------------------------------------------------------------------------------

// Query Helper method to construct query
//
// param: factory - Entity factory
// return: Query object
func (dbs *PostgresDatabase) Query(factory EntityFactory) database.IQuery {
	return &postgresDatabaseQuery{
		db:      dbs,
		factory: factory,
	}
}

//endregion

// region Database DDL methods -----------------------------------------------------------------------------------------

// ExecuteDDL create table and indexes
//
// param: ddl - The ddl parameter is a map of strings (table names) to array of strings (list of fields to index)
// return: error
func (dbs *PostgresDatabase) ExecuteDDL(ddl map[string][]string) (err error) {
	for table, fields := range ddl {

		SQL := fmt.Sprintf(ddlCreateTable, table)
		if _, err = dbs.pgDb.Exec(SQL); err != nil {
			logger.Debug("%s error: %s", SQL, err.Error())
			return
		}
		for _, field := range fields {
			sql := fmt.Sprintf(ddlCreateIndex, table, field, table, field)
			if _, err = dbs.pgDb.Exec(sql); err != nil {
				logger.Debug("%s error: %s", sql, err.Error())
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
func (dbs *PostgresDatabase) ExecuteSQL(sql string, args ...any) (affected int64, err error) {
	if result, er := dbs.pgDb.Exec(sql, args...); er != nil {
		logger.Debug("%s error: %s", sql, err.Error())
		return 0, er
	} else {
		if a, er := result.RowsAffected(); er != nil {
			return 0, er
		} else {
			return a, nil
		}
	}
}

// DropTable Drop table and indexes
//
// param: table - Table name to drop
// return: error
func (dbs *PostgresDatabase) DropTable(table string) (err error) {
	SQL := fmt.Sprintf(ddlDropTable, table)
	if _, err = dbs.pgDb.Exec(SQL); err != nil {
		logger.Debug("%s error: %s", SQL, err.Error())
	}
	return
}

// PurgeTable Fast delete table content (truncate)
//
// param: table - Table name to purge
// return: error
func (dbs *PostgresDatabase) PurgeTable(table string) (err error) {
	SQL := fmt.Sprintf(ddlPurgeTable, table)
	if _, err = dbs.pgDb.Exec(SQL); err != nil {
		logger.Debug("%s error: %s", SQL, err.Error())
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
func (dbs *PostgresDatabase) publishChange(action EntityAction, entity Entity) {

	// TODO: create pub/sub message and publish it on the injected message bug
}

// endregion

// region Datastore  methods -------------------------------------------------------------------------------------------

// IndexExists tests if index exists
func (dbs *PostgresDatabase) IndexExists(indexName string) (exists bool) {
	// TODO: Add implementation
	return false
}

// CreateIndex creates an index (without mapping)
func (dbs *PostgresDatabase) CreateIndex(indexName string) (name string, err error) {
	// TODO: Add implementation
	return indexName, fmt.Errorf("not implemented")
}

// CreateEntityIndex creates an index of entity and add entity field mapping
func (dbs *PostgresDatabase) CreateEntityIndex(factory EntityFactory, key string) (name string, err error) {
	// TODO: Add implementation
	return key, fmt.Errorf("not implemented")
}

// DropIndex drops an index
func (dbs *PostgresDatabase) DropIndex(indexName string) (ack bool, err error) {
	// TODO: Add implementation
	return false, fmt.Errorf("not implemented")
}

// endregion
