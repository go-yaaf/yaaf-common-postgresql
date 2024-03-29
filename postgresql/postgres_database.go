// Postgresql object database implementations of IDatabase interface
//

package postgresql

import (
	"cloud.google.com/go/cloudsqlconn"
	"context"
	"fmt"
	"github.com/go-yaaf/yaaf-common/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/go-yaaf/yaaf-common/database"
	. "github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/logger"
	"github.com/go-yaaf/yaaf-common/messaging"
)

// region Database store definitions -----------------------------------------------------------------------------------

type PostgresDatabase struct {
	poolDb *pgxpool.Pool
	bus    messaging.IMessageBus
	uri    string
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
// param: URI - represents the database connection string in the format of: postgresql://user:password@host:port/database_name?application_name
// return: IDatabase instance, error
func NewPostgresStore(URI string) (dbs database.IDatastore, err error) {
	return createPostgresDatabase(URI)
}

// NewPostgresDatabase factory method for database
// param: URI - represents the database connection string in the format of: postgresql://user:password@host:port/database_name?application_name
// return: IDatabase instance, error
func NewPostgresDatabase(URI string) (dbs database.IDatabase, err error) {
	return createPostgresDatabase(URI)
}

// NewPostgresDatabaseWithMessageBus factory method for database with injected message bus
// param: URI - represents the database connection string in the format of: postgresql://user:password@host:port/database_name?application_name
// return: IDatabase instance, error
func NewPostgresDatabaseWithMessageBus(URI string, bus messaging.IMessageBus) (dbs database.IDatabase, err error) {
	var db *PostgresDatabase
	if db, err = createPostgresDatabase(URI); err != nil {
		return
	}
	db.bus = bus
	return
}

func createPostgresDatabase(dbUri string) (dbs *PostgresDatabase, err error) {
	var (
		connStr  string
		poolCfg  *pgxpool.Config
		poolConn *pgxpool.Pool
	)

	// Ensure driver name
	if _, connStr, err = convertConnectionString(dbUri); err != nil {
		return nil, err
	}

	if poolCfg, err = pgxpool.ParseConfig(connStr); err != nil {
		return
	}

	poolCfg.MaxConns = int32(config.Get().MaxDbConnections())

	poolCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		logger.Debug("new client db connection established.")
		return nil
	}

	poolCfg.BeforeClose = func(conn *pgx.Conn) {
		logger.Debug("client db connection closed.")
	}

	//try to get connection name. If we got one non-empty,
	//means we are connection via cloud-sql-proxy-connector-go
	dbConnName := config.Get().RdsInstanceName()

	if dbConnName != "" {
		var d *cloudsqlconn.Dialer

		d, err = cloudsqlconn.NewDialer(context.Background())

		if err != nil {
			return
		}

		poolCfg.ConnConfig.DialFunc = func(ctx context.Context, _ string, _ string) (net.Conn, error) {
			return d.Dial(ctx, dbConnName)
		}
	}

	if poolConn, err = pgxpool.NewWithConfig(context.Background(), poolCfg); err != nil {
		return
	}

	if err = poolConn.Ping(context.Background()); err != nil {
		return
	}
	dbs = &PostgresDatabase{poolDb: poolConn, uri: dbUri}
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

	// if application_name parameter is not provided, get it from the executable name
	if len(appName) == 0 {
		executablePath := os.Args[0]            // Gets the path of the currently running executable
		appName = filepath.Base(executablePath) // Extracts the executable name from the path
	}

	connStr = fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=%s application_name=%s",
		host, port, dbName, usr, pwd, "disable", appName)

	return "pgx", connStr, nil
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
		err := dbs.poolDb.Ping(context.Background())
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
	dbs.poolDb.Close()
	return nil
}

// CloneDatabase Returns a clone (copy) of the database instance
func (dbs *PostgresDatabase) CloneDatabase() (database.IDatabase, error) {
	return NewPostgresDatabaseWithMessageBus(dbs.uri, dbs.bus)
}

// CloneDatastore Returns a clone (copy) of the database instance
func (dbs *PostgresDatabase) CloneDatastore() (database.IDatastore, error) {
	return NewPostgresStore(dbs.uri)
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
		rows pgx.Rows
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

	SQL := fmt.Sprintf(`SELECT id, data FROM "%s" WHERE id = $1`, tableName(result.TABLE(), keys...))

	if rows, err = dbs.poolDb.Query(context.Background(), SQL, entityID); err != nil {
		return nil, err
	}

	// Connection is released to pool only after rows is closed.
	defer rows.Close()

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
func (dbs *PostgresDatabase) Exists(factory EntityFactory, entityID string, keys ...string) (result bool, err error) {

	var rows pgx.Rows

	SQL := fmt.Sprintf(`SELECT id FROM "%s" WHERE id = $1`, tableName(factory().TABLE(), keys...))

	if rows, err = dbs.poolDb.Query(context.Background(), SQL, entityID); err != nil {
		return false, err
	}
	result = rows.Next()
	rows.Close()
	return result, nil
}

// List Get list of entities by IDs
//
// param: factory - Entity factory
// param: entityIDs - List of Entity IDs
// param: keys - Sharding key(s) (for sharded entities and multi-tenant support)
// return: []Entity, error
func (dbs *PostgresDatabase) List(factory EntityFactory, entityIDs []string, keys ...string) (list []Entity, err error) {

	var (
		rows pgx.Rows
	)

	list = make([]Entity, 0)

	// For empty list of ids, return empty list
	if len(entityIDs) == 0 {
		return list, nil
	}

	table := tableName(factory().TABLE(), keys...)
	SQL := fmt.Sprintf(`SELECT id, data FROM "%s" WHERE id = ANY($1)`, table)
	if rows, err = dbs.poolDb.Query(context.Background(), SQL, entityIDs); err != nil {
		return
	}
	defer rows.Close()

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
func (dbs *PostgresDatabase) Insert(entity Entity) (added Entity, err error) {
	var (
		result pgconn.CommandTag
		data   []byte
	)

	tblName := tableName(entity.TABLE(), entity.KEY())

	SQL := fmt.Sprintf(sqlInsert, tblName)
	if data, err = Marshal(entity); err != nil {
		return
	}

	if result, err = dbs.poolDb.Exec(context.Background(), SQL, entity.ID(), data); err != nil {
		return
	}
	if result.RowsAffected() == 0 {
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
		result pgconn.CommandTag
		data   []byte
	)

	tblName := tableName(entity.TABLE(), entity.KEY())
	SQL := fmt.Sprintf(sqlUpdate, tblName)
	if data, err = Marshal(entity); err != nil {
		return
	}

	if result, err = dbs.poolDb.Exec(context.Background(), SQL, entity.ID(), data); err != nil {
		return
	}
	if result.RowsAffected() == 0 {
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
		data []byte
	)

	tblName := tableName(entity.TABLE(), entity.KEY())
	SQL := fmt.Sprintf(sqlUpsert, tblName)
	if data, err = Marshal(entity); err != nil {
		return
	}

	if _, err = dbs.poolDb.Exec(context.Background(), SQL, entity.ID(), data); err != nil {
		return
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
	//affected int64
	//result   sql.Result
	)
	entity := factory()

	// Get entity
	deleted, er := dbs.Get(factory, entityID, keys...)
	if er != nil {
		return er
	}

	tblName := tableName(entity.TABLE(), keys...)
	SQL := fmt.Sprintf(sqlDelete, tblName)
	if _, err = dbs.poolDb.Exec(context.Background(), SQL, entityID); err != nil {
		return
	}
	/*
		if affected, err = result.RowsAffected(); err != nil {
			return
		} else if affected == 0 {
			return fmt.Errorf("no row affected when executing delete operation")
		}
	*/
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
		bytes, _ := Marshal(entity)
		valueArgs = append(valueArgs, string(bytes))
		i++
	}
	SQL := fmt.Sprintf(`INSERT INTO "%s" (id, data) VALUES %s`, table, strings.Join(valueStrings, ","))

	var (
	//result sql.Result
	)
	if _, err = dbs.poolDb.Exec(context.Background(), SQL, valueArgs...); err != nil {
		return
	}
	/*
		if affected, err = result.RowsAffected(); err != nil {
			return
		} else if affected == 0 {
			return affected, fmt.Errorf("no row affected when executing bulk insert operation")
		}
	*/

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
		tx pgx.Tx
	)

	// Start transaction
	ctx := context.Background()
	if tx, err = dbs.poolDb.Begin(ctx); err != nil {
		return
	}

	// Loop over entities and update each entity within the transaction scope
	for _, entity := range entities {
		table := tableName(entity.TABLE(), entity.KEY())
		SQL := fmt.Sprintf(sqlUpdate, table)
		data, _ := Marshal(entity)
		if _, err = dbs.poolDb.Exec(ctx, SQL, entity.ID(), data); err != nil {
			_ = tx.Rollback(ctx)
			return 0, err
		}
	}

	// Commit the transaction
	if err = tx.Commit(ctx); err != nil {
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
		tx pgx.Tx
	)

	// Start transaction
	ctx := context.Background()
	if tx, err = dbs.poolDb.Begin(ctx); err != nil {
		return
	}

	// Loop over entities and update each entity within the transaction scope
	for _, entity := range entities {
		table := tableName(entity.TABLE(), entity.KEY())
		SQL := fmt.Sprintf(sqlUpsert, table)
		data, _ := Marshal(entity)
		if _, err = dbs.poolDb.Exec(ctx, SQL, entity.ID(), data); err != nil {
			_ = tx.Rollback(ctx)
			return 0, err
		}
	}

	// Commit the transaction
	if err = tx.Commit(ctx); err != nil {
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
		result pgconn.CommandTag
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

	if result, err = dbs.poolDb.Exec(context.Background(), SQL, entityIDs); err != nil {
		return
	}

	affected = result.RowsAffected()
	if affected == 0 {
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

	entity := factory()
	tblName := tableName(entity.TABLE(), keys...)

	SQL := fmt.Sprintf(`UPDATE "%s" SET data = jsonb_set(data, '{%s}', $1, false) WHERE id = $2`, tblName, field)

	args := make([]any, 0)
	args = append(args, value)
	args = append(args, entityID)

	if _, err = dbs.poolDb.Exec(context.Background(), SQL, args...); err != nil {
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
func (dbs *PostgresDatabase) SetFields(factory EntityFactory, entityID string, fields map[string]any, keys ...string) (err error) {

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
func (dbs *PostgresDatabase) BulkSetFields(factory EntityFactory, field string, values map[string]any, keys ...string) (affected int64, error error) {

	if len(values) == 0 {
		return 0, nil
	}

	// Determine the type of the field
	sqlType := dbs.getSqlType(values)

	// Create temp table to map entity to field id
	tmpTable := fmt.Sprintf("ch%d", time.Now().UnixMilli())
	createTmp := fmt.Sprintf("create TEMP table %s (id character varying PRIMARY KEY NOT NULL, val %s)", tmpTable, sqlType)
	ctx := context.Background()
	if _, err := dbs.poolDb.Exec(ctx, createTmp); err != nil {
		return 0, err
	}

	// Bulk Insert values
	valueStrings := make([]string, 0, len(values))
	valueArgs := make([]any, 0, len(values)*2)
	i := 0
	for id, val := range values {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2))
		valueArgs = append(valueArgs, id)
		valueArgs = append(valueArgs, val)
		i++
	}
	SQL := fmt.Sprintf(`INSERT INTO "%s" (id, val) VALUES %s`, tmpTable, strings.Join(valueStrings, ","))

	if _, err := dbs.poolDb.Exec(ctx, SQL, valueArgs...); err != nil {
		return 0, err
	}

	// Create bulk update statement
	entity := factory()
	tblName := tableName(entity.TABLE(), keys...)

	SQL = fmt.Sprintf("UPDATE %s SET data['%s'] = to_jsonb(%s.val) FROM %s WHERE %s.id = %s.id", tblName, field, tmpTable, tmpTable, tmpTable, tblName)

	// Drop the temp table
	defer func() {
		DROP := fmt.Sprintf("DROP TABLE %s", tmpTable)
		_, _ = dbs.poolDb.Exec(ctx, DROP)
	}()

	// Execute update
	if result, err := dbs.poolDb.Exec(ctx, SQL); err != nil {
		return 0, err
	} else {
		return result.RowsAffected(), nil
	}
}

// Get the SQL type of the value
func (dbs *PostgresDatabase) getSqlType(values map[string]any) string {

	typeName := "string"
	for _, v := range values {
		typeName = fmt.Sprintf("%T", v)
		break
	}
	if strings.HasPrefix(typeName, "string") {
		return "character varying"
	}
	if strings.HasPrefix(typeName, "float") {
		return "double precision"
	}
	if strings.HasPrefix(typeName, "bool") {
		return "boolean"
	}

	// For all other types (numbers, timestamp, enums) return bigint
	return "bigint"
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

	ctx := context.Background()
	for table, fields := range ddl {

		SQL := fmt.Sprintf(ddlCreateTable, table)
		if _, err = dbs.poolDb.Exec(ctx, SQL); err != nil {
			logger.Error("%s error: %s", SQL, err.Error())
			return
		}
		for _, field := range fields {
			SQL = fmt.Sprintf(ddlCreateIndex, table, field, table, field)
			if _, err = dbs.poolDb.Exec(ctx, SQL); err != nil {
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
func (dbs *PostgresDatabase) ExecuteSQL(sql string, args ...any) (int64, error) {

	if result, err := dbs.poolDb.Exec(context.Background(), sql, args...); err != nil {
		logger.Error("%s error: %s", sql, err.Error())
		return 0, err
	} else {
		return result.RowsAffected(), nil
	}
}

// ExecuteQuery Execute native SQL query
func (dbs *PostgresDatabase) ExecuteQuery(sql string, args ...any) ([]Json, error) {

	rows, err := dbs.poolDb.Query(context.Background(), sql, args...)
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
		for i, _ := range cols {
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

// DropTable Drop table and indexes
//
// param: table - Table name to drop
// return: error
func (dbs *PostgresDatabase) DropTable(table string) (err error) {
	SQL := fmt.Sprintf(ddlDropTable, table)
	if _, err = dbs.poolDb.Exec(context.Background(), SQL); err != nil {
		logger.Error("%s error: %s", SQL, err.Error())
	}
	return
}

// PurgeTable Fast delete table content (truncate)
//
// param: table - Table name to purge
// return: error
func (dbs *PostgresDatabase) PurgeTable(table string) (err error) {
	SQL := fmt.Sprintf(ddlPurgeTable, table)
	if _, err = dbs.poolDb.Exec(context.Background(), SQL); err != nil {
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
func (dbs *PostgresDatabase) publishChange(action EntityAction, entity Entity) {

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

// ListIndices returns a list of all indices matching the pattern
func (dbs *PostgresDatabase) ListIndices(pattern string) (map[string]int, error) {
	// TODO: Add implementation
	return nil, fmt.Errorf("not implemented")
}

// DropIndex drops an index
func (dbs *PostgresDatabase) DropIndex(indexName string) (ack bool, err error) {
	// TODO: Add implementation
	return false, fmt.Errorf("not implemented")
}

// endregion
