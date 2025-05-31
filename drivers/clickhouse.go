package drivers

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/xo/dburl"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jorgerojas26/lazysql/models"
)

type Clickhouse struct {
	Connection *sql.DB
	Provider   string
}

func (db *Clickhouse) TestConnection(urlstr string) (err error) {
	return db.Connect(urlstr)
}

func (db *Clickhouse) Connect(urlstr string) (err error) {
	db.SetProvider(DriverClickhouse)

	url, err := dburl.Parse(urlstr)
	if err != nil {
		return err
	}

	password, _ := url.User.Password()

	opts := &clickhouse.Options{
		Addr: []string{url.Host},
		Auth: clickhouse.Auth{
			Database: strings.TrimPrefix(url.Path, "/"),
			Username: url.User.Username(),
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": url.Query().Get("max_execution_time"),
		},
	}

	if url.Transport == "http" {
		opts.Protocol = clickhouse.HTTP
	}

	db.Connection = clickhouse.OpenDB(opts)

	err = db.Connection.Ping()
	if err != nil {
		return err
	}

	return nil
}

func (db *Clickhouse) GetDatabases() ([]string, error) {
	var databases []string

	rows, err := db.Connection.Query("SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var database string
		err := rows.Scan(&database)
		if err != nil {
			return nil, err
		}
		if database != "information_schema" && database != "mysql" && database != "performance_schema" && database != "sys" {
			databases = append(databases, database)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return databases, nil
}

func (db *Clickhouse) GetTables(database string) (map[string][]string, error) {
	if database == "" {
		return nil, errors.New("database name is required")
	}

	rows, err := db.Connection.Query(fmt.Sprintf("SHOW TABLES FROM `%s`", database))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make(map[string][]string)
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return nil, err
		}

		tables[database] = append(tables[database], table)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tables, nil
}

func (db *Clickhouse) GetTableColumns(database, table string) (results [][]string, err error) {
	if database == "" {
		return nil, errors.New("database name is required")
	}

	if table == "" {
		return nil, errors.New("table name is required")
	}

	query := "DESCRIBE "
	query += db.formatTableName(database, table)

	rows, err := db.Connection.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results = append(results, columns)

	for rows.Next() {
		rowValues := make([]interface{}, len(columns))

		for i := range columns {
			rowValues[i] = new(sql.RawBytes)
		}

		err = rows.Scan(rowValues...)
		if err != nil {
			return nil, err
		}

		var row []string
		for _, col := range rowValues {
			row = append(row, string(*col.(*sql.RawBytes)))
		}

		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (db *Clickhouse) GetConstraints(database, table string) (results [][]string, err error) {
	if database == "" {
		return nil, errors.New("database name is required")
	}

	if table == "" {
		return nil, errors.New("table name is required")
	}

	// ClickHouse doesn't support foreign key constraints in the same way as other DBs
	// Return empty results since constraints aren't applicable
	results = append(results, []string{"CONSTRAINT_NAME", "COLUMN_NAME", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME"})
	return results, nil
}

func (db *Clickhouse) GetForeignKeys(database, table string) (results [][]string, err error) {
	if database == "" {
		return nil, errors.New("database name is required")
	}

	if table == "" {
		return nil, errors.New("table name is required")
	}
	// ClickHouse doesn't support foreign key constraints in the same way as other DBs
	// Return empty results since foreign keys aren't applicable
	results = append(results, []string{"TABLE_NAME", "COLUMN_NAME", "CONSTRAINT_NAME", "REFERENCED_COLUMN_NAME", "REFERENCED_TABLE_NAME"})
	return results, nil
}

func (db *Clickhouse) GetIndexes(database, table string) (results [][]string, err error) {
	if database == "" {
		return nil, errors.New("database name is required")
	}

	if table == "" {
		return nil, errors.New("table name is required")
	}

	// Add standard index column headers that match other drivers
	results = append(results, []string{"INDEX_NAME", "COLUMN_NAME", "NON_UNIQUE", "INDEX_TYPE"})

	// Query to get indices from system.tables
	query := `
		SELECT 
			name as index_name,
			engine as index_type,
			create_table_query as column_names
		FROM system.tables 
		WHERE database = ? AND name = ?`

	rows, err := db.Connection.Query(query, database, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var indexName, indexType, createQuery string
		err = rows.Scan(&indexName, &indexType, &createQuery)
		if err != nil {
			return nil, err
		}

		// In ClickHouse, indices are always unique
		nonUnique := "0"

		// Extract column names from the create table query
		// This is a simplified approach - you might need to enhance the parsing
		// based on your specific needs
		columns := extractColumnsFromCreateQuery(createQuery)
		for _, col := range columns {
			row := []string{
				indexName,
				strings.TrimSpace(col),
				nonUnique,
				indexType,
			}
			results = append(results, row)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// Helper function to extract column names from CREATE TABLE query
func extractColumnsFromCreateQuery(query string) []string {
	// This is a simplified implementation
	// You might need to enhance it based on your specific needs
	var columns []string

	// Look for ORDER BY clause which typically contains the primary key/index columns
	if orderByIndex := strings.Index(strings.ToUpper(query), "ORDER BY"); orderByIndex != -1 {
		// Extract the ORDER BY clause
		orderByClause := query[orderByIndex+8:]
		// Find the end of the ORDER BY clause (next keyword or end of query)
		if endIndex := strings.IndexAny(orderByClause, " \n\t,;"); endIndex != -1 {
			orderByClause = orderByClause[:endIndex]
		}
		// Split by comma and trim spaces
		for _, col := range strings.Split(orderByClause, ",") {
			col = strings.TrimSpace(col)
			if col != "" {
				columns = append(columns, col)
			}
		}
	}

	return columns
}

func (db *Clickhouse) GetRecords(database, table, where, sort string, offset, limit int) (paginatedResults [][]string, totalRecords int, err error) {
	if table == "" {
		return nil, 0, errors.New("table name is required")
	}

	if database == "" {
		return nil, 0, errors.New("database name is required")
	}

	if limit == 0 {
		limit = DefaultRowLimit
	}

	query := "SELECT * FROM "
	query += db.formatTableName(database, table)

	if where != "" {
		query += fmt.Sprintf(" %s", where)
	}

	if sort != "" {
		query += fmt.Sprintf(" ORDER BY %s", sort)
	}

	query += " LIMIT ?, ?"

	paginatedRows, err := db.Connection.Query(query, offset, limit)
	if err != nil {
		return nil, 0, err
	}
	defer paginatedRows.Close()

	columns, err := paginatedRows.Columns()
	if err != nil {
		return nil, 0, err
	}

	paginatedResults = append(paginatedResults, columns)

	for paginatedRows.Next() {
		// Get column types to identify IP addresses
		columnTypes, err := paginatedRows.ColumnTypes()
		if err != nil {
			return nil, 0, err
		}

		rowValues := make([]interface{}, len(columns))
		for i := range columns {
			// Check if the column is an IP type
			if columnTypes[i].DatabaseTypeName() == "IPv4" || columnTypes[i].DatabaseTypeName() == "IPv6" {
				var ip net.IP
				rowValues[i] = &ip
			} else {
				var nullStr sql.NullString
				rowValues[i] = &nullStr
			}
		}

		err = paginatedRows.Scan(rowValues...)
		if err != nil {
			return nil, 0, err
		}

		var row []string
		for _, col := range rowValues {
			switch v := col.(type) {
			case *net.IP:
				if v == nil {
					row = append(row, "NULL&")
				} else {
					row = append(row, v.String())
				}
			case *sql.NullString:
				if v.Valid {
					if v.String == "" {
						row = append(row, "EMPTY&")
					} else {
						row = append(row, v.String)
					}
				} else {
					row = append(row, "NULL&")
				}
			}
		}

		paginatedResults = append(paginatedResults, row)
	}
	if err := paginatedRows.Err(); err != nil {
		return nil, 0, err
	}
	// close to release the connection
	if err := paginatedRows.Close(); err != nil {
		return nil, 0, err
	}

	countQuery := "SELECT COUNT(*) FROM "
	countQuery += fmt.Sprintf("`%s`.", database)
	countQuery += fmt.Sprintf("`%s`", table)
	row := db.Connection.QueryRow(countQuery)
	if err := row.Scan(&totalRecords); err != nil {
		return nil, 0, err
	}

	return paginatedResults, totalRecords, nil
}

func (db *Clickhouse) ExecuteQuery(query string) ([][]string, int, error) {
	rows, err := db.Connection.Query(query)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, 0, err
	}

	records := make([][]string, 0)
	for rows.Next() {
		rowValues := make([]interface{}, len(columns))
		for i := range columns {
			rowValues[i] = new(sql.RawBytes)
		}

		err = rows.Scan(rowValues...)
		if err != nil {
			return nil, 0, err
		}

		var row []string
		for _, col := range rowValues {
			row = append(row, string(*col.(*sql.RawBytes)))
		}

		records = append(records, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	// Prepend the columns to the records.
	results := append([][]string{columns}, records...)

	return results, len(records), nil
}

func (db *Clickhouse) UpdateRecord(database, table, column, value, primaryKeyColumnName, primaryKeyValue string) error {
	query := "UPDATE "
	query += db.formatTableName(database, table)
	query += fmt.Sprintf(" SET %s = ? WHERE %s = ?", column, primaryKeyColumnName)

	_, err := db.Connection.Exec(query, value, primaryKeyValue)

	return err
}

func (db *Clickhouse) DeleteRecord(database, table, primaryKeyColumnName, primaryKeyValue string) error {
	query := "DELETE FROM "
	query += db.formatTableName(database, table)
	query += fmt.Sprintf(" WHERE %s = ?", primaryKeyColumnName)
	_, err := db.Connection.Exec(query, primaryKeyValue)

	return err
}

func (db *Clickhouse) ExecuteDMLStatement(query string) (result string, err error) {
	res, err := db.Connection.Exec(query)
	if err != nil {
		return "", err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%d rows affected", rowsAffected), nil
}

func (db *Clickhouse) ExecutePendingChanges(changes []models.DBDMLChange) error {
	var queries []models.Query

	for _, change := range changes {

		formattedTableName := db.formatTableName(change.Database, change.Table)

		switch change.Type {

		case models.DMLInsertType:
			queries = append(queries, buildInsertQuery(formattedTableName, change.Values, db))
		case models.DMLUpdateType:
			queries = append(queries, buildUpdateQuery(formattedTableName, change.Values, change.PrimaryKeyInfo, db))
		case models.DMLDeleteType:
			queries = append(queries, buildDeleteQuery(formattedTableName, change.PrimaryKeyInfo, db))
		}
	}

	return queriesInTransaction(db.Connection, queries)
}

func (db *Clickhouse) GetPrimaryKeyColumnNames(database, table string) (primaryKeyColumnName []string, err error) {
	if database == "" {
		return nil, errors.New("database name is required")
	}

	if table == "" {
		return nil, errors.New("table name is required")
	}

	rows, err := db.Connection.Query("SELECT name FROM system.columns WHERE database = ? AND table = ? AND is_in_primary_key = 1", database, table)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var colName string
		err = rows.Scan(&colName)
		if err != nil {
			return nil, err
		}

		if rows.Err() != nil {
			return nil, rows.Err()
		}

		primaryKeyColumnName = append(primaryKeyColumnName, colName)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return primaryKeyColumnName, nil
}

func (db *Clickhouse) SetProvider(provider string) {
	db.Provider = provider
}

func (db *Clickhouse) GetProvider() string {
	return db.Provider
}

func (db *Clickhouse) formatTableName(database, table string) string {
	return fmt.Sprintf("`%s`.`%s`", database, table)
}

func (db *Clickhouse) FormatArg(arg any) string {
	if arg == "NULL" || arg == "DEFAULT" {
		return fmt.Sprintf("%v", arg)
	}

	switch v := arg.(type) {
	case int, int64:
		return fmt.Sprintf("%d", v)
	case float64, float32:
		s := fmt.Sprintf("%f", v)
		trimmed := strings.TrimRight(s, "0")
		if strings.HasSuffix(trimmed, ".") {
			trimmed += "0"
		}
		return trimmed
	case string:
		escaped := strings.ReplaceAll(v, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	case []byte:
		escaped := strings.ReplaceAll(string(v), "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (db *Clickhouse) FormatReference(reference string) string {
	return fmt.Sprintf("`%s`", reference)
}

func (db *Clickhouse) FormatPlaceholder(_ int) string {
	return "?"
}

func (db *Clickhouse) DMLChangeToQueryString(change models.DBDMLChange) (string, error) {
	var queryStr string
	formattedTableName := db.formatTableName(change.Database, change.Table)

	switch change.Type {
	case models.DMLInsertType:
		// ClickHouse uses standard INSERT syntax
		var columnNames []string
		var values []string
		for _, val := range change.Values {
			columnNames = append(columnNames, val.Column)
			values = append(values, db.FormatArg(val.Value))
		}
		queryStr = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			formattedTableName,
			strings.Join(columnNames, ", "),
			strings.Join(values, ", "))

	case models.DMLUpdateType:
		// ClickHouse uses ALTER TABLE UPDATE syntax
		var setClauses []string
		for _, val := range change.Values {
			setClauses = append(setClauses, fmt.Sprintf("%s = %s", val.Column, db.FormatArg(val.Value)))
		}

		var whereClauses []string
		for _, pk := range change.PrimaryKeyInfo {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", pk.Name, db.FormatArg(pk.Value)))
		}

		queryStr = fmt.Sprintf("ALTER TABLE %s UPDATE %s WHERE %s",
			formattedTableName,
			strings.Join(setClauses, ", "),
			strings.Join(whereClauses, " AND "))

	case models.DMLDeleteType:
		// ClickHouse uses ALTER TABLE DELETE syntax
		var whereClauses []string
		for _, pk := range change.PrimaryKeyInfo {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", pk.Name, db.FormatArg(pk.Value)))
		}

		queryStr = fmt.Sprintf("ALTER TABLE %s DELETE WHERE %s",
			formattedTableName,
			strings.Join(whereClauses, " AND "))
	}

	return queryStr, nil
}
