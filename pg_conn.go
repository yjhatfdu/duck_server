package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/csv"
	"fmt"
	"github.com/marcboeker/go-duckdb"
	"github.com/sirupsen/logrus"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
)

var parameterStatus = map[string]string{
	"client_encoding":             "UTF8",
	"server_version":              "16.0-duckdb-1.0.0",
	"standard_conforming_strings": "on",
}

type portal struct {
	stmt   *stmtDesc
	values []driver.Value
}

type stmtDesc struct {
	query    string
	stmt     driver.Stmt
	columns  [][2]string
	numInput int
}

type PgConn struct {
	wire    *Wire
	server  *PgServer
	conn    driver.Conn
	db      *sql.DB
	stmts   map[string]*stmtDesc
	portal  map[string]portal
	cancel  context.CancelFunc
	keyData [8]byte
	inError bool
}

func newPgConn(conn net.Conn, server *PgServer) *PgConn {
	dbConn, err := server.Connector.Connect(context.Background())
	if err != nil {
		logrus.Fatalf("connect error: %v", err)
	}
	keyData := [8]byte{}
	_, _ = rand.Read(keyData[:])
	return &PgConn{
		wire: &Wire{
			conn:   conn,
			rd:     bufio.NewReaderSize(conn, 1024*1024),
			Writer: conn,
		},
		server:  server,
		conn:    dbConn,
		keyData: keyData,
		db:      server.conn,
	}
}

var copyInRegexp = regexp.MustCompile(`(?i)COPY\s+.*\s+FROM\s+STDIN`)

func detectCopyInSQl(sql string) bool {
	return copyInRegexp.MatchString(sql)
}

func (c *PgConn) Close() {
	for _, stmt := range c.stmts {
		if stmt.stmt != nil {
			_ = stmt.stmt.Close()
		}
	}
	_ = c.wire.conn.Close()
	_ = c.conn.Close()
	c.server.Close(c.keyData)
}

func (c *PgConn) Run() {
	c.stmts = make(map[string]*stmtDesc)
	c.portal = make(map[string]portal)
	go func() {
		defer c.Close()
		first, err := c.wire.ReadStartUpMessage()
		if err != nil {
			return
		}
		if m, ok := first.(*CancelRequestMessage); ok {
			c.server.CancelRequest(m.Key)
			return
		}
		startup, ok := first.(*StartUpMessage)
		if !ok {
			panic("invalid message type")
		}
		logrus.Debugf("receive startup: %v", startup)
		if err = c.Auth(startup.Parameters["user"]); err != nil {
			logrus.Debugf("auth error: %v", err)
			return
		}
		if err = c.SendBackendKeyData(); err != nil {
			logrus.Debugf("send backend key data error: %v", err)
			return
		}
		for key, value := range parameterStatus {
			if err = c.SendParameterStatus(key, value); err != nil {
				logrus.Debugf("send parameter status error: %v", err)
				return
			}
		}
		needReadyMessage := true
		for {
			if needReadyMessage {
				m := &ReadyForQueryMessage{Status: TransactionStatusIdle}
				if err = c.wire.WriteMessage(m); err != nil {
					logrus.Tracef("write ready for query error: %v", err)
					return
				}
			}
			msg, err := c.wire.ReadMessage()
			if err != nil {
				logrus.Tracef("read message error: %v", err)
				return
			}
			switch msg.Typ {
			case Query:
				if queryMsg, err := ParseQueryMessage(msg); err != nil {
					logrus.Tracef("parse query message error: %v", err)
					return
				} else {
					if err := c.SimpleQuery(queryMsg.Query); err != nil {
						logrus.Tracef("simple query error: %v", err)
						return
					}
				}
				needReadyMessage = true
				c.inError = false
			case Terminate:
				return
			case Sync:
				needReadyMessage = true
				c.inError = false
			case Parse:
				needReadyMessage = false
				if c.inError {
					continue
				}
				if parseMsg, err := ParseParseMessage(msg); err != nil {
					logrus.Tracef("parse parse message error: %v", err)
					return
				} else {
					if err := c.Prepare(parseMsg.Name, parseMsg.Query); err != nil {
						return
					}
				}
			case Describe:
				if c.inError {
					continue
				}
				needReadyMessage = false
				if describeMsg, err := ParseDescribeMessage(msg); err != nil {
					return
				} else {
					if err := c.DescribePrepared(describeMsg.Type, describeMsg.Name); err != nil {
						return
					}
				}
			case Bind:
				if c.inError {
					continue
				}
				needReadyMessage = false
				if bindMsg, err := ParseBindMessage(msg); err != nil {
					logrus.Tracef("parse bind message error: %v", err)
					return
				} else {
					if err := c.Bind(bindMsg.Statement, bindMsg.PortalName, bindMsg.ParameterValues); err != nil {
						return
					}
				}
			case Execute:
				if c.inError {
					continue
				}
				needReadyMessage = false
				if executeMsg, err := ParseExecuteMessage(msg); err != nil {
					logrus.Tracef("parse execute message error: %v", err)
					return
				} else {
					if err := c.Execute(executeMsg.PortalName, executeMsg.MaxRows); err != nil {
						return
					}
				}
			default:
				needReadyMessage = false
				logrus.Infof("unsupported message type: %c", msg.Typ)
				if err != nil {
					return
				}
			}
		}
	}()
}

const maxInputArgsUsePrepared = 20

func (c *PgConn) RunStmt(ctx context.Context, stmt driver.Stmt, values []driver.Value, sendRowDesc bool, query string) error {
	if stmt == nil {
		return c.wire.WriteMessage(NewMessage(EmptyQueryResponse, []byte{}))
	}

	var nv []driver.NamedValue
	if len(values) > 0 {
		nv = make([]driver.NamedValue, len(values))
		for i, v := range values {
			nv[i] = driver.NamedValue{Name: "", Ordinal: i + 1, Value: v}
		}
	}
	rows, err := stmt.(driver.StmtQueryContext).QueryContext(ctx, nv)
	if err != nil {
		return c.SendErrorResponse(err.Error())
	}
	defer rows.Close()
	columnNames := rows.Columns()
	rowValues := make([]driver.Value, len(columnNames))
	rowCount := 0
	if sendRowDesc {
		if err := rows.Next(rowValues); err != nil {
			if err == io.EOF {
				types, err := c.inferStmtOutputNamesAndTypes(ctx, query)
				if err != nil {
					return c.SendErrorResponse(err.Error())
				}
				if err := c.SendRowDescriptionWithColumnNameAndTypes(types); err != nil {
					return c.SendErrorResponse(err.Error())
				}
				return c.SendCommandComplete("(0 row)")
			}
			return c.SendErrorResponse(err.Error())
		}
		if err := c.SendRowDescription(columnNames, rowValues); err != nil {
			return c.SendErrorResponse(err.Error())
		}
		if err := c.SendRowData(rowValues); err != nil {
			return c.SendErrorResponse(err.Error())
		}
		rowCount++
	}
	for {
		if err := rows.Next(rowValues); err != nil {
			if err == io.EOF {
				break
			} else {
				return c.SendErrorResponse(err.Error())
			}
		} else {
			rowCount++
			if err := c.SendRowData(rowValues); err != nil {
				return c.SendErrorResponse(err.Error())
			}
		}
	}
	return c.SendCommandComplete(fmt.Sprintf("(%d row)", rowCount))
}

var createUserRegexp = regexp.MustCompile(`(?i)^\s*create\s+user\s+(\w+)\s+with\s+password\s+'(.*)'\s*;?\s*$`)
var testDiscardAllRegexp = regexp.MustCompile(`(?i)^\s*discard\s+all\s*;?\s*$`)

func (c *PgConn) SimpleQuery(query string) error {
	defer func() {
		c.inError = false
	}()
	logrus.Debugf("simple query: %s", query)
	if c.server.enableAuth {
		if createUserRegexp.MatchString(query) {
			m := createUserRegexp.FindStringSubmatch(query)
			if len(m) == 3 {
				user := m[1]
				password := m[2]
				if err := c.server.CreateUser(user, password); err != nil {
					return c.SendErrorResponse(err.Error())
				}
				return c.SendCommandComplete("CREATE USER")
			}
		}
	}
	if strings.TrimSpace(query) == "" {
		//send empty query response
		return c.wire.WriteMessage(NewMessage(EmptyQueryResponse, []byte{}))
	}
	if testDiscardAllRegexp.MatchString(query) {
		return c.DiscardAll()
	}
	if detectCopyInSQl(query) {
		return c.CopyIn(query)
	}
	if strings.HasPrefix("show transaction_read_only", query) {
		query = "select 0"
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	defer func() {
		cancel()
		c.cancel = nil
	}()
	stmt, err := c.conn.Prepare(query)
	if err != nil {
		if strings.Contains(err.Error(), "No statement to prepare") {
			return c.wire.WriteMessage(NewMessage(EmptyQueryResponse, []byte{}))
		}
		return c.SendErrorResponse(err.Error())
	}
	defer func() {
		stmt.Close()
	}()
	return c.RunStmt(ctx, stmt, nil, true, query)
}

func (c *PgConn) SendParameterDescription(numInput int) error {
	if numInput == 0 {
		return nil
	}
	data := make([]byte, 0)
	data = append(data, cint16(int16(numInput))...)
	for i := 0; i < numInput; i++ {
		data = append(data, cint32(0)...)
	}
	return c.wire.WriteMessage(NewMessage(ParameterDescription, data))
}

func (c *PgConn) SendRowDescriptionWithColumnNameAndTypes(columns [][2]string) error {

	columnData := make([]byte, 0)
	columnData = append(columnData, cint16(int16(len(columns)))...)
	for _, column := range columns {
		columnData = append(columnData, cstr(column[0])...)
		columnData = append(columnData, 0, 0, 0, 0, 0, 0)
		oid := pgOidFromType(duck2pgType(column[1]))            //oid for text
		columnData = append(columnData, cint32(oid)...)         // oid
		columnData = append(columnData, 0, 0, 0, 0, 0, 0, 0, 0) // type modifier and format code
	}
	return c.wire.WriteMessage(NewMessage(RowDescription, columnData))
}

func (c *PgConn) SendRowDescription(columnNames []string, firstRowValues []driver.Value) error {
	columnData := make([]byte, 0)
	columnData = append(columnData, cint16(int16(len(columnNames)))...)
	if firstRowValues == nil {
		for _, name := range columnNames {
			columnData = append(columnData, cstr(name)...)
			columnData = append(columnData, 0, 0, 0, 0)
			columnData = append(columnData, 0, 0)
			columnData = append(columnData, 0, 0, 0, 25) //oid for text
			columnData = append(columnData, 0, 0)        // type size
			columnData = append(columnData, 0, 0, 0, 0)  // type modifier
			columnData = append(columnData, 0, 0)        // format code
		}
	} else {
		for i, name := range columnNames {
			v := firstRowValues[i]
			pgVal, err := toPgValue(v)
			if err != nil {
				panic(err)
			}
			columnData = append(columnData, cstr(name)...)
			columnData = append(columnData, 0, 0, 0, 0)
			columnData = append(columnData, 0, 0)
			columnData = append(columnData, cint32(pgVal.typ.Oid)...)    // oid
			columnData = append(columnData, cint16(pgVal.typ.Typlen)...) // type size
			columnData = append(columnData, 0, 0, 0, 0)                  // type modifier
			columnData = append(columnData, 0, 0)                        // format code
		}
	}
	return c.wire.WriteMessage(NewMessage(RowDescription, columnData))
}

func (c *PgConn) SendErrorResponse(errStr string) error {
	logrus.Errorf("send error response: %s", errStr)
	c.inError = true
	data := make([]byte, 0)
	data = append(data, 'S')
	data = append(data, cstr("ERROR")...)
	data = append(data, 'C')
	data = append(data, cstr("SQL-0000")...)
	data = append(data, 'M')
	data = append(data, cstr(errStr)...)
	data = append(data, 0)
	return c.wire.WriteMessage(NewMessage(ErrorResponse, data))
}

func (c *PgConn) SendRowData(values []driver.Value) error {
	data := make([]byte, 0)
	data = append(data, cint16(len(values))...)
	for _, v := range values {
		if v == nil {
			data = append(data, cint32(-1)...)
		} else {
			pgVal, err := toPgValue(v)
			if err != nil {
				return err
			}
			if pgVal.val == nil {
				data = append(data, cint32(-1)...)
				continue
			}
			data = append(data, cint32(len(pgVal.val))...)
			data = append(data, pgVal.val...)
		}
	}
	return c.wire.WriteMessage(NewMessage(DataRow, data))
}

func (c *PgConn) SendBackendKeyData() error {
	return c.wire.WriteMessage(NewMessage(BackendKeyData, c.keyData[:]))
}

func (c *PgConn) SendCommandComplete(tag string) error {
	data := make([]byte, 0)
	data = append(data, cstr(tag)...)
	return c.wire.WriteMessage(NewMessage(CommandComplete, data))
}

func (c *PgConn) SendParameterStatus(key, value string) error {
	data := make([]byte, 0)
	data = append(data, cstr(key)...)
	data = append(data, cstr(value)...)
	return c.wire.WriteMessage(NewMessage(ParameterStatus, data))
}

func (c *PgConn) Prepare(name, sql string) error {
	if sql == "" {
		c.stmts[name] = &stmtDesc{query: sql}
		msg := NewMessage(ParseComplete, []byte{})
		return c.wire.WriteMessage(msg)
	}
	if strings.HasPrefix("show transaction_read_only", sql) {
		sql = "select 0"
	}
	//work around for datagrip in clickhouse mode
	if strings.HasPrefix(sql, "SET extra_float_digits") {
		sql = "select 1 limit 0"
	}
	if strings.HasPrefix(sql, "SET application_name") {
		sql = "select 1 limit 0"
	}
	logrus.Debugf("prepare %s: %s", name, sql)
	if name != "" {
		if _, ok := c.stmts[name]; ok {
			return c.SendErrorResponse(fmt.Sprintf("prepared statement %s already exists", name))
		}
	}
	stmt, err := c.conn.Prepare(sql)
	if err != nil {
		return c.SendErrorResponse(err.Error())
	}
	c.stmts[name] = &stmtDesc{stmt: stmt, query: sql, numInput: stmt.NumInput()}
	msg := NewMessage(ParseComplete, []byte{})
	return c.wire.WriteMessage(msg)
}

func (c *PgConn) DescribePrepared(typ byte, name string) error {
	var stmt *stmtDesc
	if typ == 'S' {
		stmt = c.stmts[name]
	} else if typ == 'P' {
		stmt = c.portal[name].stmt
	} else {
		return c.SendErrorResponse(fmt.Sprintf("unsupported describe type: %c", typ))
	}
	if stmt == nil {
		return c.SendErrorResponse(fmt.Sprintf("prepared statement %s not found", name))
	}
	if stmt.stmt == nil {
		return c.wire.WriteMessage(NewMessage(NoData, []byte{}))
	}
	n := stmt.stmt.NumInput()
	if err := c.SendParameterDescription(n); err != nil {
		return err
	}
	if stmt.columns == nil {
		out, err := c.inferStmtOutputNamesAndTypes(context.Background(), stmt.query)
		if err != nil {
			stmt.columns = make([][2]string, 0)
		}
		stmt.columns = out
	}
	return c.SendRowDescriptionWithColumnNameAndTypes(stmt.columns)
}

func (c *PgConn) Bind(name, portalName string, args []driver.Value) error {
	stmt, ok := c.stmts[name]
	if !ok {
		return c.SendErrorResponse(fmt.Sprintf("prepared statement %s not found", name))
	}
	p := portal{stmt: stmt, values: args}
	c.portal[portalName] = p
	msg := NewMessage(BindComplete, nil)
	return c.wire.WriteMessage(msg)
}

func (c *PgConn) Execute(portalName string, maxRows int32) error {
	p, ok := c.portal[portalName]
	if !ok {
		return c.SendErrorResponse(fmt.Sprintf("portal %s not found", portalName))
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	defer func() {
		cancel()
		c.cancel = nil
	}()
	// work around for bad performance of using prepared statement with many input args, use simple query instead
	// todo reduce cgo call in duckdb driver
	if p.stmt.numInput > maxInputArgsUsePrepared {
		query := bindValues(p.stmt.query, p.values)
		stmt, err := c.conn.Prepare(query)
		if err != nil {
			return c.SendErrorResponse(err.Error())
		}
		defer stmt.Close()
		return c.RunStmt(ctx, stmt, nil, false, p.stmt.query)
	}
	return c.RunStmt(ctx, p.stmt.stmt, p.values, false, p.stmt.query)
}

func (c *PgConn) DiscardAll() error {
	c.portal = make(map[string]portal)
	for _, stmt := range c.stmts {
		if stmt.stmt != nil {
			_ = stmt.stmt.Close()
		}
	}
	c.stmts = make(map[string]*stmtDesc)
	return c.SendCommandComplete("DISCARD ALL")
}

var extractCopyInRegexp = regexp.MustCompile(`(?i)COPY\s+(.*)\s+FROM\s+STDIN`)

func (c *PgConn) CopyIn(sql string) error {
	tableNames := strings.Split(extractCopyInRegexp.FindStringSubmatch(sql)[1], ".")
	var tableName, schemaName string
	if len(tableNames) == 1 {
		tableName = tableNames[0]
		schemaName = "main"
	} else {
		tableName = tableNames[1]
		schemaName = tableNames[0]
	}
	appender, err := duckdb.NewAppenderFromConn(c.conn, schemaName, tableName)
	if err != nil {
		return c.SendErrorResponse(err.Error())
	}
	defer appender.Close()
	columnTypes, err := c.QueryTableColumns(schemaName, tableName)
	if err != nil {
		return c.SendErrorResponse(err.Error())
	}
	convertors := make([]converter, len(columnTypes))
	for i, columnType := range columnTypes {
		convertor := getDuckDBConverter(columnType)
		if convertor == nil {
			return c.SendErrorResponse(fmt.Sprintf("unsupported column type: %s", columnType))
		}
		convertors[i] = convertor
	}
	buf := make([]byte, 0)
	buf = append(buf, 0)
	buf = append(buf, cint16(len(columnTypes))...)
	buf = append(buf, make([]byte, len(columnTypes)*2)...)
	if err := c.wire.WriteMessage(NewMessage(CopyInResponse, buf)); err != nil {
		return err
	}
	cr := csv.NewReader(&copyReader{wire: c.wire})
	v := make([]driver.Value, len(columnTypes))
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	defer func() {
		cancel()
		c.cancel = nil
	}()
	var canceled bool
	go func() {
		<-ctx.Done()
		canceled = true
	}()
	rowCount := 0
	for {
		if canceled {
			return c.SendCopyFail()
		}
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return c.SendErrorResponse(err.Error())
		}
		for i, val := range row {
			v[i], err = convertors[i](val)
			if err != nil {
				return c.SendErrorResponse(err.Error())
			}
		}
		if err := appender.AppendRow(v...); err != nil {
			return c.SendErrorResponse(err.Error())
		}
		rowCount++
	}
	if err := appender.Flush(); err != nil {
		return c.SendErrorResponse(err.Error())
	}
	return c.SendCommandComplete(fmt.Sprintf("COPY %d", rowCount))
}

func (c *PgConn) QueryTableColumns(schema, table string) ([]string, error) {
	stmt, err := c.conn.Prepare(`select data_type from information_schema.columns where table_schema=? and table_name=?`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query([]driver.Value{schema, table})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columnTypes := make([]string, 0)
	columns := make([]driver.Value, 1)
	for {
		if err := rows.Next(columns); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		columnTypes = append(columnTypes, columns[0].(string))
	}
	return columnTypes, nil
}

func (c *PgConn) SendCopyFail() error {
	return c.wire.WriteMessage(NewMessage(CopyFail, nil))
}

var placeholderRegexp = regexp.MustCompile(`\$\d+`)

func (c *PgConn) inferStmtOutputNamesAndTypes(ctx context.Context, query string) ([][2]string, error) {
	probeQuery := fmt.Sprintf("describe %s", placeholderRegexp.ReplaceAllString(query, "null"))
	rows, err := c.db.QueryContext(ctx, probeQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columnNameTypes := make([][2]string, 0)
	var columnName, columnType, n1, n2, n3, n4 sql.Null[string]
	for rows.Next() {
		if err := rows.Scan(&columnName, &columnType, &n1, &n2, &n3, &n4); err != nil {
			return nil, err
		}
		columnNameTypes = append(columnNameTypes, [2]string{columnName.V, columnType.V})
	}
	return columnNameTypes, nil
}

type copyReader struct {
	wire *Wire
	buf  []byte
}

func (r *copyReader) Read(p []byte) (n int, err error) {
	if len(r.buf) > 0 {
		n := copy(p, r.buf)
		if n < len(r.buf) {
			r.buf = r.buf[len(p):]
		} else {
			r.buf = nil
		}
		return n, nil
	} else {
		msg, err := r.wire.ReadMessage()
		if err != nil {
			return 0, err
		}
		switch msg.Typ {
		case CopyData:
			_, err := msg.Read()
			if err != nil {
				return 0, err
			}
			r.buf = msg.buf
			n := copy(p, r.buf)
			if n < len(r.buf) {
				r.buf = r.buf[len(p):]
			} else {
				r.buf = nil
			}
			return n, nil
		case CopyDone:
			return 0, io.EOF
		case CopyFail:
			return 0, fmt.Errorf("copy fail")
		default:
			return 0, fmt.Errorf("unexpected message type: %v", msg.Typ)
		}
	}
}

// todo use lexer for better correctness
func bindValues(sql string, args []driver.Value) string {
	sb := strings.Builder{}
	lastIndex := 0
	for {
		idx := strings.IndexByte(sql[lastIndex:], '$')
		if idx < 0 {
			break
		}
		sb.WriteString(sql[lastIndex : lastIndex+idx])
		lastIndex += idx
		i := 0
		seg := sql[lastIndex:]
		for ; i < len(seg)-1; i++ {
			chr := seg[i+1]
			if chr < '0' || chr > '9' {
				break
			}
		}
		if i == 0 {
			sb.WriteByte('$')
			lastIndex++
			continue
		}
		valueIdx, _ := strconv.ParseInt(seg[1:i+1], 10, 64)
		if int(valueIdx) > len(args) {
			sb.WriteString("null")
			lastIndex += i + 1
			continue
		} else {
			v := args[valueIdx-1]
			if v == nil {
				sb.WriteString("null")
				lastIndex += i + 1
				continue
			}
			switch vv := v.(type) {
			case string:
				sb.WriteString("'" + strings.ReplaceAll(vv, "'", "''") + "'")
			case int64:
				sb.WriteString(strconv.FormatInt(vv, 10))
			case float64:
				sb.WriteString(strconv.FormatFloat(vv, 'f', -1, 64))
			default:
				panic(fmt.Sprintf("unsupported bind type: %T", vv))
			}
			lastIndex += i + 1
		}
	}
	sb.WriteString(sql[lastIndex:])
	return sb.String()
}
