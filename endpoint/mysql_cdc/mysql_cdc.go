/*
 * Copyright 2024 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mysql_cdc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
	"net/textproto"
	"regexp"
	"strings"
	"time"
)

// Type returns the component type
const Type = types.EndpointTypePrefix + "mysql_cdc"

const (
	KeyTableSchema   = "tableSchema"
	KeyTableName     = "tableName"
	KeyTableFullName = "tableFullName"
	KeyAction        = "action"
	KeyColumnNames   = "columnNames"
	KeyPkColumnNames = "pkColumnNames"
	KeyLogPos        = "logPos"
	// MatchAll matches all data
	MatchAll     = "*"
	ActionUpdate = "update"
)

// Endpoint alias
type Endpoint = MySqlCDC

var _ endpointApi.Endpoint = (*Endpoint)(nil)

// Register the component
func init() {
	_ = endpoint.Registry.Register(&Endpoint{})
}

// RequestMessage
type RequestMessage struct {
	Table *schema.Table
	//insert/update/delete
	Action string
	// Header can be used to inspect the event
	Header *replication.EventHeader
	// Data structure[][]interface{}, if updating the action, format: [Update previous data, update subsequent data]
	body []byte
	msg  *types.RuleMsg
	err  error
}

func (r *RequestMessage) Body() []byte {
	return r.body
}

func (r *RequestMessage) ColumnNames() []string {
	if r.Table != nil {
		var names []string
		for _, c := range r.Table.Columns {
			names = append(names, c.Name)
		}
		return names
	}
	return nil
}

func (r *RequestMessage) PKColumns() []string {
	if r.Table != nil {
		var names []string
		for _, index := range r.Table.PKColumns {
			names = append(names, r.Table.Columns[index].Name)
		}
		return names
	}
	return nil
}
func (r *RequestMessage) Headers() textproto.MIMEHeader {
	header := make(textproto.MIMEHeader)
	header.Set(KeyAction, r.Action)
	if r.Table == nil {
		header.Set(KeyTableFullName, r.Table.String())
		header.Set(KeyTableSchema, r.Table.Schema)
		header.Set(KeyTableName, r.Table.Name)
		header.Set(KeyColumnNames, str.ToString(r.ColumnNames()))
		header.Set(KeyPkColumnNames, str.ToString(r.PKColumns()))
	}
	if r.Header != nil {
		header.Set(KeyLogPos, str.ToString(r.Header.LogPos))
	}
	return header
}

func (r *RequestMessage) From() string {
	return r.Action
}

func (r *RequestMessage) GetParam(key string) string {
	return ""
}

func (r *RequestMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

func (r *RequestMessage) GetMsg() *types.RuleMsg {
	if r.msg == nil {
		//The default specification is JSON format. If it is not this type, please modify it in the process function
		ruleMsg := types.NewMsg(0, r.From(), types.JSON, types.NewMetadata(), string(r.Body()))
		ruleMsg.Metadata.PutValue(KeyAction, r.Action)
		if r.Table != nil {
			ruleMsg.Metadata.PutValue(KeyTableFullName, r.Table.String())
			ruleMsg.Metadata.PutValue(KeyTableSchema, r.Table.Schema)
			ruleMsg.Metadata.PutValue(KeyTableName, r.Table.Name)
			ruleMsg.Metadata.PutValue(KeyColumnNames, str.ToString(r.ColumnNames()))
			ruleMsg.Metadata.PutValue(KeyPkColumnNames, str.ToString(r.PKColumns()))
		}
		if r.Header != nil {
			ruleMsg.Metadata.PutValue(KeyLogPos, str.ToString(r.Header.LogPos))
		}
		r.msg = &ruleMsg
	}
	return r.msg
}

func (r *RequestMessage) SetStatusCode(statusCode int) {
}

func (r *RequestMessage) SetBody(body []byte) {
	r.body = body
}

func (r *RequestMessage) SetError(err error) {
	r.err = err
}

func (r *RequestMessage) GetError() error {
	return r.err
}

// ResponseMessage http Response message
type ResponseMessage struct {
	Table   *schema.Table
	Action  string
	body    []byte
	msg     *types.RuleMsg
	headers textproto.MIMEHeader
	err     error
	log     func(format string, v ...interface{})
}

func (r *ResponseMessage) Body() []byte {
	return r.body
}

func (r *ResponseMessage) Headers() textproto.MIMEHeader {
	if r.headers == nil {
		r.headers = make(map[string][]string)
	}
	return r.headers
}

func (r *ResponseMessage) From() string {
	return r.Action
}

func (r *ResponseMessage) GetParam(key string) string {
	return ""
}

func (r *ResponseMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}
func (r *ResponseMessage) GetMsg() *types.RuleMsg {
	return r.msg
}

func (r *ResponseMessage) SetStatusCode(statusCode int) {
}

func (r *ResponseMessage) SetBody(body []byte) {
	r.body = body
}

func (r *ResponseMessage) SetError(err error) {
	r.err = err
}

func (r *ResponseMessage) GetError() error {
	return r.err
}

type Config struct {
	// MySQL server address
	Server string `json:"server" label:"Server" desc:"MySQL server address, format: host:port" required:"true"`
	//Username
	User string `json:"user" label:"Username" desc:"MySQL authentication username"`
	// Password
	Password string `json:"password" label:"Password" desc:"MySQL authentication password"`
	//Does FromOldest synchronize from the oldest binlog, or if not, synchronize from the latest binlog and location?
	FromOldest bool `json:"fromOldest" label:"From Oldest" desc:"Sync from oldest binlog, otherwise sync from latest position"`
	// Database
	Dbs []string `json:"dbs" label:"Databases" desc:"Database names to watch, empty means all databases"`
	// IncludeTables or ExcludeTables should contain database name.
	// IncludeTables defines the tables that will be included, if empty, all tables will be included.
	// ExcludeTables defines the tables that will be excluded from the ones defined by IncludeTables.
	// Only a table which matches IncludeTables and dismatches ExcludeTables will be processed
	// eg, IncludeTables : [".*\\.canal","test.*"], ExcludeTables : ["mysql\\..*"]
	//     this will include all database's 'canal' table, except database 'mysql'.
	// Default IncludeTables and ExcludeTables are empty, this will include all tables
	IncludeTables []string `json:"includeTables" label:"Include Tables" desc:"Table regex patterns to include, e.g. mydb\\.users, test.*"`
	ExcludeTables []string `json:"excludeTables" label:"Exclude Tables" desc:"Table regex patterns to exclude, e.g. mysql\\..*"`

	// mysqldump execution path, like mysqldump or /usr/bin/mysqldump, etc...
	// If not set, ignore using mysqldump.
	ExecutionPath string `json:"executionPath" label:"Execution Path" desc:"mysqldump execution path, e.g. mysqldump or /usr/bin/mysqldump"`
	//Character set
	Charset string `json:"charset" label:"Charset" desc:"Connection charset, default utf8"`
	//mysql or mariadb
	Flavor string `json:"flavor" label:"Flavor" desc:"Database flavor: mysql or mariadb"`
	//Heartbeat is measured in seconds
	Heartbeat int `json:"heartbeat" label:"Heartbeat" desc:"Heartbeat interval in seconds"`
	// Read timeout units in seconds
	ReadTimeout int `json:"readTimeout" label:"Read Timeout" desc:"Read timeout in seconds"`
	//Limit the number of entries: 0: No limit, Other: If the value exceeds this value, ignore and do not process. Used to filter data for batch operations
	Limit int `json:"limit" label:"Limit" desc:"Max rows per event, 0 means no limit"`
}

// MySqlCDC receiving endpoint
type MySqlCDC struct {
	impl.BaseEndpoint
	RuleConfig types.Config
	//Config configuration
	Config Config
	// Route mapping table
	routers map[string]*RegexpRouter
	canal   *canal.Canal
}

// Type returns the component type
func (x *MySqlCDC) Type() string {
	return Type
}

func (x *MySqlCDC) Id() string {
	return x.Config.Server
}

func (x *MySqlCDC) New() types.Node {
	return &MySqlCDC{
		Config: Config{
			Server:        mysql.DEFAULT_ADDR,
			User:          mysql.DEFAULT_USER,
			Charset:       mysql.DEFAULT_CHARSET,
			Flavor:        mysql.MySQLFlavor,
			Heartbeat:     60,
			ReadTimeout:   90,
			ExecutionPath: mysql.DEFAULT_DUMP_EXECUTION_PATH,
		},
	}
}

// Init initializes the component
func (x *MySqlCDC) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if x.Config.Limit < 0 {
		x.Config.Limit = 0
	}
	x.RuleConfig = ruleConfig
	return err
}

// Destroy releases resources
func (x *MySqlCDC) Destroy() {
	_ = x.Close()
}

// Desc returns the component description
func (x *MySqlCDC) Desc() string {
	return "MySQL CDC endpoint for capturing database changes via binlog replication"
}

// Category returns the component category
func (x *MySqlCDC) Category() string {
	return "endpoint"
}

func (x *MySqlCDC) Def() types.ComponentForm {
	return types.ComponentForm{
		Desc: "MySQL CDC endpoint for capturing database row-level changes via binlog replication",
		RouterForm: &types.RouterForm{
			From: &types.RouterFormField{
				Path: types.ComponentFormField{
					Name:     "path",
					Type:     "string",
					Label:    "Table Pattern",
					Desc:     "MySQL table identifier to watch, e.g. mydb.users or regex pattern ^mydb\\..*, use * for all tables",
					Required: true,
				},
			},
		},
	}
}

func (x *MySqlCDC) Close() error {
	x.BaseEndpoint.Destroy()
	if x.canal != nil {
		x.canal.Close()
	}
	return nil
}
func (x *MySqlCDC) GetDefaultConfig(newConfig Config) *canal.Config {
	c := canal.NewDefaultConfig()
	if newConfig.Server != "" {
		c.Addr = newConfig.Server
	}
	if newConfig.User != "" {
		c.User = newConfig.User
	}
	if newConfig.Password != "" {
		c.Password = newConfig.Password
	}
	if newConfig.Charset != "" {
		c.Charset = newConfig.Charset
	}
	if newConfig.Flavor != "" {
		c.Flavor = newConfig.Flavor
	}
	if newConfig.Heartbeat != 0 {
		c.HeartbeatPeriod = time.Duration(newConfig.Heartbeat) * time.Second
	}
	if newConfig.ReadTimeout != 0 {
		c.ReadTimeout = time.Duration(newConfig.ReadTimeout) * time.Second
	}
	c.IncludeTableRegex = newConfig.IncludeTables
	c.ExcludeTableRegex = newConfig.ExcludeTables
	c.Dump.Databases = newConfig.Dbs

	if c.Dump.ExecutionPath == "" {
		c.Dump.ExecutionPath = mysql.DEFAULT_DUMP_EXECUTION_PATH
	}
	return c
}
func (x *MySqlCDC) Start() error {
	config := x.Config
	cfg := x.GetDefaultConfig(x.Config)

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return err
	}
	x.canal = c

	// Register a handler to handle RowsEvent
	c.SetEventHandler(&EventHandler{
		endpoint: x,
		name:     config.Server + "-handler",
		config:   x.Config,
	})
	if config.FromOldest {
		go func() {
			err2 := c.Run()
			if err2 != nil {
				x.Printf("Run canal error: %s", err2.Error())
			}
		}()
	} else {
		post, err := c.GetMasterPos()
		if err != nil {
			return err
		}
		go func() {
			err2 := c.RunFrom(post)
			if err2 != nil {
				x.Printf("RunFrom canal error: %s", err2.Error())
			}
		}()
	}

	return nil
}

func (x *MySqlCDC) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}

func (x *MySqlCDC) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	if router == nil {
		return "", errors.New("router can not nil")
	} else {
		expr := strings.TrimSpace(router.GetFrom().ToString())
		if expr == "" {
			expr = MatchAll
		}
		//Allow empty expr, indicating matching all items
		var regexpV *regexp.Regexp
		if expr != "" && expr != MatchAll && strings.HasPrefix(expr, "^") {
			//Compiling expressions
			if re, err := regexp.Compile(expr); err != nil {
				return "", err
			} else {
				regexpV = re
			}
		}

		x.CheckAndSetRouterId(router)
		x.Lock()
		defer x.Unlock()
		if x.routers == nil {
			x.routers = make(map[string]*RegexpRouter)
		}
		if _, ok := x.routers[router.GetId()]; ok {
			return router.GetId(), fmt.Errorf("duplicate router %s", expr)
		} else {
			x.routers[router.GetId()] = &RegexpRouter{
				router: router,
				regexp: regexpV,
				path:   expr,
			}
			return router.GetId(), nil
		}

	}
}
func (x *MySqlCDC) RemoveRouter(routerId string, params ...interface{}) error {
	x.Lock()
	defer x.Unlock()
	if x.routers != nil {
		if _, ok := x.routers[routerId]; ok {
			delete(x.routers, routerId)
		} else {
			return fmt.Errorf("router: %s not found", routerId)
		}
	}
	return nil
}

// RegexpRouter is a regular expression for routing
type RegexpRouter struct {
	//Route ID
	id string
	//Route
	router endpointApi.Router
	//Regular expression
	regexp *regexp.Regexp
	path   string
}
type EventHandler struct {
	canal.DummyEventHandler
	name     string
	endpoint *MySqlCDC
	config   Config
}

func (h *EventHandler) OnRow(e *canal.RowsEvent) error {
	if h.config.Limit > 0 {
		length := len(e.Rows)
		if e.Action == ActionUpdate {
			length = length / 2
		}
		if length > h.config.Limit {
			return nil
		}
	}
	b, err := json.Marshal(e.Rows)
	if err != nil {
		h.endpoint.Printf("OnRow json marshal error: %s", err.Error())
		return nil
	}
	//h.endpoint.Printf("OnRow action:%s, table:%s,rows: %s", e.Table.String(), e.Action, string(b))
	// Create an exchange object to store input and output messages
	exchange := &endpoint.Exchange{
		In: &RequestMessage{
			Table:  e.Table,
			Action: e.Action,
			Header: e.Header,
			body:   b,
		},
		Out: &ResponseMessage{
			Table:  e.Table,
			Action: e.Action,
		}}

	// Matching the matching routes and processing messages
	for _, v := range h.endpoint.routers {
		if e.Table != nil {
			tableNameStr := e.Table.String()
			if v.path == MatchAll || v.path == tableNameStr || (v.regexp != nil && v.regexp.Match([]byte(tableNameStr))) {
				h.endpoint.DoProcess(context.Background(), v.router, exchange)
			}
		}
	}
	return nil
}

func (h *EventHandler) String() string {
	return h.name
}
