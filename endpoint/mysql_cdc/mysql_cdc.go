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
	"time"
)

// Type 组件类型
const Type = types.EndpointTypePrefix + "mysql_cdc"

const (
	KeyTableSchema   = "tableSchema"
	KeyTableName     = "tableName"
	KeyTableFullName = "tableFullName"
	KeyAction        = "action"
	KeyColumnNames   = "columnNames"
	KeyPkColumnNames = "pkColumnNames"
	KeyLogPos        = "logPos"
	// MatchAll 匹配所有数据
	MatchAll = "*"
)

// Endpoint 别名
type Endpoint = MySqlCDC

var _ endpointApi.Endpoint = (*Endpoint)(nil)

// 注册组件
func init() {
	_ = endpoint.Registry.Register(&Endpoint{})
}

// RequestMessage 请求消息
type RequestMessage struct {
	Table *schema.Table
	//insert/update/delete
	Action string
	// Header can be used to inspect the event
	Header *replication.EventHeader
	// 数据结构[][]interface{} ,如果是更新时间，格式： [更新前行数据, 更新后行数据]
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
		//默认指定是JSON格式，如果不是该类型，请在process函数中修改
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

// ResponseMessage http响应消息
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
	// mysql服务器地址
	Server string
	//用户名
	User string
	// 密码
	Password string
	//FromOldest 是否从最旧binlog同步，否则从最新的binlog和位置同步
	FromOldest bool
	// 数据库
	Dbs []string
	// IncludeTables or ExcludeTables should contain database name.
	// IncludeTables defines the tables that will be included, if empty, all tables will be included.
	// ExcludeTables defines the tables that will be excluded from the ones defined by IncludeTables.
	// Only a table which matches IncludeTables and dismatches ExcludeTables will be processed
	// eg, IncludeTables : [".*\\.canal","test.*"], ExcludeTables : ["mysql\\..*"]
	//     this will include all database's 'canal' table, except database 'mysql'.
	// Default IncludeTables and ExcludeTables are empty, this will include all tables
	IncludeTables []string
	ExcludeTables []string

	// mysqldump execution path, like mysqldump or /usr/bin/mysqldump, etc...
	// If not set, ignore using mysqldump.
	ExecutionPath string
	//字符集
	Charset string
	//mysql or mariadb
	Flavor string
	//心跳单位秒
	Heartbeat int
	// 读超时单位秒
	ReadTimeout int
}

// MySqlCDC 接收端端点
type MySqlCDC struct {
	impl.BaseEndpoint
	RuleConfig types.Config
	//Config 配置
	Config Config
	// 路由映射表
	routers map[string]*RegexpRouter
	canal   *canal.Canal
}

// Type 组件类型
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

// Init 初始化
func (x *MySqlCDC) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	x.RuleConfig = ruleConfig
	return err
}

// Destroy 销毁
func (x *MySqlCDC) Destroy() {
	_ = x.Close()
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
		expr := router.GetFrom().ToString()
		//允许空expr，表示匹配所有
		var regexpV *regexp.Regexp
		if expr != "" && expr != MatchAll {
			//编译表达式
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

// RegexpRouter 正则表达式路由
type RegexpRouter struct {
	//路由ID
	id string
	//路由
	router endpointApi.Router
	//正则表达式
	regexp *regexp.Regexp
}
type EventHandler struct {
	canal.DummyEventHandler
	name     string
	endpoint *MySqlCDC
}

func (h *EventHandler) OnRow(e *canal.RowsEvent) error {
	b, err := json.Marshal(e.Rows)
	if err != nil {
		h.endpoint.Printf("OnRow json marshal error: %s", err.Error())
		return nil
	}
	//h.endpoint.Printf("OnRow action:%s, table:%s,rows: %s", e.Table.String(), e.Action, string(b))
	// 创建一个交换对象，用于存储输入和输出的消息
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

	// 匹配符合的路由，处理消息
	for _, v := range h.endpoint.routers {
		if v.regexp == nil || e.Table == nil || v.regexp.Match([]byte(e.Table.String())) {
			h.endpoint.DoProcess(context.Background(), v.router, exchange)
		}
	}
	return nil
}

func (h *EventHandler) String() string {
	return h.name
}
