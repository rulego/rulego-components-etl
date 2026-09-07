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
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/test/assert"
)

func newCDCTestEndpoint(t *testing.T, locker types.Locker) *MySqlCDC {
	t.Helper()
	x := &MySqlCDC{}
	configuration := types.Configuration{
		"server": "127.0.0.1:1",
		types.NodeConfigurationKeyRuleChainDefinition: &types.RuleChain{
			RuleChain: types.RuleChainBaseInfo{ID: "election-chain"},
		},
	}
	assert.Nil(t, x.Init(types.Config{Locker: locker}, configuration))
	return x
}

// TestMySqlCDCSingleInstanceStartSyncError 单实例（无 Locker）保持既有语义：
// Start 同步启动，MySQL 不可达直接报错。
func TestMySqlCDCSingleInstanceStartSyncError(t *testing.T) {
	x := newCDCTestEndpoint(t, nil)
	defer x.Destroy()
	if err := x.Start(); err == nil {
		t.Fatal("expected Start to fail synchronously without MySQL")
	}
}

// TestMySqlCDCStandbyDoesNotStart 预占租约模拟远端主副本：本实例保持待命，
// 不启动 binlog 读取；激活尝试持续失败也不影响进程稳定。
func TestMySqlCDCStandbyDoesNotStart(t *testing.T) {
	locker := types.NewLocalLocker()
	x := newCDCTestEndpoint(t, locker)
	defer x.Destroy()

	// 锁键必须与 Init 内部构造完全一致
	key := "rulego:active:" + types.OnceScope(Type, "", "election-chain", base.ConfigKey(x.Config))
	_, ok, err := locker.TryLock(context.Background(), key, time.Second)
	assert.Nil(t, err)
	assert.True(t, ok)

	assert.Nil(t, x.Start())
	time.Sleep(500 * time.Millisecond)
	assert.False(t, x.guard.IsActive())
	assert.True(t, x.canal == nil)
}

// TestMySqlCDCElectionInstanceStartAsync 多副本部署下 Start 异步返回：
// 激活失败（MySQL 不可达）不阻塞部署，由守卫在后台持续重试。
func TestMySqlCDCElectionInstanceStartAsync(t *testing.T) {
	locker := types.NewLocalLocker()
	x := newCDCTestEndpoint(t, locker)
	defer x.Destroy()
	assert.Nil(t, x.Start())
	time.Sleep(300 * time.Millisecond)
	assert.True(t, x.canal == nil)
}
