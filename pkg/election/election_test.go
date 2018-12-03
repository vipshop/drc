// Copyright 2018 vip.com.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package election

import (
	"path"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/require"
	"github.com/vipshop/drc/pkg/utils"
)

func TestBecomeLeaderImmediately(t *testing.T) {
	var err error
	zkAddrs := []string{"10.10.10.1:2181"}
	zkRoot := "/mysql_applier"
	config := &ElectionConfig{
		ZkAddrs:         zkAddrs,
		ZkRoot:          zkRoot,
		ElectionTimeout: 10,
	}

	election := NewElection(config)
	err = election.Start()
	require.Nil(t, err)
	defer election.Stop()

	// 第一次通知是进入follower模式
	<-election.ChangeNotify()
	require.False(t, election.IsLeader())

	<-election.ChangeNotify()
	require.True(t, election.IsLeader())

}

func TestBecomeLeaderLater(t *testing.T) {
	var err error
	zkAddrs := []string{"10.10.10.1:2181", "10.10.10.2:2181", "10.10.10.3:2181"}
	zkRoot := "/unit_test_mysql_applier"

	// 建立与zk的连接
	zkConn, _, err := zk.Connect(zkAddrs, 4*time.Second)
	require.Nil(t, err)
	defer func() {
		if zkConn != nil {
			zkConn.Close()
		}
	}()

	// 抢先建立临时节点
	nodesPath := path.Join(zkRoot, "nodes")
	err = utils.EnsurePath(zkConn, nodesPath, []byte(""))
	require.Nil(t, err)
	_, err = utils.CreateEphemeralSequential(zkConn, nodesPath)
	require.Nil(t, err)

	// 初始化election模块
	config := &ElectionConfig{
		ZkAddrs:         zkAddrs,
		ZkRoot:          zkRoot,
		ElectionTimeout: 10,
	}
	election := NewElection(config)
	err = election.Start()
	require.Nil(t, err)
	defer election.Stop()

	// 第一次通知是进入follower模式
	<-election.ChangeNotify()
	require.False(t, election.IsLeader())

	ticker := time.NewTicker(5 * time.Second)
	select {
	case <-election.ChangeNotify():
		// 不应该进入该分支
		require.True(t, false)
	case <-ticker.C:
		// 应该进入该分支
		// 关闭zk连接
		zkConn.Close()
		zkConn = nil
	}

	<-election.ChangeNotify()
	require.True(t, election.IsLeader())

}
