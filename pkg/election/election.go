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
	"context"
	log "github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/vipshop/drc/pkg/status"
	"github.com/vipshop/drc/pkg/utils"
	"path"
	"strconv"
	"sync"
	"time"
)

type WorkingMode string

const (
	modeChannelSize = 6
)

const (
	WorkingMode_INIT     WorkingMode = "init"
	WorkingMode_FOLLOWER             = "follower"
	WorkingMode_LEADER               = "leader"
	WorkingMode_UNSAFE               = "unsafe"
)

const (
	StatusKey_WorkMode = "work_mode"
)

type ElectionConfig struct {
	ZkAddrs         []string
	ZkRoot          string
	ElectionTimeout int
}

type Election struct {
	config   *ElectionConfig
	zkConn   *zk.Conn
	zkEventC <-chan zk.Event
	// mode变化时，通知外界
	modeC chan struct{}

	workingMode WorkingMode
	l           *sync.Mutex
	// 临时节点名字
	nodeName string

	// 通知内部协程退出
	cancel context.CancelFunc
	// 内部协程已退出信号
	quitC   chan struct{}
	started bool
}

func NewElection(config *ElectionConfig) *Election {
	return &Election{
		config:      config,
		workingMode: WorkingMode_INIT,
		modeC:       make(chan struct{}, modeChannelSize),
		l:           new(sync.Mutex),
		quitC:       make(chan struct{}),
	}
}

// 注册一些需要向其他模块暴露的status
func (o *Election) registerStatus() {
	func1 := func() interface{} {
		return o.getWorkingMode()
	}
	status.Register(StatusKey_WorkMode, func1)
}

func (o *Election) getWorkingMode() WorkingMode {
	o.l.Lock()
	defer o.l.Unlock()
	return o.workingMode
}

func (o *Election) setWorkingMode(mode WorkingMode) {
	o.l.Lock()
	defer o.l.Unlock()
	o.workingMode = mode
	select {
	case o.modeC <- struct{}{}:
	default:
		log.Warnf("Election.setWorkingMode: signal mode change would block")
	}
}

func (o *Election) IsLeader() bool {
	mode := o.getWorkingMode()
	return mode == WorkingMode_LEADER
}

func (o *Election) ChangeNotify() <-chan struct{} {
	return o.modeC
}

func (o *Election) handleSessionEvent(event zk.Event) {
	switch event.State {
	case zk.StateHasSession:
		if o.getWorkingMode() == WorkingMode_INIT {
			// 初次建立session成功, 进入follower模式
			log.Warnf("Election.handleSessionEvent: I enter follower mode!")
			o.setWorkingMode(WorkingMode_FOLLOWER)
		} else if o.getWorkingMode() == WorkingMode_UNSAFE {
			// 重连成功，重新进入leader模式
			log.Warnf("Election.handleSessionEvent: I leave unsafe mode!")
			o.setWorkingMode(WorkingMode_LEADER)
		}
	case zk.StateDisconnected:
		if o.getWorkingMode() == WorkingMode_LEADER {
			// 与zk的TCP连接已经超时, 已经超过了2/3*seesion timeout无响应
			// 处于临界状态，session即将超时，建议应用立即退出进程, 防止双leader存在
			log.Warnf("Election.handleSessionEvent: I enter unsafe mode!")
			o.setWorkingMode(WorkingMode_UNSAFE)
		}
	case zk.StateExpired:
		// session超时，无法恢复session，需要退出程序
		log.Panicf("Election.handleSessionEvent: zookeeper session expired")

	default:
		// 不关心其他类型的event
		log.Infof("Election.handleSessionEvent: not need to handle event: %v", event)

	}
}

func (o *Election) handleChildrenEvent(event zk.Event) {
	// 启动携程异步处理，防止阻塞其他event
	go func(path string) {
		var children []string
		for tryTimes := 0; ; tryTimes++ {
			var err error
			children, _, _, err = o.zkConn.ChildrenW(path)
			if err != nil {
				log.Errorf("Election.handleChildrenEvent: get zookeeper children error: %s", err)
				if tryTimes >= 60 {
					// 重试多次仍然失败，处于未决状态，需要panic
					log.Panic("Election.handleChildrenEvent: get zookeeper children failed after 60 tries")
				}
				// 重试
				time.Sleep(time.Second)
				continue
			}

			minSeq := o.getNodeSeq(children[0])
			mySeq := o.getNodeSeq(o.nodeName)
			for i := 1; i < len(children); i++ {
				seq := o.getNodeSeq(children[i])
				if seq <= minSeq {
					minSeq = seq
				}
			}
			log.Infof("Election.handleChildrenEvent: min seq: %d, my seq: %d", minSeq, mySeq)

			if minSeq == mySeq {
				if !o.IsLeader() {
					log.Warnf("Election.handleChildrenEvent: I enter leader mode")
					o.setWorkingMode(WorkingMode_LEADER)
				}
			} else {
				if o.IsLeader() {
					// 本节点是leader，但是最小编号不是本节点，属于异常情况
					log.Panicf("Election.handleChildrenEvent: Become follower unexpectly, abort now")
				}
			}
			break
		} // for
	}(event.Path)
}

func (o *Election) run(ctx context.Context) {
	// var err error
	defer close(o.quitC)

	for {
		select {
		case event := <-o.zkEventC:
			log.Infof("Election.run: zookeeper event %v", event)
			switch event.Type {
			case zk.EventSession:
				// session状态相关event
				o.handleSessionEvent(event)

			case zk.EventNodeChildrenChanged:
				// children节点变更event
				o.handleChildrenEvent(event)

			default:
				// 不关心其他类型的event
			}
		case <-ctx.Done():
			log.Warnf("Election.run: ctx cancel, need to quit")
			return
		}
	}
}

// 获取zk临时节点的index
func (o *Election) getNodeSeq(name string) int {
	// 获取最后10个字符,zk临时节点的序号格式是%010d, 类型是signed int
	seqStr := name[len(name)-10:]
	seq, err := strconv.Atoi(seqStr)
	if err != nil || seq < 0 {
		// 正常情况下不可能会解析失败, 如果失败，那么panic
		log.Panicf("Election.getNodeIndex: impossible err: ", err)
	}
	return seq
}

func (o *Election) GetEpoch() int {
	if o.nodeName == "" {
		return -1
	}
	return o.getNodeSeq(o.nodeName)
}

func (o *Election) QuitNotify() <-chan struct{} {
	return o.quitC
}

func (o *Election) Start() error {
	var err error
	defer func() {
		if err == nil {
			return
		}
		if o.zkConn != nil {
			o.zkConn.Close()
		}
	}()

	// 建立与zk的连接
	zkOpt := zk.WithLogger(log.StandardLogger())
	o.zkConn, o.zkEventC, err = zk.Connect(o.config.ZkAddrs,
		time.Duration(o.config.ElectionTimeout)*time.Second, zkOpt)
	if err != nil {
		log.Errorf("Election.Start: error: %s, addrs: %v", err, o.config.ZkAddrs)
		return err
	}

	// 检查nodes相关的目录是否存在，不存在则创建
	nodesPath := path.Join(o.config.ZkRoot, "nodes")
	err = utils.EnsurePath(o.zkConn, nodesPath, []byte(""))
	if err != nil {
		log.Errorf("Election.Start: EnsurePath error: %s, path: %s", err, nodesPath)
		return err
	}

	// 对nodes节点进行watch
	_, _, _, err = o.zkConn.ChildrenW(nodesPath)
	if err != nil {
		log.Errorf("Election.Start: ChildrenW error: %s", err)
		return err
	}

	// 创建node来触发event
	o.nodeName, err = utils.CreateEphemeralSequential(o.zkConn, nodesPath)
	if err != nil {
		log.Errorf("Election.Start: createEphemeralSequential error: %s", err)
		return err
	}

	log.Infof("Election.Start: created ephemeral node: %s", o.nodeName)

	// 注册status
	o.registerStatus()

	var ctx context.Context
	ctx, o.cancel = context.WithCancel(context.Background())
	go o.run(ctx)

	o.started = true

	return nil

}

func (o *Election) Stop() error {
	if !o.started {
		return nil
	}

	// 通知协程退出
	o.cancel()
	// 等待协程退出
	<-o.quitC

	if o.zkConn != nil {
		o.zkConn.Close()
	}

	log.Warnf("Election.Stop: stop succeed")
	o.started = false
	return nil
}
