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

package applier

import (
	"context"

	log "github.com/Sirupsen/logrus"
)

const (
	CheckpointTablePrefix = "drc_ckt_"
)

var (
	CheckpointDatabase = ""
	CheckpointTable    = ""
)

type ApplierServer struct {
	fetcher     *Fetcher
	coordinator *Coordinator
	config      *ApplierConfig

	// 重试事务信号
	retryC chan string
	// 跳过事务信号
	skipC chan string

	// 通知内部协程退出
	cancel context.CancelFunc
	// 内部协程已退出信号
	quitC   chan struct{}
	started bool
}

type ApplierConfig struct {
	KafkaBrokerList string
	KafkaTopic      string
	KafkaPartition  int
	KafkaVersion    string
	KafkaOffset     int64
	// MySQL 配置
	MysqlHost           string
	MysqlPort           int
	MysqlUser           string
	MysqlPasswd         string
	MysqlCharset        string
	MysqlConnectTimeout int
	MysqlReadTimeout    int
	MysqlWriteTimeout   int
	MysqlWaitTimeout    int
	// 冲突处理
	HandleConflictStrategy string
	//时间冲突处理比较字段
	UpdateTimeColumn string
	// 是否允许DDL
	AllowDDL bool
	// 最大消费速率, kb/s
	MaxRate int
	// 乱序pkg数量累计一定程度后的告警阈值
	IncontinuousRdpPkgThreshold int
	// 慢事务的时间阈值
	SlowTrxThreshold int
}

func (cfg *ApplierConfig) CheckArgs() {
	if len(cfg.KafkaBrokerList) == 0 || len(cfg.KafkaTopic) == 0 ||
		cfg.KafkaPartition < 0 || len(cfg.KafkaVersion) == 0 ||
		cfg.KafkaOffset < -2 {
		log.Fatalf("ApplierConfig.CheckArgs:kafka args error, KafkaBrokerList=%s,KafkaTopic=%s,KafkaPartition=%d,"+
			"KafkaVersion=%s,KafkaOffset=%d", cfg.KafkaBrokerList, cfg.KafkaTopic, cfg.KafkaPartition,
			cfg.KafkaVersion, cfg.KafkaOffset)
	}

	if len(cfg.MysqlHost) == 0 || cfg.MysqlPort <= 0 ||
		len(cfg.MysqlUser) == 0 || cfg.MysqlConnectTimeout <= 0 ||
		cfg.MysqlWaitTimeout <= 0 || len(cfg.MysqlCharset) == 0 {
		log.Fatalf("ApplierConfig.CheckArgs:mysql args error, MysqlHost=%s,MysqlPort=%d,MysqlUser=%s,"+
			"MysqlConnectTimeout=%d,MysqlWaitTimeout=%d,MysqlCharset=%s", cfg.MysqlHost, cfg.MysqlPort, cfg.MysqlUser,
			cfg.MysqlConnectTimeout, cfg.MysqlWaitTimeout, cfg.MysqlCharset)
	}

	if cfg.HandleConflictStrategy != TimeOverwriteStrategy && cfg.HandleConflictStrategy != TimeIgnoreStrategy &&
		cfg.HandleConflictStrategy != IgnoreStrategy && cfg.HandleConflictStrategy != OverwriteStrategy {
		log.Fatalf("ApplierConfig.CheckArgs: handle conflict strategy error,HandleConflictStrategy=%s",
			cfg.HandleConflictStrategy)
	}

	if cfg.HandleConflictStrategy == TimeOverwriteStrategy || cfg.HandleConflictStrategy == TimeIgnoreStrategy {
		if len(cfg.UpdateTimeColumn) == 0 {
			log.Fatalf("ApplierConfig.CheckArgs: handle conflict based on time,but UpdateTimeColumn is emptys")
		}
	}
}

func NewApplierServer(config *ApplierConfig) *ApplierServer {
	config.CheckArgs()
	return &ApplierServer{
		config: config,
		quitC:  make(chan struct{}),
		retryC: make(chan string),
		skipC:  make(chan string),
	}
}

func (o *ApplierServer) RetryTrx(gtid string) {
	if o.started {
		o.retryC <- gtid
	}
}

func (o *ApplierServer) SkipTrx(gtid string) {
	if o.started {
		o.skipC <- gtid
	}
}

func (o *ApplierServer) QuitNotify() <-chan struct{} {
	return o.quitC
}

func (o *ApplierServer) Start() error {
	var err error

	defer func() {
		if err == nil {
			return
		}
		if o.fetcher != nil {
			o.fetcher.Stop()
		}
		if o.coordinator != nil {
			o.coordinator.Stop()
		}
	}()

	o.fetcher = NewFetcher(o.config)
	err = o.fetcher.Start()
	if err != nil {
		return err
	}

	o.coordinator = NewCoordinator(o.config, o.fetcher.NewTrxNotify(), o.retryC, o.skipC)
	err = o.coordinator.Start()
	if err != nil {
		return err
	}

	var ctx context.Context
	ctx, o.cancel = context.WithCancel(context.Background())
	go o.run(ctx)

	o.started = true

	return nil
}

func (o *ApplierServer) Stop() error {
	if !o.started {
		return nil
	}

	// 通知协程退出
	o.cancel()
	// 等待协程退出
	<-o.quitC

	if o.fetcher != nil {
		o.fetcher.Stop()
	}
	if o.coordinator != nil {
		o.coordinator.Stop()
	}

	o.started = false
	log.Warnf("ApplierServer.Stop: stop succeed")
	return nil
}

func (o *ApplierServer) Retry(gtid string) {

}

func (o *ApplierServer) run(ctx context.Context) {
	// 通知外界已经退出
	defer close(o.quitC)

	// 等待fetcher或coordinator退出
	select {
	case <-o.fetcher.QuitNotify():
		log.Warnf("ApplierServer.Run: fetcher quited")
	case <-o.coordinator.QuitNotify():
		log.Warnf("ApplierServer.Run: coordinator quited")
	case <-ctx.Done():
		log.Warnf("ApplierServer.Run: ctx canceled, need to quit")
	}
}

func SetCheckpointDBandTableName(groupId string) error {
	if len(groupId) == 0 {
		return ErrArgsIsNil
	}
	CheckpointTable = CheckpointTablePrefix + groupId
	CheckpointDatabase = "drc_checkpoint_db"
	return nil
}
