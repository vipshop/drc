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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/vipshop/drc/pkg/api"
	"github.com/vipshop/drc/pkg/applier"
	"github.com/vipshop/drc/pkg/election"
	"github.com/vipshop/drc/pkg/metrics"
	"github.com/vipshop/drc/pkg/utils"
	"time"
)

var (
	gConfig        *Config = NewConfig()
	gBecomeLeaderC         = make(chan struct{})
)

func handleModeChange(election *election.Election) {
	isLeader := false
	for {
		<-election.ChangeNotify()
		if election.IsLeader() {
			if !isLeader {
				// 获得leader角色
				log.Warnf("handleModeChange: I become leader")
				isLeader = true
				// 通知开始工作
				gBecomeLeaderC <- struct{}{}
			}
		} else {
			if isLeader {
				// 丢失leader角色,立即退出, 防止多个双leader存在
				log.Panicf("handleModeChange: I lost leader role")
			}
		}
	}

}

func main() {
	var err error

	// 解析命令行参数
	configPath := flag.String("config", "../etc/applier.ini", "the config file of mysql applier ")
	kafkaOffset := flag.Int64("offset", utils.OffsetCheckpoint, "the kafka offset to comsume,-1:OffsetNewest,-2:OffsetOldest,-3:Checkpoint")
	printVersion := flag.Bool("version", false, "print applier version info")
	flag.Parse()

	if *printVersion {
		fmt.Printf("version is %s, build at %s\n", BuildVersion, BuildDate)
		return
	}

	//解析配置文件
	err = gConfig.Load(*configPath)
	if err != nil {
		log.Errorf("main: init config failed: %s", err)
		return
	}
	//设置checkpoint的DB名和表名
	//后续这两个变量只读，不会修改
	err = applier.SetCheckpointDBandTableName(gConfig.GroupId)
	if err != nil {
		log.Errorf("main: SetCheckpointDBandTableName failed: %s", err)
		return
	}
	//初始化日志
	err = InitLog(gConfig.LogDir, gConfig.LogLevel)
	if err != nil {
		log.Errorf("main: init log failed: %s", err)
		return
	}
	defer UninitLog()

	log.Infof("main: application started, version is %s, build at %s", BuildVersion, BuildDate)
	defer log.Warnf("main: application quited")

	ctx, cancel := context.WithCancel(context.Background())

	//优雅退出
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	go func() {
		for {
			sig := <-sc
			if sig == syscall.SIGINT || sig == syscall.SIGTERM || sig == syscall.SIGQUIT {
				// 通知server退出
				log.Warn("main: application received terminate signal from user")
				cancel()
			}
		}
	}()

	//创建API server
	adminServerCfg := api.AdminServerConfig{
		Addr: gConfig.AdminAddr,
	}
	adminServer := api.NewAdminServer(&adminServerCfg)
	go adminServer.Start()
	defer adminServer.Stop()

	//等待admin server成功后
	time.Sleep(time.Second * 2)
	select {
	case <-adminServer.QuitNotify():
		log.Warnf("main:amdin api server quited")
		return
	default:
		log.Info("main:admin server start successs")
	}

	// 初始化election模块
	electionConfig := &election.ElectionConfig{
		ZkAddrs:         strings.Split(gConfig.ZkAddrList, ","),
		ZkRoot:          gConfig.ZkRoot,
		ElectionTimeout: gConfig.ElectionTimeout,
	}
	election := election.NewElection(electionConfig)
	go handleModeChange(election)
	err = election.Start()
	if err != nil {
		log.Errorf("main: election start error: %s", err)
		return
	}
	defer election.Stop()

	// 等待变成leader
	select {
	case <-gBecomeLeaderC:
		log.Warnf("main: start to work")
	case <-ctx.Done():
		log.Warnf("main: ctx canceled, need to quit")
	}

	// 更新metric
	drcEpoch.Update(int64(election.GetEpoch()))

	if *kafkaOffset == utils.OffsetCheckpoint {
		//从checkpoint表中加载消费位置
		*kafkaOffset, err = getConsumeOffset()
		if err != nil {
			log.Errorf("main.getConsumeOffset error,err :%s", err)
			return
		}
	}
	// 初始化applier模块
	applierConfig := applier.ApplierConfig{
		KafkaBrokerList:             gConfig.KafkaBrokerList,
		KafkaTopic:                  gConfig.KafkaTopic,
		KafkaPartition:              gConfig.KafkaPartition,
		KafkaVersion:                gConfig.KafkaVersion,
		KafkaOffset:                 *kafkaOffset,
		MysqlHost:                   gConfig.MysqlHost,
		MysqlPort:                   gConfig.MysqlPort,
		MysqlUser:                   gConfig.MysqlUser,
		MysqlPasswd:                 gConfig.MysqlPasswd,
		MysqlCharset:                gConfig.MysqlCharset,
		MysqlConnectTimeout:         gConfig.MysqlConnectTimeout,
		MysqlReadTimeout:            gConfig.MysqlReadTimeout,
		MysqlWriteTimeout:           gConfig.MysqlWriteTimeout,
		MysqlWaitTimeout:            gConfig.MysqlWaitTimeout,
		HandleConflictStrategy:      gConfig.HandleConflictStrategy,
		UpdateTimeColumn:            applier.EscapeName(gConfig.UpdateTimeColumn),
		AllowDDL:                    gConfig.AllowDDL,
		MaxRate:                     gConfig.MaxRate,
		IncontinuousRdpPkgThreshold: gConfig.IncontinuousRdpPkgThreshold,
		SlowTrxThreshold:            gConfig.SlowTrxThreshold,
	}
	applierServer := applier.NewApplierServer(&applierConfig)
	err = applierServer.Start()
	if err != nil {
		log.Errorf("main: applier start error: %s", err)
		return
	}
	defer applierServer.Stop()

	//设置applierHandler
	adminServer.SetApplierHandler(applierServer)

	// 初始化metric上报模块
	metricsReporterConfig := metrics.ReporterConfig{
		Owner:       "drc",
		PublishType: gConfig.MetricPublishType,
		HostName:    gConfig.HostName,
		Endpoint:    "drc" + gConfig.GroupId,
		FileDir:     gConfig.MetricFileDir,
		FileName:    gConfig.MetricFileName,
		Url:         gConfig.MetricUrl,
		Timeout:     gConfig.MetricTimeout,
		Interval:    gConfig.MetricInterval,
	}
	metricsReporter := metrics.NewReporter(&metricsReporterConfig)
	err = metricsReporter.Start()
	if err != nil {
		log.Errorf("main: metrics reporter start error: %s", err)
		return
	}
	defer metricsReporter.Stop()

	select {
	case <-election.QuitNotify():
		log.Warnf("main: election quited")
	case <-applierServer.QuitNotify():
		log.Warnf("main: applierServer quited")
	//case <-reporter.QuitNotify():
	//	log.Warnf("main: checkpoint reporter quited")
	case <-metricsReporter.QuitNotify():
		log.Warnf("main: metrics reporter quited")
	case <-adminServer.QuitNotify():
		log.Warnf("main:amdin api server quited")
	case <-ctx.Done():
		log.Warnf("main: ctx canceled, need to quit")
	}

}

func getConsumeOffset() (int64, error) {
	tableExist, err := utils.CheckTableExist(gConfig.MysqlHost, gConfig.MysqlPort, gConfig.MysqlUser,
		gConfig.MysqlPasswd, applier.CheckpointDatabase, applier.CheckpointTable)
	if err != nil {
		return utils.OffsetCheckpoint, err
	}

	if !tableExist {
		//表不存在，从最旧位置开始消费
		return utils.OffsetOldest, nil
	}

	kafkaOffset, err := utils.GetConsumeOffset(gConfig.MysqlHost, gConfig.MysqlPort, gConfig.MysqlUser,
		gConfig.MysqlPasswd, applier.CheckpointDatabase, applier.CheckpointTable, gConfig.KafkaTopic)
	if err != nil {
		return utils.OffsetCheckpoint, err
	}

	return kafkaOffset, nil
}
