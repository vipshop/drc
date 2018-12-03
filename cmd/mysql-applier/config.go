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
	log "github.com/Sirupsen/logrus"
	"github.com/astaxie/beego/config"
)

type Config struct {
	// Applier group唯一表示
	GroupId string

	// Kafka 配置
	KafkaBrokerList string
	KafkaTopic      string
	KafkaPartition  int
	KafkaVersion    string

	//zk配置
	ZkAddrList string
	ZkRoot     string

	// 选主超时
	ElectionTimeout int

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

	//api server配置
	AdminAddr string

	// 是否允许DDL
	AllowDDL bool

	// 最大消费速率, kb/s
	MaxRate int

	// 特性数据相关
	HostName       string
	MetricInterval int
	// 特性数据发布的方式: 本地文件 或者 上报到falcon
	MetricPublishType string
	// 本地文件路径
	MetricFileDir  string
	MetricFileName string
	// falcon地址
	MetricUrl     string
	MetricTimeout int

	LogDir   string
	LogLevel string

	// 处理冲突的策略
	HandleConflictStrategy string

	// 检测冲突时依赖的update time字段
	UpdateTimeColumn string

	// 乱序pkg数量累计一定程度后的告警阈值
	// 例如，该值为1000，那么乱序pkg达到1000*n时(n=0,1,2...)，触发告警
	IncontinuousRdpPkgThreshold int

	// 慢事务的时间阈值
	SlowTrxThreshold int

	cnfPath  string
	innerCnf config.Configer
}

func NewConfig() *Config {
	cnf := &Config{}
	return cnf
}

func (o *Config) Load(cnfPath string) error {
	var err error
	o.cnfPath = cnfPath

	if o.cnfPath != "" {
		o.innerCnf, err = config.NewConfig("ini", o.cnfPath)
		if err != nil {
			log.Error(err)
			return err
		}
	} else {
		o.innerCnf = config.NewFakeConfig()
	}

	o.GroupId = o.innerCnf.String("group_id")
	o.HandleConflictStrategy = o.innerCnf.DefaultString("handle_conflict_strategy", "user")
	o.UpdateTimeColumn = o.innerCnf.DefaultString("update_time_column", "")

	o.AllowDDL = o.innerCnf.DefaultBool("allow_ddl", true)
	o.MaxRate = o.innerCnf.DefaultInt("max_rate", 40960)

	o.LogDir = o.innerCnf.DefaultString("log::dir", "../logs")
	o.LogLevel = o.innerCnf.DefaultString("log::level", "info")

	o.ZkAddrList = o.innerCnf.String("zk::zk_addr_list")
	o.ZkRoot = o.innerCnf.String("zk::zk_root")
	o.ElectionTimeout = o.innerCnf.DefaultInt("election_timeout", 40)

	o.KafkaBrokerList = o.innerCnf.String("kafka::brokerlist")
	o.KafkaTopic = o.innerCnf.String("kafka::topic")
	o.KafkaPartition, _ = o.innerCnf.Int("kafka::partition")
	o.KafkaVersion = o.innerCnf.DefaultString("kafka::version", "0.8.2.0")

	o.MysqlHost = o.innerCnf.DefaultString("mysql::host", "localhost")
	o.MysqlPort = o.innerCnf.DefaultInt("mysql::port", 3306)
	o.MysqlUser = o.innerCnf.DefaultString("mysql::user", "root")
	o.MysqlPasswd = o.innerCnf.DefaultString("mysql::passwd", "")
	o.MysqlConnectTimeout = o.innerCnf.DefaultInt("mysql::connect_timeout", 30)
	o.MysqlReadTimeout = o.innerCnf.DefaultInt("mysql::read_timeout", 10)
	o.MysqlWriteTimeout = o.innerCnf.DefaultInt("mysql::write_timeout", 10)
	o.MysqlWaitTimeout = o.innerCnf.DefaultInt("mysql::wait_timeout", 3600)
	o.MysqlCharset = o.innerCnf.DefaultString("mysql::charset", "utf8mb4")

	o.AdminAddr = o.innerCnf.String("admin::addr")

	o.HostName = o.innerCnf.DefaultString("host_name", "localhost")

	o.MetricPublishType = o.innerCnf.DefaultString("metric::publish_type", "file")
	o.MetricFileDir = o.innerCnf.DefaultString("metric::dir", "../metrics")
	o.MetricFileName = o.innerCnf.DefaultString("metric::name", "app.metric")
	o.MetricUrl = o.innerCnf.DefaultString("metric::url", "http://127.0.0.1:22230/v1/push")
	o.MetricTimeout = o.innerCnf.DefaultInt("metric::timeout", 5)
	o.MetricInterval = o.innerCnf.DefaultInt("metric::interval", 10)

	o.IncontinuousRdpPkgThreshold = o.innerCnf.DefaultInt("incontinuous_rdp_pkg_threshold", 1000)
	o.SlowTrxThreshold = o.innerCnf.DefaultInt("slow_trx_threshold", 30)

	return nil
}
