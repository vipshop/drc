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
	"encoding/json"
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/siddontang/go/hack"
	"github.com/vipshop/drc/pkg/utils"
	"golang.org/x/time/rate"
	"math"
	"strings"
	"time"
)

type Fetcher struct {
	config *ApplierConfig
	// 通知外界有新trx
	trxC chan *Trx
	msgC chan *sarama.ConsumerMessage
	// Kafka连接
	consumer          sarama.Consumer
	partitionConsumer sarama.PartitionConsumer
	// 通知内部协程退出
	cancel context.CancelFunc
	// 内部协程已退出信号
	quitC   chan struct{}
	started bool
}

func NewFetcher(config *ApplierConfig) *Fetcher {
	return &Fetcher{
		config: config,
		trxC:   make(chan *Trx, 3000),
		msgC:   make(chan *sarama.ConsumerMessage, 3000),
		quitC:  make(chan struct{}),
	}
}

func (o *Fetcher) NewTrxNotify() <-chan *Trx {
	return o.trxC
}
func (o *Fetcher) QuitNotify() <-chan struct{} {
	return o.quitC
}

func (o *Fetcher) Start() error {
	var err error
	if o.started {
		return nil
	}

	defer func() {
		if err == nil {
			return
		}
		if o.consumer != nil {
			o.consumer.Close()
		}
		if o.partitionConsumer != nil {
			o.partitionConsumer.Close()
		}
	}()

	// 设置sarama的日志
	sarama.Logger = log.StandardLogger()

	// 初始化consumer对象
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version, err = sarama.ParseKafkaVersion(o.config.KafkaVersion)
	if err != nil {
		log.Errorf("Fetcher.Start: ParseKafkaVersion error: %s", err)
		return err
	}

	log.Infof("Fetcher.Start: kafka brokerlist: %s, topic: %s, partition: %d, offset: %d",
		o.config.KafkaBrokerList, o.config.KafkaTopic, o.config.KafkaPartition, o.config.KafkaOffset)
	o.consumer, err = sarama.NewConsumer(strings.Split(o.config.KafkaBrokerList, ","), kafkaConfig)
	if err != nil {
		log.Errorf("Fetcher.Start: NewConsumer error: %s", err)
		return err
	}

	o.partitionConsumer, err = o.consumer.ConsumePartition(o.config.KafkaTopic, 0, o.config.KafkaOffset)
	if err != nil {
		log.Errorf("Fetcher.Start: ConsumePartition error: %s", err)
		return err
	}

	var ctx context.Context
	ctx, o.cancel = context.WithCancel(context.Background())
	go o.run(ctx)

	o.started = true

	return nil
}

func (o *Fetcher) Stop() error {
	if !o.started {
		return nil
	}

	// 通知协程退出
	o.cancel()
	// 等待协程退出
	<-o.quitC

	if o.consumer != nil {
		o.consumer.Close()
	}
	if o.partitionConsumer != nil {
		o.partitionConsumer.Close()
	}

	o.started = false
	log.Warnf("Fetcher.Stop: stop succeed")
	return nil
}

// 判断是否需要过滤
func (o *Fetcher) needFilter(trx *Trx) bool {
	if trx.Gtid == "" {
		// 过滤FDE, rotate等非事务数据
		return true
	}

	if len(trx.Events) == 0 {
		log.Panicf("Fetcher.needFilter: empty trx: %s", trx.Gtid)
	}
	if trx.Events[0].DatabaseName == CheckpointDatabase &&
		strings.HasPrefix(trx.Events[0].TableName, CheckpointTablePrefix) {
		// 产生自DRC的row event
		return true
	}

	if trx.Events[0].SqlStatement != "" {
		if strings.Contains(trx.Events[0].SqlStatement, "/*mysql-applier*/") {
			// 产生自DRC的statement event, 比如DDL
			return true
		}

		upperSql := strings.ToUpper(trx.Events[0].SqlStatement)
		if strings.HasPrefix(upperSql, "DROP") && !o.config.AllowDDL {
			// 该语句是DROP语句，无法区分是由applier产生还是由用户产生
			// 如果该方向不允许执行DDL，那么需要过滤掉该DDL
			return true
		}

		if strings.HasPrefix(upperSql, "CREATE USER") {
			// 过滤掉GRANT语句, 因为需要CREATE_USER权限
			log.Warnf("Fetcher.needFilter: filter a sql: %s", upperSql)
			return true
		}

		if strings.HasPrefix(upperSql, "GRANT") {
			// 过滤掉GRANT语句, 因为需要GRANT权限
			log.Warnf("Fetcher.needFilter: filter a sql: %s", upperSql)
			return true
		}

		if strings.HasPrefix(upperSql, "FLUSH") {
			// 过滤掉FLUSH xxx语句, 因为需要RELOAD权限
			log.Warnf("Fetcher.needFilter: filter a sql: %s", upperSql)
			return true
		}

		if strings.HasPrefix(upperSql, "CREATE DEFINER") ||
			strings.HasPrefix(upperSql, "CREATE TRIGGER") ||
			strings.HasPrefix(upperSql, "CREATE PROCEDURE") ||
			strings.HasPrefix(upperSql, "CREATE FUNCTION") {
			// 过滤掉创建触发器和存储过程
			log.Warnf("Fetcher.needFilter: filter a sql: %s", upperSql)
			utils.Alarm("fetcher filter a sql that creates trigger or procedure: " + upperSql)

			return true
		}

		if strings.HasPrefix(upperSql, "DROP TRIGGER") ||
			strings.HasPrefix(upperSql, "DROP PROCEDURE") ||
			strings.HasPrefix(upperSql, "DROP FUNCTION") {
			// 过滤掉删除触发器和存储过程
			log.Warnf("Fetcher.needFilter: filter a sql: %s", upperSql)
			return true
		}

	}
	return false
}

func (o *Fetcher) decode(ctx context.Context) {
	var err error
	decoder := NewRdpDecoder(o.config.IncontinuousRdpPkgThreshold)
	for {
		select {
		case msg := <-o.msgC:
			err = decoder.feed(msg.Value, msg.Topic, msg.Offset)
			if err != nil {
				log.Errorf("Fetcher.decode: feed msg error: %s, offset: %d", err, msg.Offset)
				return
			}
			if !decoder.canPeek() {
				// 如果没有完整的事务
				break
			}

			var trx *Trx
			trx, err = decoder.peek()
			if err != nil {
				log.Errorf("Fetcher.decode: peek trx error: %s, offset: %d", err, msg.Offset)
				utils.Alarm("fetcher peek trx error: " + err.Error())
				return
			}

			// 更新消费延时, 当设备之间没有时差问题时，该值才有参考意义
			secondsBehindMaster.Update(time.Now().Unix() - int64(trx.Timestamp))
			// 统计收到的事务数量
			fetchedTrxCount.Inc(1)

			if !o.needFilter(trx) {
				// 统计事务大小
				trxSizeHistogram.Update(trx.EndOffset - trx.BeginOffset + 1)

				select {
				case o.trxC <- trx:
				case <-ctx.Done():
					log.Infof("Fetcher.decode: ctx canceled, need to quit")
					return
				}
			}
		case <-ctx.Done():
			log.Infof("Fetcher.decode: ctx canceled, need to quit")
			return
		}
	}
}

func (o *Fetcher) run(ctx context.Context) {
	var err error
	// 通知外界已经退出
	defer close(o.quitC)

	go o.decode(ctx)

	rateLimiter := rate.NewLimiter(rate.Every(time.Second/time.Duration(o.config.MaxRate*1024)), math.MaxInt32)

	for {
		select {
		case msg := <-o.partitionConsumer.Messages():
			if msg == nil {
				log.Errorf("Fetcher.run: kafka consumer closed, need to quit")
				return
			}

			log.Debugf("Fetcher.run: consumed message offset %d", msg.Offset)
			// 进行限速
			err = rateLimiter.WaitN(ctx, len(msg.Value))
			if err != nil {
				log.Error("Fetcher.run: rate limiter wait failed: %s", err)
				return
			}
			select {
			case o.msgC <- msg:
			case <-ctx.Done():
				log.Infof("Fetcher.run: ctx canceled, need to quit")
				return
			}

		case <-ctx.Done():
			log.Infof("Fetcher.run: ctx canceled, need to quit")
			return
		}
	}
}

func dumpTrx(trx *Trx) string {
	buf, err := json.Marshal(trx)
	if err != nil {
		return ""
	}
	return hack.String(buf)
}
