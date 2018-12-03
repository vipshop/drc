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

package metrics

import (
	"context"
	"errors"

	"time"

	log "github.com/Sirupsen/logrus"
	gometrics "github.com/rcrowley/go-metrics"
)

const (
	PublishType_File   = "file"
	PublishType_Falcon = "falcon"
)

type ReporterConfig struct {
	// 命名空间
	Owner string

	// 设备hostname
	HostName string

	// 实例id
	Endpoint string

	// 上报metric间隔, 以秒为单位
	Interval int

	// 上报metric的目标类型: file或者falcon
	PublishType string

	// 数据文件保存位置
	FileDir  string
	FileName string

	Url     string
	Timeout int
}

type Reporter struct {
	config *ReporterConfig
	reg    gometrics.Registry

	publisher Publisher

	// 通知内部协程退出
	cancel context.CancelFunc
	// 内部协程已退出信号
	quitC   chan struct{}
	started bool
}

func NewCounter() gometrics.Counter {
	return gometrics.NewCounter()
}

func NewGauge() gometrics.Gauge {
	return gometrics.NewGauge()
}

func NewMeter() gometrics.Meter {
	return gometrics.NewMeter()
}

func NewHistogram(reservoirSize int) gometrics.Histogram {
	return gometrics.NewHistogram(NewUniformSample(reservoirSize))
}

func Register(name string, metric interface{}) error {
	return gometrics.Register(name, metric)
}

// Metric的数据格式
type DrcMetric struct {
	Owner    string `json:"owner"`
	HostName string `json:"hostname"`

	Metric   string `json:"metric"`
	Endpoint string `json:"endpoint"`

	Timestamp int64 `json:"timestamp"`
	Value     int64 `json:"value"`

	// 上报间隔
	Interval int `json:"interval"`
}

func NewReporter(config *ReporterConfig) *Reporter {
	return &Reporter{
		config: config,
		reg:    gometrics.DefaultRegistry,
		quitC:  make(chan struct{}),
	}
}

func (o *Reporter) NewDrcMetric(metricName string, t time.Time, value int64) *DrcMetric {
	var timestamp int64 = t.UnixNano() / int64(time.Millisecond)
	return &DrcMetric{
		Owner:    o.config.Owner,
		HostName: o.config.HostName,

		Metric:    metricName,
		Endpoint:  o.config.Endpoint,
		Timestamp: timestamp,
		Value:     value,

		Interval: o.config.Interval,
	}

}

func (o *Reporter) publishMetrics() {
	var drcMetrics []*DrcMetric

	o.reg.Each(func(name string, i interface{}) {
		now := time.Now()
		switch metric := i.(type) {
		case gometrics.Counter:
			// 计数类型
			ms := metric.Snapshot()
			drcMetrics = append(drcMetrics, o.NewDrcMetric(name, now, ms.Count()))

		case gometrics.Gauge:
			// 瞬时值类型
			ms := metric.Snapshot()
			drcMetrics = append(drcMetrics, o.NewDrcMetric(name, now, ms.Value()))

		case gometrics.Meter:
			// 速率类型
			ms := metric.Snapshot()
			// 获取10s内的移动平均速率
			drcMetrics = append(drcMetrics, o.NewDrcMetric(name, now, int64(ms.Rate10s())))

		case gometrics.Histogram:
			// 直方图类型
			ms := metric.Snapshot()
			drcMetrics = append(drcMetrics, o.NewDrcMetric(name+".min", now, int64(ms.Min())))
			drcMetrics = append(drcMetrics, o.NewDrcMetric(name+".max", now, int64(ms.Max())))
			drcMetrics = append(drcMetrics, o.NewDrcMetric(name+".mean", now, int64(ms.Mean())))
			drcMetrics = append(drcMetrics, o.NewDrcMetric(name+".p50", now, int64(ms.Percentile(0.5))))
			drcMetrics = append(drcMetrics, o.NewDrcMetric(name+".p99", now, int64(ms.Percentile(0.99))))

		default:
			log.Warnf("Reporter.publishMetrics: unsupported metric type: %v", metric)
		}
	})

	o.publisher.Publish(drcMetrics)

}

func (o *Reporter) run(ctx context.Context) {
	// 通知外界已经退出
	defer close(o.quitC)

	ticker := time.Tick(time.Duration(o.config.Interval) * time.Second)
	for {
		select {
		case <-ticker:
			// 写数据文件, 或者上报到falcon
			// 如果出错打印日志, 不做特殊处理
			o.publishMetrics()

		case <-ctx.Done():
			log.Warnf("Reporter.run: ctx canceled, need to quit")
			return
		}
	}
}

func (o *Reporter) checkConfig() error {
	if o.config.PublishType != PublishType_File &&
		o.config.PublishType != PublishType_Falcon {
		return errors.New("Invalid config: PublishType")
	}

	if o.config.PublishType == PublishType_File {
		if o.config.FileDir == "" {
			return errors.New("Invalid config: FileDir")
		}
		if o.config.FileName == "" {
			return errors.New("Invalid config: FileName")
		}
	}

	if o.config.PublishType == PublishType_Falcon {
		if o.config.Url == "" {
			return errors.New("Invalid config: Url")
		}
	}

	if o.config.HostName == "" {
		return errors.New("Invalid config: HostName")
	}
	if o.config.Endpoint == "" {
		return errors.New("Invalid config: Endpoint")
	}
	if o.config.Interval <= 0 {
		return errors.New("Invalid config: Interval")
	}
	return nil

}

func (o *Reporter) Start() error {
	var err error

	err = o.checkConfig()
	if err != nil {
		log.Errorf("Reporter.Start: checkConfig error: %s", err)
		return err
	}

	// Init publisher
	if o.config.PublishType == PublishType_File {
		cfg := FilePublisherConfig{
			FileDir:  o.config.FileDir,
			FileName: o.config.FileName,
		}
		o.publisher, err = NewFilePublisher(cfg)
	} else if o.config.PublishType == PublishType_Falcon {
		cfg := FalconPublisherConfig{
			Url:     o.config.Url,
			Timeout: o.config.Timeout,
		}
		o.publisher, err = NewFalconPublisher(cfg)
	}

	if err != nil {
		log.Errorf("Reporter.Start: new publisher error: %s", err)
		return err
	}

	var ctx context.Context
	ctx, o.cancel = context.WithCancel(context.Background())
	go o.run(ctx)

	o.started = true
	return nil
}

func (o *Reporter) QuitNotify() <-chan struct{} {
	return o.quitC
}

func (o *Reporter) Stop() error {
	if !o.started {
		return nil
	}

	// 通知协程退出
	o.cancel()
	// 等待协程退出
	<-o.quitC

	if o.publisher != nil {
		o.publisher.Close()
	}

	o.started = false

	log.Warnf("Reporter.Stop: stop succeed")
	return nil
}
