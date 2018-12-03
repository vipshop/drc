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
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"
	"path"
	"testing"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/require"
)

func TestReport(t *testing.T) {
	var err error
	config := ReporterConfig{
		PublishType: "file",
		Owner:       "drc",
		HostName:    "127.0.0.1",
		Endpoint:    "10001",
		Interval:    1,
		FileDir:     "./",
		FileName:    "drc.metrics",
	}

	// 清理测试数据文件
	filePath := path.Join(config.FileDir, config.FileName)
	os.Remove(filePath)

	c := metrics.NewCounter()
	metrics.Register("counter", c)
	c.Inc(600)

	g := metrics.NewGauge()
	metrics.Register("gauge", g)
	g.Update(600)

	m := metrics.NewMeter()
	metrics.Register("meter", m)
	m.Mark(600) // 标记有600个事件

	reporter := NewReporter(&config)
	err = reporter.Start()
	require.Nil(t, err)

	time.Sleep(time.Duration(6*config.Interval) * time.Second)

	// 打开数据文件检查
	fr, err := os.OpenFile(filePath, os.O_RDONLY, 0660)
	require.Nil(t, err)

	rb := bufio.NewReaderSize(fr, 1024*16)
	counterOk := false
	gaugeOk := false
	meterOk := false

	for {
		line, err := rb.ReadString('\n')
		if err == io.EOF {
			break
		}
		require.Nil(t, err)

		var metric DrcMetric
		err = json.Unmarshal([]byte(line), &metric)
		require.Nil(t, err)

		if metric.Metric == "counter" && metric.Value == 600 {
			if !counterOk {
				log.Println("TestReport: counter metric is ok")
			}
			counterOk = true
		}
		if metric.Metric == "gauge" && metric.Value == 600 {
			if !gaugeOk {
				log.Println("TestReport: gauge metric is ok")
			}
			gaugeOk = true
		}
		// go-metrics库每5秒钟会计算meter的值
		// 5秒钟内产生了600个事件，速率是600/5
		if metric.Metric == "meter" && metric.Value == 119 {
			if !meterOk {
				log.Println("TestReport: meter metric is ok")
			}
			meterOk = true
		}
	}

	require.True(t, counterOk && gaugeOk && meterOk)
	os.Remove(filePath)

}
