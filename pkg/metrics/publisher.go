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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"time"

	log "github.com/Sirupsen/logrus"
	rotater "github.com/firnsan/file-rotater"
)

// Publisher接口用于发布metric, 比如写入到文件或者上报到falcon
type Publisher interface {
	Publish([]*DrcMetric) error
	Close() error
}

// FilePublisher用于将metric写入本地文件
type FilePublisher struct {
	fw *rotater.FileRotater
}

type FilePublisherConfig struct {
	// 数据文件保存位置
	FileDir  string
	FileName string
}

func NewFilePublisher(cfg FilePublisherConfig) (*FilePublisher, error) {
	filePath := path.Join(cfg.FileDir, cfg.FileName)
	fw, err := rotater.NewFileRotater(filePath)
	if err != nil {
		log.Errorf("NewFilePublisher: NewFileRotater error: %s, filePath:%s", err, filePath)
		return nil, err
	}

	publisher := FilePublisher{
		fw: fw,
	}

	return &publisher, nil
}

func (o *FilePublisher) Publish(drcMetrics []*DrcMetric) error {
	for _, drcMetric := range drcMetrics {
		buf, err := json.Marshal(drcMetric)
		if err != nil {
			log.Errorf("FilePublisher.Publish: json marshl error: %s, struct: %v", err, drcMetric)
			continue
		}
		buf = append(buf, '\n')
		o.fw.Write(buf)
	}
	o.fw.Flush()

	return nil
}

func (o *FilePublisher) Close() error {
	o.fw.Close()
	return nil
}

// FalconPublisher用于将metric写入到falcon
type FalconPublisher struct {
	url    string
	client *http.Client
}

type FalconPublisherConfig struct {
	Url     string
	Timeout int
}

func NewFalconPublisher(cfg FalconPublisherConfig) (*FalconPublisher, error) {
	client := &http.Client{
		Timeout: time.Duration(cfg.Timeout) * time.Second,
	}
	publisher := FalconPublisher{
		url:    cfg.Url,
		client: client,
	}
	return &publisher, nil
}

func (o *FalconPublisher) Publish(drcMetrics []*DrcMetric) error {
	var metrics []interface{}
	for _, drcMetric := range drcMetrics {
		tags := fmt.Sprintf("owner=%s,hostname=%s", drcMetric.Owner, drcMetric.HostName)

		metric := map[string]interface{}{
			"tags":        tags,
			"metric":      drcMetric.Metric,
			"endpoint":    drcMetric.Endpoint,
			"timestamp":   drcMetric.Timestamp / 1000, // 秒为单位
			"value":       drcMetric.Value,
			"step":        drcMetric.Interval,
			"counterType": "GAUGE",
		}

		metrics = append(metrics, metric)
	}

	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	err := enc.Encode(metrics)
	if err != nil {
		log.Errorf("FalconPublisher.Publish: json marshl error: %s", err)
		return err
	}

	req, err := http.NewRequest("POST", o.url, buf)
	if err != nil {
		log.Errorf("FalconPublisher.Publish: http new request error: %s", err)
		return err
	}

	req.Header.Add("Accept", "application/json")
	resp, err := o.client.Do(req)
	if err != nil {
		log.Errorf("FalconPublisher.Publish: http do request error: %s", err)
		return err
	}
	defer resp.Body.Close()

	status := resp.StatusCode
	if status != 200 {
		return fmt.Errorf("FalconPublisher.Publish: http status is not ok: %d", status)
	}

	return nil
}

func (o *FalconPublisher) Close() error {
	return nil
}
