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
	"io"
	stdlog "log"
	"os"

	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	rotater "github.com/firnsan/file-rotater"
	"strings"
)

type DragonflyFormatter struct {
}

func (f *DragonflyFormatter) Format(entry *log.Entry) ([]byte, error) {
	data := make(log.Fields, len(entry.Data))
	for k, v := range entry.Data {
		switch v := v.(type) {
		case error:
			// Otherwise errors are ignored by `encoding/json`
			// https://github.com/sirupsen/logrus/issues/137
			data[k] = v.Error()
		default:
			data[k] = v
		}
	}

	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	//write time
	err := b.WriteByte('[')
	if err != nil {
		return nil, err
	}
	_, err = b.WriteString(entry.Time.Format("2006-01-02 15:04:05.000"))
	if err != nil {
		return nil, err
	}
	err = b.WriteByte(']')
	if err != nil {
		return nil, err
	}

	//write log level
	_, err = b.WriteString(" [")
	if err != nil {
		return nil, err
	}
	_, err = b.WriteString(strings.ToUpper(entry.Level.String()))
	if err != nil {
		return nil, err
	}
	err = b.WriteByte(']')
	if err != nil {
		return nil, err
	}

	//write the fixed prefix
	_, err = b.WriteString(" [1] [applier] >>> [applier]")
	if err != nil {
		return nil, err
	}

	//write json
	if len(data) != 0 {
		_, err = b.WriteString(" json=")
		if err != nil {
			return nil, err
		}
		encoder := json.NewEncoder(b)
		if err := encoder.Encode(data); err != nil {
			return nil, fmt.Errorf("Failed to marshal fields to JSON, %v", err)
		}
		//json会产生一个回车符，需要截断
		b.Truncate(b.Len() - 1)
	}

	//write message
	_, err = b.WriteString(" msg=")
	if err != nil {
		return nil, err
	}
	_, err = b.WriteString(entry.Message)
	if err != nil {
		return nil, err
	}
	err = b.WriteByte('\n')
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func InitLog(logDir string, logLevel string) error {
	var err error

	if logDir == "" {
		logDir = "./logs"
	}

	level, err := log.ParseLevel(logLevel)
	if err != nil {
		log.Errorf("parse log level failed: %s", err)
		return err
	}

	fw, err := rotater.NewFileRotater(logDir + "/app.log")
	if err != nil {
		log.Errorf("set log failed: %s", err)
		return err
	}

	log.SetFormatter(&DragonflyFormatter{})
	log.SetOutput(fw)
	log.SetLevel(level)
	log.AddHook(NewAlarmHook())

	// Also need to set the stdlog's output to this writer
	w := log.StandardLogger().Writer()
	stdlog.SetOutput(w)

	return nil
}

func UninitLog() {
	// Reset the hooks
	log.StandardLogger().Hooks = make(log.LevelHooks)

	// Close Writer
	w := log.StandardLogger().Out
	log.SetOutput(os.Stderr)
	if wc, ok := w.(io.Closer); ok {
		wc.Close()
	}

	log.Printf("uninit log success")
}

type AlarmHook struct {
}

func NewAlarmHook() *AlarmHook {
	return &AlarmHook{}
}

func (o *AlarmHook) Fire(entry *log.Entry) error {
	msg, err := entry.String()
	if err != nil {

	}

	switch entry.Level {
	case log.PanicLevel:
		fallthrough
	case log.FatalLevel:
		return o.alarm(msg, "")
	case log.ErrorLevel:
		return o.alarm(msg, "")
	default:
		return nil
	}
}

func (o *AlarmHook) Levels() []log.Level {
	return []log.Level{log.ErrorLevel, log.FatalLevel, log.PanicLevel}
}

func (o *AlarmHook) alarm(msg string, alarmId string) error {
	return nil
}
