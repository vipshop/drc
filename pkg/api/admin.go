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

package api

import (
	"context"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/labstack/echo"
	mw "github.com/labstack/echo/middleware"
	"github.com/vipshop/drc/pkg/applier"
)

type AdminServerConfig struct {
	Addr string
}

type AdminServer struct {
	addr           string
	web            *echo.Echo
	applierHandler *ApplierHandler
	debugHandler   *DebugHandler
	//退出信号
	quitC chan struct{}
}

func NewAdminServer(cfg *AdminServerConfig) *AdminServer {
	cfg.checkArgs()
	return &AdminServer{
		addr:           cfg.Addr,
		web:            echo.New(),
		applierHandler: newApplierHandler(nil),
		debugHandler:   newDebugHandler(),
		quitC:          make(chan struct{}),
	}
}

func (cfg *AdminServerConfig) checkArgs() {
	if len(cfg.Addr) == 0 {
		log.Fatalf("AdminServerConfig.checkArgs:addr is nil")
	}
}

func (s *AdminServer) Start() {
	s.web.HideBanner = true
	s.web.HidePort = true

	s.RegisterMiddleware()
	s.RegisterURL()

	log.Infof("AdminServer.Start:start web server")

	defer close(s.quitC)
	err := s.web.Start(s.addr)
	if err != nil {
		log.Errorf("AdminServer.Start:web server start error,err:%s", err)
	}
}

func (s *AdminServer) RegisterMiddleware() {
	//logConfig := mw.LoggerConfig{
	//	Format: `time:${time_rfc3339_nano},id:${id},remote_ip:${remote_ip},host:${host},` +
	//		`method:${method},uri:${uri},status:${status}, ` +
	//		`latency:${latency_human},bytes_in:${bytes_in},` +
	//		`bytes_out:${bytes_out}` + "\n",
	//	Output:  log.StandardLogger().Writer(),
	//}
	//s.web.Use(mw.LoggerWithConfig(logConfig))
	s.web.Use(mw.Recover())
}

func (s *AdminServer) SetApplierHandler(applierServer *applier.ApplierServer) {
	s.applierHandler.applier = applierServer
}

func (s *AdminServer) RegisterURL() {
	s.web.GET("/api/v1/applier/failed_trx", s.applierHandler.GetFailedTrxList)
	s.web.PUT("/api/v1/applier/failed_trx/retry", s.applierHandler.RetryFailedTrx)
	s.web.PUT("/api/v1/applier/failed_trx/skip", s.applierHandler.SkipFailedTrx)
	s.web.GET("/api/v1/applier/progress", s.applierHandler.GetApplyProgress)
	s.web.GET("/api/v1/applier/work_mode", s.applierHandler.GetWorkMode)
	s.web.GET("/debug/profile", s.debugHandler.Profile)
	//for k8s health check
	s.web.GET("/_health_check", s.applierHandler.HealthCheck)
}

func (s *AdminServer) QuitNotify() <-chan struct{} {
	return s.quitC
}

func (s *AdminServer) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.web.Shutdown(ctx); err != nil {
		log.Infof("AdminServer.Stop: Shutdown error:%s", err.Error())
	}
	<-s.quitC
	log.Infof("AdminServer Stop success")
}
