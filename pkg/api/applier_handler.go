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
	"net/http"

	log "github.com/Sirupsen/logrus"
	"github.com/labstack/echo"
	"github.com/vipshop/drc/pkg/applier"
	"github.com/vipshop/drc/pkg/election"
	"github.com/vipshop/drc/pkg/status"
)

type ApplierHandler struct {
	applier *applier.ApplierServer
}

func newApplierHandler(applier *applier.ApplierServer) *ApplierHandler {
	return &ApplierHandler{
		applier: applier,
	}
}

func (h *ApplierHandler) GetFailedTrxList(ctx echo.Context) error {
	v := status.Get(applier.StatusKey_FailedTrx)
	if v == nil {
		log.Infof("ApplierHandler.GetFailedTrxList: status not ready")
		return ctx.JSON(http.StatusNoContent, NewResp().SetError(ErrStatusNotReady.Error()))
	}
	execResult, ok := v.(map[string]applier.ExecResult)
	if !ok {
		log.Errorf("ApplierHandler.GetFailedTrxList: get value is illegal,v:%v", v)
		return ctx.JSON(http.StatusInternalServerError, NewResp().SetError(ErrStatusIllegal.Error()))
	}
	return ctx.JSON(http.StatusOK, NewResp().SetData(execResult))
}

func (h *ApplierHandler) RetryFailedTrx(ctx echo.Context) error {
	args := struct {
		Gtid string `json:"gtid"`
	}{}
	err := ctx.Bind(&args)
	if err != nil {
		return ctx.JSON(http.StatusBadRequest, NewResp().SetError(err.Error()))
	}
	if args.Gtid == "" {
		return ctx.JSON(http.StatusBadRequest, NewResp().SetError(ErrArgsIllegal.Error()))
	}

	// 检查指定的gtid是否存在
	v := status.Get(applier.StatusKey_FailedTrx)
	if v == nil {
		log.Infof("ApplierHandler.RetryFailedTrx: status not ready")
		return ctx.JSON(http.StatusNoContent, NewResp().SetError(ErrStatusNotReady.Error()))
	}
	execResult, ok := v.(map[string]applier.ExecResult)
	if !ok {
		log.Errorf("ApplierHandler.RetryFailedTrx: get value is illegal,v:%v", v)
		return ctx.JSON(http.StatusInternalServerError, NewResp().SetError(ErrStatusIllegal.Error()))
	}
	if _, ok := execResult[args.Gtid]; !ok {
		// 指定的gtid不存在
		return ctx.JSON(http.StatusBadRequest, NewResp().SetError(ErrArgsIllegal.Error()))
	}
	//如果节点为Follower，则返回出错信息
	if h.applier == nil {
		return ctx.JSON(http.StatusInternalServerError, NewResp().SetError("not leader"))
	}
	h.applier.RetryTrx(args.Gtid)
	return ctx.JSON(http.StatusOK, NewResp().SetData(args.Gtid))
}

func (h *ApplierHandler) SkipFailedTrx(ctx echo.Context) error {
	args := struct {
		Gtid string `json:"gtid"`
	}{}
	err := ctx.Bind(&args)
	if err != nil {
		return ctx.JSON(http.StatusBadRequest, NewResp().SetError(err.Error()))
	}
	if args.Gtid == "" {
		return ctx.JSON(http.StatusBadRequest, NewResp().SetError(ErrArgsIllegal.Error()))
	}

	// 检查指定的gtid是否存在
	v := status.Get(applier.StatusKey_FailedTrx)
	if v == nil {
		log.Infof("ApplierHandler.RetryFailedTrx: status not ready")
		return ctx.JSON(http.StatusNoContent, NewResp().SetError(ErrStatusNotReady.Error()))
	}
	execResult, ok := v.(map[string]applier.ExecResult)
	if !ok {
		log.Errorf("ApplierHandler.RetryFailedTrx: get value is illegal,v:%v", v)
		return ctx.JSON(http.StatusInternalServerError, NewResp().SetError(ErrStatusIllegal.Error()))
	}
	if _, ok := execResult[args.Gtid]; !ok {
		// 指定的gtid不存在
		return ctx.JSON(http.StatusBadRequest, NewResp().SetError(ErrArgsIllegal.Error()))
	}
	//如果节点为Follower，则返回出错信息
	if h.applier == nil {
		return ctx.JSON(http.StatusInternalServerError, NewResp().SetError("not leader"))
	}
	h.applier.SkipTrx(args.Gtid)
	return ctx.JSON(http.StatusOK, NewResp().SetData(args.Gtid))
}

func (h *ApplierHandler) GetApplyProgress(ctx echo.Context) error {
	v := status.Get(applier.StatusKey_ApplyProgress)
	if v == nil {
		log.Infof("ApplierHandler.GetApplyProgress: status not ready")
		return ctx.JSON(http.StatusNoContent, NewResp().SetError(ErrStatusNotReady.Error()))
	}
	progress, ok := v.(applier.ApplyProgress)
	if !ok {
		log.Errorf("ApplierHandler.GetApplyProgress: status get value is illegal,v:%v", v)
		return ctx.JSON(http.StatusInternalServerError, NewResp().SetError(ErrStatusIllegal.Error()))
	}
	return ctx.JSON(http.StatusOK, NewResp().SetData(progress))
}

func (h *ApplierHandler) GetWorkMode(ctx echo.Context) error {
	v := status.Get(election.StatusKey_WorkMode)
	if v == nil {
		log.Infof("ApplierHandler.GetWorkMode: status not ready")
		return ctx.JSON(http.StatusNoContent, NewResp().SetError(ErrStatusNotReady.Error()))
	}
	workMode, ok := v.(election.WorkingMode)
	if !ok {
		log.Errorf("ApplierHandler.GetWorkMode: status get value is illegal,v:%v", v)
		return ctx.JSON(http.StatusInternalServerError, NewResp().SetError(ErrStatusIllegal.Error()))
	}
	return ctx.JSON(http.StatusOK, NewResp().SetData(workMode))
}

func (h *ApplierHandler) HealthCheck(ctx echo.Context) error {
	return ctx.String(http.StatusOK, "ok")
}

type Resp struct {
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func NewResp() *Resp {
	return &Resp{
		Message: "success",
	}
}

func (r *Resp) SetData(data interface{}) *Resp {
	r.Data = data
	return r
}

func (r *Resp) SetError(msg string) *Resp {
	r.Message = msg
	return r
}
