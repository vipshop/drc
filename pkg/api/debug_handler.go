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
	"github.com/labstack/echo"
	"net/http/pprof"
)

type DebugHandler struct {
}

func newDebugHandler() *DebugHandler {
	return &DebugHandler{}
}

func (h *DebugHandler) Profile(ctx echo.Context) error {
	pprof.Profile(ctx.Response().Writer, ctx.Request())
	return nil
}
