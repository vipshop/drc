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

import "errors"

var (
	ErrNotDeleteType          = errors.New("applier: event is not delete type")
	ErrNotUpdateType          = errors.New("applier: event is not update type")
	ErrNotInsertType          = errors.New("applier: event is not insert type")
	ErrColumnArrayIsNil       = errors.New("applier: column array is nil")
	ErrRowArrayIsNil          = errors.New("applier: row array is nil")
	ErrColumnIsNil            = errors.New("applier: column array is nil")
	ErrValueIsNil             = errors.New("applier: value is nil")
	ErrArrayLengthNotEqual    = errors.New("applier: array length is not equal")
	ErrArgsIsNil              = errors.New("applier: args is nil")
	ErrUnsupportedRdpMessage  = errors.New("applier: unsupported rdp messages")
	ErrIncontinuousRdpMessage = errors.New("applier: incontinuous rdp messages")
	ErrNeedToPeekTrx          = errors.New("applier: need to peek the trx")
	ErrUpdateCheckpointTable  = errors.New("applier: update checkpoint table error")
	ErrColumnCountNotEqual    = errors.New("applier: column count not equal")
	ErrTimeIsZero             = errors.New("applier: time is zero")
	ErrNoPK                   = errors.New("applier: no primary key")
	ErrorRowsAffectedCount    = errors.New("applier: the count of affected row is not expected")
	ErrStrategyNotSupport     = errors.New("applier: this strategy of handling conflict is not support")
	ErrrUnexpect              = errors.New("applier: unexpected error")
	ErrTimeType               = errors.New("applier: time type is not expected")
	ErrEventType              = errors.New("applier: event type is not expected")
	ErrLargeTrx               = errors.New("applier: large trx has been filtered by rdp")
	ErrIllegalTrx             = errors.New("applier: illegal trx that is not allowed")
	ErrNotEqual               = errors.New("applier:args count not equal")
	ErrSkip                   = errors.New("applier: skip fast-path; continue as if unimplemented")
	ErrServerIdChange         = errors.New("applier: server_id has changed")
)
