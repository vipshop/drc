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

package status

import (
	"sync"
)

type Getter func() interface{}
type StatusRegister struct {
	m map[string]Getter
	l *sync.Mutex
}

var (
	gRegister = StatusRegister{
		m: make(map[string]Getter),
		l: new(sync.Mutex),
	}
)

func Register(name string, getter Getter) {
	gRegister.l.Lock()
	defer gRegister.l.Unlock()
	gRegister.m[name] = getter
}

func Unregister(name string) {
	gRegister.l.Lock()
	defer gRegister.l.Unlock()
	delete(gRegister.m, name)
}

func Get(name string) interface{} {
	gRegister.l.Lock()
	defer gRegister.l.Unlock()
	if getter, ok := gRegister.m[name]; ok {
		return getter()
	}
	return nil
}
