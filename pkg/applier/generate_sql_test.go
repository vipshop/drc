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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerateSQL(t *testing.T) {
	query := "select a,b from c where d=? and e=?" +
		" and f=? and g=? and h=? and i=? and j=? and k=?" +
		" and l=? and m=? and n=? and o=? and p=?"
	var d int8 = -11
	var e uint8 = 2
	var f int16 = 34
	var g uint16 = 35
	var h int32 = -123
	var i uint32 = 456
	var j int = 45
	var k uint = 90
	var l int64 = 13565
	var m uint64 = 45676
	var n float32 = 2.5
	var o float64 = 5676.45
	var p string = "hello"
	args := []interface{}{d, e, f, g, h, i, j, k, l, m, n, o, p}
	sql, err := generateSQL(query, args)
	assert.Nil(t, err)
	assert.Equal(t, "select a,b from c where d=-11 and e=2 and f=34 and g=35 "+
		"and h=-123 and i=456 and j=45 and k=90 and l=13565 and m=45676 and n=2.5 and o=5676.45 and p='hello'", sql)
}
