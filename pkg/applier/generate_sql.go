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
	"strconv"
	"strings"
	"time"
)

const (
	maxPacketSize = 1<<24 - 1
	digits01      = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
	digits10      = "0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999"
)

func generateSQL(query string, args []interface{}) (string, error) {
	// Number of ? should be same to len(args)
	if strings.Count(query, "?") != len(args) {
		return "", ErrNotEqual
	}

	buf := make([]byte, 0, 128)
	buf = buf[:0]
	argPos := 0

	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf = append(buf, query[i:]...)
			break
		}
		buf = append(buf, query[i:i+q]...)
		i += q

		arg := args[argPos]
		argPos++

		if arg == nil {
			buf = append(buf, "NULL"...)
			continue
		}

		switch v := arg.(type) {
		case int8:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case uint8:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case int16:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case uint16:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case int32:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case uint32:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case int:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case uint:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case int64:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case uint64:
			buf = strconv.AppendUint(buf, v, 10)
		case float32:
			buf = strconv.AppendFloat(buf, float64(v), 'g', -1, 64)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case time.Time:
			if v.IsZero() {
				buf = append(buf, "'0000-00-00'"...)
			} else {
				//
				loc, err := time.LoadLocation("Asia/Shanghai")
				if err != nil {
					return "", err
				}
				v := v.In(loc)
				v = v.Add(time.Nanosecond * 500) // To round under microsecond
				year := v.Year()
				year100 := year / 100
				year1 := year % 100
				month := v.Month()
				day := v.Day()
				hour := v.Hour()
				minute := v.Minute()
				second := v.Second()
				micro := v.Nanosecond() / 1000

				buf = append(buf, []byte{
					'\'',
					digits10[year100], digits01[year100],
					digits10[year1], digits01[year1],
					'-',
					digits10[month], digits01[month],
					'-',
					digits10[day], digits01[day],
					' ',
					digits10[hour], digits01[hour],
					':',
					digits10[minute], digits01[minute],
					':',
					digits10[second], digits01[second],
				}...)

				if micro != 0 {
					micro10000 := micro / 10000
					micro100 := micro / 100 % 100
					micro1 := micro % 100
					buf = append(buf, []byte{
						'.',
						digits10[micro10000], digits01[micro10000],
						digits10[micro100], digits01[micro100],
						digits10[micro1], digits01[micro1],
					}...)
				}
				buf = append(buf, '\'')
			}
		case []byte:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				buf = append(buf, "_binary'"...)
				buf = escapeBytesQuotes(buf, v)
				buf = append(buf, '\'')
			}
		case string:
			buf = append(buf, '\'')
			buf = escapeStringQuotes(buf, v)
			buf = append(buf, '\'')
		default:
			return "", ErrSkip
		}

		if len(buf)+4 > maxPacketSize {
			return "", ErrSkip
		}
	}
	if argPos != len(args) {
		return "", ErrSkip
	}
	return string(buf), nil
}

// escapeBytesQuotes escapes apostrophes in []byte by doubling them up.
// This escapes the contents of a string by doubling up any apostrophes that
// it contains. This is used when the NO_BACKSLASH_ESCAPES SQL_MODE is in
// effect on the server.
// https://github.com/mysql/mysql-server/blob/mysql-5.7.5/mysys/charset.c#L963-L1038
func escapeBytesQuotes(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeStringQuotes is similar to escapeBytesQuotes but for string.
func escapeStringQuotes(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// reserveBuffer checks cap(buf) and expand buffer to len(buf) + appendSize.
// If cap(buf) is not enough, reallocate new buffer.
func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		// Grow buffer exponentially
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}
