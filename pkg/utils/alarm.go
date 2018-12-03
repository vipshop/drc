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

package utils

import (
	log "github.com/Sirupsen/logrus"
	"os/exec"
)

func Alarm(msg string) {
	if msg == "" {
		return
	}
	log.Infof("Alarm: executing alarm cmd, msg: %s", msg)

	for tryTimes := 1; tryTimes <= 5; tryTimes++ {
		cmd := exec.Command("./alarm.sh", msg)
		buf, err := cmd.Output()
		log.Infof("Alarm: execute alarm output: %s", buf)
		if err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				log.Errorf("Alarm: execute alarm cmd error: %s, stderr: %s, try_times: %d",
					ee, string(ee.Stderr), tryTimes)
			} else {
				log.Errorf("Alarm: execute alarm cmd error: %s, try_times: %d", err, tryTimes)
			}
			continue
		}

		// Alarm success
		break
	}
}
