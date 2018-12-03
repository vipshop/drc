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
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"
)

func CreateEphemeralSequential(zkConn *zk.Conn, nodesPath string) (string, error) {
	if nodesPath[len(nodesPath)-1] != '/' {
		// Make sure to create node under nodesPath
		nodesPath += "/"
	}
	nodeName, err := zkConn.CreateProtectedEphemeralSequential(nodesPath, []byte(""), zk.WorldACL(zk.PermAll))
	if err != nil {
		return "", err
	}
	_, err = zkConn.Sync(nodeName)
	if err != nil {
		return "", err
	}
	return nodeName, err
}

func EnsurePath(zkConn *zk.Conn, path string, value []byte) error {

	pathTrimmed := strings.Trim(path, "/")
	dirs := strings.Split(pathTrimmed, "/")
	currPath := ""

	for i := 0; i < len(dirs); i++ {
		currPath += "/" + dirs[i]
		exists, _, err := zkConn.Exists(currPath)
		if err != nil {
			log.Errorf("ensurePath:Exists error,err:%s,path:%s", err, currPath)
			return err
		}

		if exists {
			continue
		}
		if i == len(dirs)-1 {
			_, err = zkConn.Create(currPath, value, 0, zk.WorldACL(zk.PermAll))
			if err != nil {
				log.Errorf("ensurePath:Create error,err:%s,path:%s,value:%v", err, currPath, value)
				return err
			}
		} else {
			_, err = zkConn.Create(currPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
			if err != nil {
				log.Errorf("ensurePath:Create error,err:%s,path:%s,value:%v", err, currPath, nil)
				return err
			}
		}
		log.Info("Created zookeeper path: ", currPath)
	}
	return nil
}
