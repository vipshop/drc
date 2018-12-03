
COMMIT_HASH=$(shell git rev-parse --short HEAD || echo "GitNotFound")
BUILD_DATE=$(shell date '+%Y-%m-%d %H:%M:%S')
PKG_NAME=mysql-applier.$(shell date "+%Y%m%d")

all: build

build: mysql-applier

mysql-applier:
	$(info COMMIT_HASH: $(COMMIT_HASH))
	cd ./cmd/mysql-applier/ && go build -ldflags "-X \"main.BuildVersion=${COMMIT_HASH}\" -X \"main.BuildDate=$(BUILD_DATE)\"" -o ../../build/mysql-applier/bin/mysql-applier 
	cp -r etc build/mysql-applier/
	cp pkg/utils/alarm.sh build/mysql-applier/bin/
	chmod +x build/mysql-applier/bin/*.sh

package: mysql-applier
	mkdir -p build/${PKG_NAME}/bin
	cp build/mysql-applier/bin/mysql-applier build/${PKG_NAME}/bin
	cp build/mysql-applier/bin/*.sh build/${PKG_NAME}/bin 
	cp build/mysql-applier/etc/applier.ini.example build/${PKG_NAME}
	cd build/${PKG_NAME} && tar -czvf ../${PKG_NAME}.tgz	 .
	rm -rf build/${PKG_NAME}


test:
	@echo ${MYSQL_ADDR}
	cd pkg/applier/ && go test -v --mysql-addr=${MYSQL_ADDR}
	cd pkg/election && go test -v
	cd pkg/metrics && go test -v

