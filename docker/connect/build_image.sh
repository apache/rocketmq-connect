#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
cd ../..
cd `dirname $0`

WORK_DIR=`pwd`

mvn clean install -DskipTests -Dmaven.test.skip=true -U

if test -f ${WORK_DIR}/docker/connect/distribution.tar.gz; then
    rm -f ${WORK_DIR}/docker/connect/distribution.tar.gz
fi

if test -f ${WORK_DIR}/docker/connect/distribution.tar.gz; then
    rm -f ${WORK_DIR}/docker/connect/distribution.tar.gz
fi

if test -f ${WORK_DIR}/docker/connect/rocketmq-connect-sample-0.0.1-SNAPSHOT.jar; then
    rm -f ${WORK_DIR}/docker/connect/rocketmq-connect-sample-0.0.1-SNAPSHOT.jar
fi

if test -f ${WORK_DIR}/docker/connect/rocketmq-replicator-0.1.0-SNAPSHOT-jar-with-dependencies.jar; then
    rm -f ${WORK_DIR}/docker/connect/rocketmq-replicator-0.1.0-SNAPSHOT-jar-with-dependencies.jar
fi

cd ${WORK_DIR}/rocketmq-connect-runtime/target/

mv distribution.tar.gz runtime.tar.gz
cp runtime.tar.gz ${WORK_DIR}/docker/connect/

cd ${WORK_DIR}/rocketmq-connect-cli/target
mv distribution.tar.gz connect-cli.tar.gz
cp connect-cli.tar.gz ${WORK_DIR}/docker/connect/

mkdir -p ${WORK_DIR}/docker/connect/plugins/

cd ${WORK_DIR}/rocketmq-connect-sample/target

cp rocketmq-connect-sample-0.0.1-SNAPSHOT.jar ${WORK_DIR}/docker/connect/plugins/

cd ${WORK_DIR}/connectors/rocketmq-replicator/
mvn clean package -DskipTests -Dmaven.test.skip=true -U
cd ${WORK_DIR}/connectors/rocketmq-replicator/target
cp rocketmq-replicator-0.1.0-SNAPSHOT-jar-with-dependencies.jar ${WORK_DIR}/docker/connect/plugins/

CONNECT_VERSION=0.0.1-SNAPSHOT

cd ${WORK_DIR}/docker/connect
docker build --no-cache -f Dockerfile -t apache/rocketmqconnect:${CONNECT_VERSION} .

