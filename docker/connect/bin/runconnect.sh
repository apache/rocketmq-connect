#!/bin/sh

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

#===========================================================================================
# Java 环境设置
#===========================================================================================
error_exit ()
{
    echo "ERROR: $1 !!"
    exit 1
}


check_java_version ()
{
  _java=$1
  _version=$2
  version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}' | grep -o '^[0-9]\+\.\+[0-9]\+')
  flag="true"
  if [[ $(echo "$version < $_version" | bc) -eq 1 ]]; then
      flag="false"
  fi

  if [[ $("$_java" -version 2>&1) != *"64-Bit"* ]]; then
      flag="false"
  fi

  echo $flag
}

[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=$HOME/jdk/java
[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=/usr/java
[ ! -e "$JAVA_HOME/bin/java" ] && error_exit "Please set the JAVA_HOME variable in your environment, We need java(x64)!"

export JAVA_HOME
export JAVA="$JAVA_HOME/bin/java"
export BASE_DIR=$(dirname $0)/..
export CLASSPATH=.:${BASE_DIR}/conf:${CLASSPATH}
export LANG=en_US.UTF-8


#===========================================================================================
# JVM Parameters Configuration
#===========================================================================================

JAVA_OPT="${JAVA_OPT} -server -Xms2g -Xmx2g"
JAVA_OPT="${JAVA_OPT} -XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:G1ReservePercent=25 -XX:InitiatingHeapOccupancyPercent=30 -XX:SoftRefLRUPolicyMSPerMB=0 -XX:SurvivorRatio=8 -XX:+DisableExplicitGC"
JAVA_OPT="${JAVA_OPT} -verbose:gc"
JAVA_OPT="${JAVA_OPT} -XX:-OmitStackTraceInFastThrow"
JAVA_OPT="${JAVA_OPT} -XX:+AlwaysPreTouch"
JAVA_OPT="${JAVA_OPT} -XX:MaxDirectMemorySize=10g"
JAVA_OPT="${JAVA_OPT} -XX:-UseLargePages -XX:-UseBiasedLocking"
if [[ $(check_java_version "$JAVA" "1.8") == "false" ]]; then
  JAVA_OPT="${JAVA_OPT} -XX:PermSize=128m -XX:MaxPermSize=320m"
fi
if [[ $(check_java_version "$JAVA" "9") == "false" ]]; then
  JAVA_OPT="${JAVA_OPT} -Xloggc:/dev/shm/mq_gc_%p.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintAdaptiveSizePolicy"
  JAVA_OPT="${JAVA_OPT} -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=30m"
  JAVA_OPT="${JAVA_OPT} -Djava.ext.dirs=${BASE_DIR}/lib:${JAVA_HOME}/jre/lib/ext"
  JAVA_OPT="${JAVA_OPT} -cp ${CLASSPATH}"
else
  if [ "$(uname)" == "Darwin" ]; then
    JAVA_OPT="${JAVA_OPT} -Xlog:gc:/tmp/mq_gc_$$.log -Xlog:gc*"
    JAVA_OPT="${JAVA_OPT} -cp $(find "${BASE_DIR}/lib" -name '*.jar' | tr "\n" ":"):${CLASSPATH}"
  else
    JAVA_OPT="${JAVA_OPT} -Xlog:gc:/dev/shm/mq_gc_%p.log -Xlog:gc*"
    JAVA_OPT="${JAVA_OPT} -cp $(find "${BASE_DIR}/lib" -name '*.jar' | sed ':a;N;s/\n/:/;ba;'):${CLASSPATH}"
  fi
fi
JAVA_OPT="${JAVA_OPT} -DisSyncFlush=false"

#===========================================================================================
# SSL Configuration
#===========================================================================================
JAVA_OPT="${JAVA_OPT} -Dtls.server.mode=disabled"
JAVA_OPT="${JAVA_OPT} -Dtls.private.key.encrypted=false"
JAVA_OPT="${JAVA_OPT} -Djdk.tls.rejectClientInitiatedRenegotiation=true"


if [[ $(check_java_version "$JAVA" "1.7") == "false" ]]; then
    error_exit "Java version is too low, we need java(x64) 1.7+!"
fi

numactl --interleave=all pwd > /dev/null 2>&1
if [ $? -eq 0 ]
then
    exec numactl --interleave=all $JAVA ${JAVA_OPT} $@
else
    exec $JAVA ${JAVA_OPT} $@
fi
