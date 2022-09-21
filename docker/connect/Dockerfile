FROM centos:7

RUN yum install -y java-1.8.0-openjdk-devel.x86_64 unzip gettext nmap-ncat openssl, which gnupg, telnet \
 && yum clean all -y

ARG user=connect
ARG group=connect
ARG uid=3000
ARG gid=3000

# RocketMQ is run with user `rocketmq`, uid = 3000
# If you bind mount a volume from the host or a data container,
# ensure you use the same uid
RUN groupadd -g ${gid} ${group} \
    && useradd -u ${uid} -g ${gid} -m -s /bin/bash ${user}

ARG version

# RocketMQ Connect Version
ENV CONNECT_VERSION ${version}

# Rocketmq home
ENV CONNECT_HOME  /home/connect/mq-connect

WORKDIR /home/connect

RUN chown -R ${uid}:${gid}  /home/connect


# 设置环境变量 USERNAME 默认值为 admin，后面可以通过docker run -e USERNAME="XXXXXX"修改，这个环境变量在容器里也可以$USERNAME获取
ENV NAMESRV_ADDR=localhost:9876
# expose namesrv port
EXPOSE 8082

USER ${user}
#WORKDIR /home/connect

RUN mkdir -p /home/connect/mq-connect/bin /home/connect/mq-connect/plugins /home/connect/mq-connect/target /home/connect/mq-connect/cli && \
    chown -R connect:connect /home/connect/ && \
    echo "export JAVA_HOME=/lib/jvm/java-1.8.0-openjdk" >> ~/.bash_profile && \
    echo "export PATH=$PATH:/lib/jvm/java-1.8.0-openjdk/bin" >> ~/.bash_profile && \
    echo "export CLASSPATH=.:/lib/jvm/java-1.8.0-openjdk/lib" >> ~/.bash_profile && \
    source ~/.bash_profile

COPY ./plugins/*.jar /home/connect/mq-connect/plugins/
ADD runtime.tar.gz /home/connect/mq-connect/
ADD connect-cli.tar.gz /home/connect/mq-connect/cli/

COPY bin/connect-distributed.sh \
     bin/connect-standalone.sh \
     bin/connectshutdown.sh \
     bin/runconnect.sh \
     bin/connectAdmin \
     /home/connect/mq-connect/bin/

ENTRYPOINT ["/usr/bin/sh", "/home/connect/mq-connect/bin/connect-distributed.sh", "-c", "/home/connect/mq-connect/conf/connect-distributed.conf"]
