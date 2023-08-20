## How to Use Connnector-Archetype

1. 进入脚手架文件夹
   
   ```shell
   cd rocketmq-connect-connectors-archetype/
   ```

2. 将脚手架安装到本地
   
   ```shell
   mvn -e clean install
   ```

3. 创建connector模版工程
   
   ```shell
   cd connectors/
   mvn archetype:generate \
    -DarchetypeGroupId=org.apache.rocketmq \
    -DarchetypeArtifactId=rocketmq-connect-connectors-archetype \
    -DarchetypeVersion=1.0-SNAPSHOT \
    -DdatabaseName=<databasename>
   ```
   
   例：创建Clickhouse-Connector
   
   ```shell
   mvn archetype:generate \
    -DarchetypeGroupId=org.apache.rocketmq \
    -DarchetypeArtifactId=rocketmq-connect-connectors-archetype \
    -DarchetypeVersion=1.0-SNAPSHOT \
    -DdatabaseName=clickhouse
   ```

4. 如上指令将创建一个connector的框架，开发者主要关心`helper/xxxHelperClient`以及`xxxxSourceTask`,`xxxSinkTask`的实现即可，剩余配置可以按需修改。
