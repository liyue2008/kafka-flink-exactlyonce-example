# 消息队列高手课

极客时间《消息队列高手课》案例篇《30. 流计算与消息（二）：在流计算中使用Kafka链接计算任务》示例源代码。

## 环境要求

运行示例之前需要先安装：

* JDK 1.8
* Scala 2.12
* Maven 3.3.9

```bash
$java -version
java version "1.8.0_202"
Java(TM) SE Runtime Environment (build 1.8.0_202-b08)
Java HotSpot(TM) 64-Bit Server VM (build 25.202-b08, mixed mode)

$scala -version
Scala code runner version 2.12.4 -- Copyright 2002-2017, LAMP/EPFL and Lightbend, Inc.

$mvn -version
Apache Maven 3.3.9 (bb52d8502b132ec0a5a3f4c09453c07478323dc5; 2015-11-11T00:41:47+08:00)
```

## 下载编译源代码

```bash
$git clone git@github.com:liyue2008/kafka-flink-exactlyonce-example.git
$cd kafka-flink-exactlyonce-example
$mvn package
```

## 下载启动Flink

去[Flink官网下载页面](https://flink.apache.org/downloads.html)，下载Apache Flink 1.9.0 for Scala 2.12，文件名为：flink-1.9.0-bin-scala_2.12.tgz。

解压到目录：flink-1.9.0

修改flink-1.9.0/conf/flink-conf.yaml，将槽数改为8：

```yaml
 # The number of task slots that each TaskManager offers. Each slot runs one parallel pipeline.

taskmanager.numberOfTaskSlots: 8
```

启动Flink集群：

```bash
$ bin/start-cluster.sh
Starting cluster.
Starting standalonesession daemon on host localhost.
Starting taskexecutor daemon on host localhost.
```

## 下载启动Kafka

去[Kafka官网下载页面](https://kafka.apache.org/downloads)下载[kafka_2.12-2.3.0.tgz](https://www.apache.org/dyn/closer.cgi?path=/kafka/2.3.0/kafka_2.12-2.3.0.tgz)。

解压到目录：kafka_2.12-2.3.0

修改kafka_2.12-2.3.0/config/server.properties，设置事务超时：

```properties
max.transaction.timeout.ms=90000
```

启动ZooKeeper和Kafka：

```bash
$cd kafka_2.12-2.3.0
$bin/zookeeper-server-start.sh config/zookeeper.properties
$bin/kafka-server-start.sh config/server.properties
```

创建主题

```bash
$bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic ip_count_source
$bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic ip_count_sink
```

## 启动模拟日志Kafka Producer

首先需要启动模拟日志的Kafka Producer，作为数据源

```bash
$cd kafka-flink-exactlyonce-example
$java -jar nginx-log-producer/target/nginx-log-producer-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## 提交任务

```bash
$flink-1.9.0/bin/flink run ip-count/target/ip-count-1.0-SNAPSHOT.jar
Starting execution of program
```

## 消费Kafka Topic ip_count_sink查看结果

注意设置isolation.level=read_committed，否则会消费到未提交的事务消息。

```bash
$cd kafka_2.12-2.3.0
$bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --property isolation.level=read_committed --topic ip_count_sink
2019-09-15_16:23:35 192.168.1.1 5
2019-09-15_16:23:35 192.168.1.4 5
2019-09-15_16:23:35 192.168.1.3 5
2019-09-15_16:23:35 192.168.1.2 5
2019-09-15_16:23:40 192.168.1.2 5
2019-09-15_16:23:40 192.168.1.1 5
2019-09-15_16:23:40 192.168.1.4 5
2019-09-15_16:23:40 192.168.1.3 5
2019-09-15_16:23:45 192.168.1.2 5
2019-09-15_16:23:45 192.168.1.3 5
```
