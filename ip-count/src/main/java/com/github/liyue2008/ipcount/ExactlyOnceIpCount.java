package com.github.liyue2008.ipcount;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * @author LiYue
 * Date: 2019-09-15
 */
public class ExactlyOnceIpCount {
    public static void main(String[] args) throws Exception {
        Properties consumerProperties = setupKafkaConsumerProperties();
        Properties producerProperties = setupKafkaProducerProperties();

        FlinkKafkaConsumer011<IpAndCount> sourceConsumer = new FlinkKafkaConsumer011<>("ip_count_source", new AbstractDeserializationSchema<IpAndCount>() {
            @Override
            public IpAndCount deserialize(byte[] bytes) { // 数据转换：将非结构化的以空格分隔的文本转成结构化数据IpAndCount
                String str = new String(bytes, StandardCharsets.UTF_8);
                String [] splt = str.split("\\s");
                try {
                    return new IpAndCount(new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").parse(splt[0]),splt[1], 1L);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        }, consumerProperties);





        sourceConsumer.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<IpAndCount>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(IpAndCount ipAndCount) {
                return ipAndCount.getDate().getTime();// 告诉Flink时间从哪个字段中获取
            }
        });
        // 获取运行时环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 按照EventTime来统计
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.enableCheckpointing(10000); // 每5秒保存一次CheckPoint
        CheckpointConfig config = env.getCheckpointConfig();
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // CheckPoint设置为EXACTLY_ONCE
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // 取消任务时保留CheckPoint
        config.setPreferCheckpointForRecovery(true); // 启动时恢复CheckPoint
        // CheckPoint保存在本地临时目录中，只适合单节点做实验
        File tmpDirFile = new File(System.getProperty("java.io.tmpdir"));
        env.setStateBackend((StateBackend) new FsStateBackend(tmpDirFile.toURI().toURL().toString()));



        // 定义输入：从Kafka中获取数据
        DataStream<IpAndCount> input = env
                .addSource(sourceConsumer);

        // 计算：每5秒钟按照ip对count求和
        DataStream<IpAndCount> output =
                input
                .keyBy(IpAndCount::getIp) // 按照ip地址统计
                .window(TumblingEventTimeWindows.of(Time.seconds(5))) // 每5秒钟统计一次
                .allowedLateness(Time.seconds(5))
                .sum("count"); // 对count字段求和

        // 输出到kafka topic
        FlinkKafkaProducer011<String> sinkProducer = new FlinkKafkaProducer011<>(
                "ip_count_sink",                  // target topic
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), producerProperties, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);   // serialization schema

        output.map(IpAndCount::toString).addSink(sinkProducer);
        // execute program
        env.execute("Exactly-once IpCount");
    }

    private static Properties setupKafkaProducerProperties() {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", "localhost:9092");
        producerProperties.put("transaction.timeout.ms", "60000");
        return producerProperties;
    }


    private static Properties setupKafkaConsumerProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "IpCount");
        return properties;

    }
}
