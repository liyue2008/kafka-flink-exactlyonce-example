package com.github.liyue2008.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author LiYue
 * Date: 2019-09-15
 */
public class NginxLogProducer {
    private static final Logger logger = LoggerFactory.getLogger(NginxLogProducer.class);
    private final static List<String> ipList = Arrays.asList("192.168.1.1","192.168.1.2","192.168.1.3","192.168.1.4");
    private final static String topic = "ip_count_source";
    public static void main(String [] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        scheduledExecutorService.scheduleAtFixedRate(() -> {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
            long timestamp = System.currentTimeMillis();
            String date = sdf.format(new Date(timestamp));

            for (String ip : ipList) {
                final String msg = date + " " + ip;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, ip, msg);
                producer.send(record, (recordMetadata, e) -> {
                    if(null != e) {

                        logger.warn("Send failed: {}!", msg, e);
                        System.exit(1);
                    } else {
                        logger.info("Send success: {}.", msg);
                    }
                });

            }
        },0L, 1L, TimeUnit.SECONDS);

    }
}
