package com.sk.spout;

import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;

import java.util.Map;
import java.util.Properties;

import static com.sk.constants.DataIngestConstants.*;

/**
 * Created by SamK on 12/4/16.
 */
public class SpoutBuilder {
    private Map config;

    public SpoutBuilder(Map config){
        this.config = config;
    }

    public KafkaSpout initializeKafkaSpout(){

        String zkHosts          = String.valueOf(config.get(KAFKA_ZOOKEEPER));
        String topic            = String.valueOf(config.get(KAFKA_DATA_TOPIC));
        String zkroot           = String.valueOf(config.get(KAFKA_ZKROOT));
        String consumerGroupID  = String.valueOf(config.get(KAFKA_CONSUMER_GROUP_ID));

        BrokerHosts hosts = new ZkHosts(zkHosts);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkroot, consumerGroupID);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.startOffsetTime = -1; //start where ZK last left off. Necessary, otherwise Kafka emitting all messages
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        return kafkaSpout;
    }

}
