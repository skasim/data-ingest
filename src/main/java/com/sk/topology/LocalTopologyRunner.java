package com.sk.topology;

import com.sk.bolts.GeohashBolt;
import com.sk.spout.SpoutBuilder;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import static com.sk.constants.DataIngestConstants.KAFKA_ZOOKEEPER;

/**
 * Created by SamK on 12/4/16.
 */
public class LocalTopologyRunner {
    private static final int TEN_MINUTES = 600000;

    public static void main(String[] args) {
        InputStream is = ClassLoader.class.getResourceAsStream("/data-ingest.yaml");;
        Yaml yaml = new Yaml();
        Map config = (Map) yaml.load(is);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-feed", new SpoutBuilder(config).initializeKafkaSpout());
        builder.setBolt("geohash-bolt", new GeohashBolt())
                .shuffleGrouping("kafka-feed");

        Config topologyConfig = new Config();
        topologyConfig.setDebug(true);

        StormTopology topology = builder.createTopology();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("data-ingest-topology", topologyConfig, topology);

        Utils.sleep(TEN_MINUTES);
        cluster.killTopology("data-ingest-topology");
        cluster.shutdown();


    }
}
