package com.sk.simulators;


import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class DataSimulator
{
    public static void main( String[] args )
    {
        InputStream is = ClassLoader.class.getResourceAsStream("/data-ingest.yaml");;
        Yaml yaml = new Yaml();
        Map config = (Map) yaml.load(is);
        Producer producer = new Producer();
        try {
            Producer.produce(producer.kafkaProperties(config), config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
