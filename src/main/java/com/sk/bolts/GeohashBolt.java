package com.sk.bolts;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


import java.util.List;
import java.util.Map;

/**
 * Created by SamK on 12/5/16.
 */
public class GeohashBolt extends BaseBasicBolt {

    @Override
    public void prepare (Map config, TopologyContext content) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String tupleString = tuple.getString(0);
        System.out.println("TESTZZZ"+tupleString);
        JSONParser parser = new JSONParser();
        JSONObject json = null;
        try {
            json = (JSONObject) parser.parse(tupleString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        System.out.printf("PRIMARY LAT");
        System.out.println(json.get("PRIM_LAT_DEC"));
        //get the lat and lon
        // use late and lon to create a geohash
        //put the geohash into the json object...emit either object or string
        //next bolt is ES bolt
        //ES bolt needs to convert string to json and persist

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
