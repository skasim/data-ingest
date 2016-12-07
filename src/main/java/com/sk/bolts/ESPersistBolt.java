package com.sk.bolts;

import com.sk.utils.ESUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.client.transport.TransportClient;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Map;

/**
 * Created by SamK on 12/6/16.
 */
public class ESPersistBolt extends BaseBasicBolt {
    Map _map;
    TopologyContext _toppologyContext;
    OutputCollector _outputCollector;

//    @Override
//    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
//        this._map = map;
//        this._toppologyContext = topologyContext;
//        this._outputCollector = outputCollector;
//    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector _outputCollector) {
        String tupleString = String.valueOf(tuple.getString(0));
        System.out.println("TESTZZZZZZZZ");
        System.out.println(tupleString);

        JSONParser parser = new JSONParser();
        JSONObject json = null;
        try {
            json = (JSONObject) parser.parse(tupleString);
        } catch (ParseException e) {
            e.printStackTrace();

        }

            TransportClient client = ESUtils.initializeESTransportClient();
        if(!(ESUtils.indexExists("test1", client))){
            ESUtils.createESIndex(client);
        }
        ESUtils.indexDocument("test1", client, json.toJSONString());


        ESUtils.terminateESTransportClient(client);

        //check if index exists in es
        //if it doesn't then create index with index name and alias
        //if it does then
        //post document


        //next time
        //i need to add an alias to my index
        //actually have the data post to index
        //make the index name configurable
        //i think the acking and failing is off bc there are a lot of messages in kafka queue

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //no fields to declare because final bolt in topology
    }
}
