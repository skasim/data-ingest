package com.sk.bolts;

import com.sk.utils.ESUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
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
public class ESPersistBolt extends BaseRichBolt {
    Map _map;
    TopologyContext _toppologyContext;
    OutputCollector _outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._map = map;
        this._toppologyContext = topologyContext;
        this._outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String tupleString = String.valueOf(tuple.getString(0));

        JSONParser parser = new JSONParser();
        JSONObject json = null;
        try {
            json = (JSONObject) parser.parse(tupleString);
        } catch (ParseException e) {
            e.printStackTrace();
            _outputCollector.ack(tuple);
        }
            TransportClient client = ESUtils.initializeESTransportClient();
        //check if index exists in ES
        if(!(ESUtils.indexExists(client))){
            //if index doesn't exist, create it with an alias
            ESUtils.createESIndex(client);
            ESUtils.createAlias(client);
        }
        // if index exists, post document
        ESUtils.indexDocument(client, json.toJSONString());
        _outputCollector.ack(tuple);

        ESUtils.terminateESTransportClient(client);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //no fields to declare because final bolt in topology
    }
}
