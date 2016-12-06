package com.sk.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

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
        String testzz = String.valueOf(tuple.getValues());
        System.out.println("TESTZZZZZZZZ");
        System.out.println(testzz);
        //check if index exists in es
        //if it doesn't then create index with index name and alias
        //if it does then
        //post document

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
