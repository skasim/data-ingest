package com.sk.bolts;

import ch.hsr.geohash.GeoHash;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Map;

import static com.sk.constants.GeohashConstants.GEOHASH_PRECISION;

/**
 * Created by SamK on 12/5/16.
 */
public class GeohashBolt extends BaseBasicBolt {
//    Map _map;
//    TopologyContext _toppologyContext;
//    OutputCollector _outputCollector;

//    @Override
//    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
//        this._map = map;
//        this._toppologyContext = topologyContext;
//        this._outputCollector = outputCollector;
//    }

//    @Override
//    public void execute(Tuple tuple) {
//        String tupleString = tuple.getString(0);
//        JSONParser parser = new JSONParser();
//        JSONObject json = null;
//        try {
//            json = (JSONObject) parser.parse(tupleString);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        Object lat = json.get("PRIM_LAT_DEC");
//        Object lon = json.get("PRIM_LONG_DEC");
//
//        if (!(lat==null) || !(lon==null)){
//            String geohash = String.valueOf(GeoHash.withCharacterPrecision(Double.valueOf(lat.toString()),
//                    Double.valueOf(lon.toString()), GEOHASH_PRECISION));
//            geohash = geohash.substring(geohash.length() - GEOHASH_PRECISION);
//
//            json.put("geohash", geohash);
//            System.out.println(json.toJSONString());
//            _outputCollector.emit(tuple, new Values(json.toJSONString()));
//            _outputCollector.ack(tuple);
//
//        } else {
//            _outputCollector.ack(tuple);
//        }
//    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("json"));

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String tupleString = tuple.getString(0);
        JSONParser parser = new JSONParser();
        JSONObject json = null;
        try {
            json = (JSONObject) parser.parse(tupleString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Object lat = json.get("PRIM_LAT_DEC");
        Object lon = json.get("PRIM_LONG_DEC");

        if (!(lat == null) || !(lon == null)) {
            String geohash = String.valueOf(GeoHash.withCharacterPrecision(Double.valueOf(lat.toString()),
                    Double.valueOf(lon.toString()), GEOHASH_PRECISION));
            geohash = geohash.substring(geohash.length() - GEOHASH_PRECISION);

            json.put("geohash", geohash);
            System.out.println(json.toJSONString());
            basicOutputCollector.emit(new Values(json.toJSONString()));
        }
    }
}
