package com.sk.utils;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.Transport;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import static com.sk.constants.DataIngestConstants.*;
import static org.elasticsearch.client.Requests.createIndexRequest;

/**
 * Created by SamK on 12/6/16.
 */
public class ESUtils {

    private static Map loadConfigs(){
        //TODO Load config object only once and not every time you call below methods
        //TODO create a config object. instantiate in the prepare for ESPersistBolt as a global value
        //TODO pass that object execute
        InputStream is = ClassLoader.class.getResourceAsStream("/data-ingest.yaml");;
        Yaml yaml = new Yaml();

        Map config = (Map) yaml.load(is);
        return config;
    }

    public static TransportClient initializeESTransportClient(){
        Map config = loadConfigs();
        TransportClient client = null;
        try {
            client = TransportClient.builder().build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(String.valueOf(config.get(ELASTICSEARCH_HOST_ADDRESS))), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }

    public static void terminateESTransportClient(TransportClient client){
        client.close();
    }

    //create an index
    public static boolean createESIndex(TransportClient client){
        Map config = loadConfigs();
        CreateIndexResponse createResponse = client.admin().indices()
                .create(Requests.createIndexRequest(String.valueOf(config.get(ELASTICSEARCH_INDEX_NAME)))).actionGet();
        return true;
    }
    //check if index exists
    public static boolean indexExists(TransportClient client) {
        Map config = loadConfigs();
        IndexMetaData indexMetaData = client.admin().cluster()
                .state(Requests.clusterStateRequest())
                .actionGet()
                .getState()
                .getMetaData()
                .index(String.valueOf(config.get(ELASTICSEARCH_INDEX_NAME)));

        return (indexMetaData != null);

    }
    //create an alias
    public static boolean createAlias(TransportClient client){
        Map config = loadConfigs();
        client.admin().indices().prepareAliases()
                .addAlias(String.valueOf(config.get(ELASTICSEARCH_INDEX_NAME)),
                        String.valueOf(config.get(ELASTICSEARCH_INDEX_ALIAS)))
                .execute().actionGet();

        return true;
    }

    //index document
    public static boolean indexDocument(TransportClient client, String json){
        Map config = loadConfigs();
        IndexResponse response = client.prepareIndex(String.valueOf(config.get(ELASTICSEARCH_INDEX_NAME)),
                String.valueOf(config.get(ELASTICSEARCH_INDEX_TYPE)))
                .setSource(json)
                .get();

        return true;
    }

    //delete document (not urgent)
}
