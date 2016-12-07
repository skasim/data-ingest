package com.sk.utils;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.elasticsearch.client.Requests.createIndexRequest;

/**
 * Created by SamK on 12/6/16.
 */
public class ESUtils {

    public static TransportClient initializeESTransportClient(){
        TransportClient client = null;
        try {
            client = TransportClient.builder().build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
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
        CreateIndexResponse createResponse = client.admin().indices().create(Requests.createIndexRequest("test1")).actionGet();
        return true;
    }
    //check if index exists
    public static boolean indexExists(String index, TransportClient client) {

        IndexMetaData indexMetaData = client.admin().cluster()
                .state(Requests.clusterStateRequest())
                .actionGet()
                .getState()
                .getMetaData()
                .index(index);

        return (indexMetaData != null);

    }
    //create an alias
    public static boolean addAlias(TransportClient client, String alias, String index){
        client.admin().indices().prepareAliases()
                .addAlias(index,  alias)
                .execute().actionGet();
        return true;
    }

    //index document
    public static boolean indexDocument(String index, TransportClient client, String json){
        IndexResponse response = client.prepareIndex(index, "type")
                .setSource(json)
                .get();

        return true;
    }

    //delete document (not urgent)
}
