package com.sk;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.io.*;
import java.util.Map;
import java.util.Properties;

import static com.sk.constants.DataIngestConstants.*;

/**
 * Created by SamK on 12/3/16.
 */
public class Producer {

    public static void produce(Properties props, Map config) throws IOException {

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        BufferedReader br;
        String line = null;
        String header = null;
        String[] headerArray = null;
        JSONObject jsonObject = null;
        try {
            br = new BufferedReader(new FileReader(DATA_FILE_LOC));
            int totalLines = countLines(DATA_FILE_LOC);
            System.out.println(totalLines);

            //iterate over each line
            for (int i = 0; i<totalLines; i++){
                if (i==0){
                    header = br.readLine();
                    headerArray = header.split("\\|");
                    System.out.println(header);
                }
                else{
                    line = br.readLine();
                    String[] lineArr = line.split("\\|");
                    int k = 0;
                    jsonObject = new JSONObject();
                    while(k < headerArray.length-1){
                        jsonObject.put(headerArray[k],lineArr[k]);
                        k++;
                    }
                    //emit jsonObject to String
                    //System.out.println(String.valueOf(jsonObject));
                    producer.send(new ProducerRecord<String, String>(String.valueOf(config.get(KAFKA_DATA_TOPIC)), String.valueOf(jsonObject)));
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        producer.close();
    }

    public Properties kafkaProperties(Map config) {
        Properties props = new Properties();
        props.put("bootstrap.servers", config.get(KAFKA_SERVERS));
        props.put("acks", config.get(KAFKA_ACKS));
        props.put("retries", config.get(KAFKA_RETRIES));
        props.put("batch.size", config.get(KAFKA_BATCH_SIZE));
        props.put("linger.ms", config.get(KAFKA_LINGER_MS));
        props.put("buffer.memory", config.get(KAFKA_BUFFER_MEMORY));
        props.put("key.serializer", config.get(KAFKA_KEY_SERLALIZER));
        props.put("value.serializer", config.get(KAFKA_VALUE_SERIALIZER));

        return props;
    }

    private static int countLines(BufferedReader br){
        int totalLines = 0;
        try {
            while (br.readLine() != null) totalLines++;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalLines;
    }

    public static int countLines(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }
}
