package com.yigang.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Time：2019-11-12 15:41
 * Email： yiigang@126.com
 * Desc：生产者读取本地json文件，发送给kafka集群
 *
 * @author： 王一钢
 */
public class MyProducer {
    public static void main(String[] args) throws IOException {
        Properties prop = null;
        Producer<String, String> producer = null;
        try{
            prop = new Properties();
            prop.load(MyProducer.class.getClassLoader().getResourceAsStream("producer.properties"));
            producer = new KafkaProducer<String, String>(prop);
            BufferedReader reader = new BufferedReader(new FileReader(new File("data/flink/cmcc.json")));
            String line = null;
            while((line = reader.readLine()) != null){
                ProducerRecord<String,String> record = new ProducerRecord<String, String>("pay1",line);
                producer.send(record);
            }
            reader.close();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(producer != null){
                producer.close();
            }
        }
    }
}
