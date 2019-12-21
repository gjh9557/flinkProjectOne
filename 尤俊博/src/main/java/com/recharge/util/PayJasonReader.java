package com.recharge.util;

import com.alibaba.fastjson.JSON;
import com.recharge.model.PayData;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class PayJasonReader implements SourceFunction<PayData> {
    private final String dataFilePath;
    private transient BufferedReader reader;

    public PayJasonReader(String dataFilePath) {
        this.dataFilePath = dataFilePath;
    }

    public void run(SourceContext<PayData> sourceContext) throws Exception {
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilePath), "UTF-8"));
        String jsonLine = new String();
        while (reader.ready() && (jsonLine = reader.readLine()) != null) {
            PayData payData = JSON.parseObject(jsonLine, PayData.class);
            sourceContext.collect(payData);
            Thread.sleep(500);
        }
        reader.close();
    }

    public void cancel() {

    }
}
