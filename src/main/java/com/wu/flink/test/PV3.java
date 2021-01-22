package com.wu.flink.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class PV3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env3 = StreamExecutionEnvironment.getExecutionEnvironment();
        env3.readTextFile("D:\\ideacode\\flink\\input\\UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] datas = value.split(",");
                        String behavior = datas[3];
                        if ("pv".equals(behavior)){
                            out.collect(Tuple2.of("pv",1L));
                        }
                    }
                }).keyBy(r->r.f0).sum(1).print();
        env3.execute();


    }
}
