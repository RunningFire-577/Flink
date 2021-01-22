package com.wu.flink.test;

import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<pvbean> ds = env.readTextFile("D:\\ideacode\\flink\\input\\UserBehavior.csv")
                .map(new MapFunction<String, pvbean>() {
                    @Override
                    public pvbean map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new pvbean(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                Integer.valueOf(datas[2]),
                                datas[3],
                                Long.valueOf(datas[4])
                        );
                    }
                });
        ds.filter(r->"pv".equals(r.getBehavior()))
                .map(new MapFunction<pvbean, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(pvbean value) throws Exception {
                        return Tuple2.of("pv",1L);
                    }
                }).keyBy(r->r.f0).sum(1).print();

        env.execute();
    }
}
