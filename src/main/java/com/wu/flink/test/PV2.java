package com.wu.flink.test;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class PV2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<pvbean> filterds = env2.readTextFile("D:\\ideacode\\flink\\input\\UserBehavior.csv")
                .map(new MapFunction<String, pvbean>() {
                    @Override
                    public pvbean map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new pvbean(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                Integer.valueOf(datas[2]),
                                datas[3],
                                Long.valueOf(datas[4]));
                    }
                }).filter(r -> "pv".equals(r.getBehavior()));
        filterds.process(new ProcessFunction<pvbean, Long>() {
            private Long pvcount=0L;
            @Override
            public void processElement(pvbean pvbean, Context context, Collector<Long> collector) throws Exception {
                             pvcount++;
                             collector.collect(pvcount);
            }
        }).setParallelism(1).print();
        env2.execute();


    }


}
