package com.wu.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.util.Collector;


public class WC_Batch {

    public static void main(String[] args) throws Exception {
        //批处理
        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.读取数据
        DataSource<String> intputDS = env.readTextFile("D:\\ideacode\\flink\\input\\word.txt");

        //3.处理数据 先切分 再转换成元组  分组 聚合
        FlatMapOperator<String, String> wordDS = intputDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
        MapOperator<String, Tuple2<String, Long>> wordandone = wordDS.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return Tuple2.of(value, 1L);
            }
        });
        UnsortedGrouping<Tuple2<String, Long>> wordandOne = wordandone.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> resultDS = wordandOne.sum(1);

        //4.输出

        resultDS.print();




        //5.触发执行

    }
}
