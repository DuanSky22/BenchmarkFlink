package com.duansky.benchmark.flink.app;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Created by DuanSky on 2016/12/7.
 */
public class DreamBill {

    public static void main(String args[]) throws Exception{
        String path = DreamBill.class.getClassLoader().getResource("")+"bills.txt";
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        calculate(env,path);
    }

    /**
     * the data format must like this.
     * p1,d1;p2,d2;p3,d3:a1,a2,a3,a4
     * @param env
     * @param path
     */
    public static void calculate(ExecutionEnvironment env,String path) throws Exception {
        DataSet<String> dataSet = env.readTextFile(path);
        dataSet
                .flatMap(new EveryOneBill())
                .groupBy(0)
                .reduce((ReduceFunction<Tuple2<String, Double>>) (value1, value2) -> new Tuple2<String,Double>(value1.f0,value1.f1 + value2.f1))
                .print();
    }

    public static class EveryOneBill implements FlatMapFunction<String,Tuple2<String,Double>>{
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Double>> out) throws Exception {
            String[] data = value.split(":");

            double totalMoney = 0.0;
            for(String payer : data[0].split(";")){
                String[] pair = payer.split(",");
                double thisPay = Double.parseDouble(pair[1]);
                totalMoney += thisPay; // calculate the total money.
                out.collect(new Tuple2<>(pair[0],thisPay));
            }

            String[] actors = data[1].split(",");
            double singlePay = totalMoney / actors.length;
            for(String actor : actors){
                out.collect(new Tuple2<>(actor,-singlePay));
            }
        }
    }
}
