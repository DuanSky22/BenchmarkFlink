package com.duansky.benchmark.flink.analysis.dataset.transformations;

import com.duansky.benchmark.flink.analysis.dataset.transformations.pojo.WC;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/10.
 */
public class _KeySelector {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        List<WC> list = new ArrayList<WC>(10);
        list.add(new WC("a",1)); list.add(new WC("b",1));
        list.add(new WC("b",2)); list.add(new WC("b",1));
        DataSet<WC> words = env.fromCollection(list);

        words
                .groupBy(new SelectWord())
                .reduce(new WordCounter())
                .print()
                ;
    }

    public static class WordCounter implements ReduceFunction<WC>{
        @Override
        public WC reduce(WC value1, WC value2) throws Exception {
            return new WC(value1.getWord(),value1.getCount()+value2.getCount());
        }
    }

    public static class SelectWord implements KeySelector<WC,String>{
        @Override
        public String getKey(WC value) throws Exception {
            return value.getWord();
        }
    }
}
