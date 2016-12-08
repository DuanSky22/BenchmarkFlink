package com.duansky.benchmark.flink.analysis.dataset.transformations;

import com.duansky.benchmark.flink.analysis.dataset.transformations.pojo.Coord;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/15.
 */
public class _Cross {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        List<Coord> a = new ArrayList<>(4);
        a.add(new Coord(1,1,1)); a.add(new Coord(2,2,2));
        List<Coord> b = new ArrayList<>(4);
        b.add(new Coord(3,3,3)); b.add(new Coord(4,4,4));

        DataSet<Coord> dataSet1 = env.fromCollection(a);
        DataSet<Coord> dataSet2 = env.fromCollection(b);

        dataSet1.cross(dataSet2)
                .with(new Distance())
                .print();
    }

    public static class Distance implements CrossFunction<Coord,Coord,Tuple3<Integer,Integer,Double>>{
        @Override
        public Tuple3<Integer, Integer, Double> cross(Coord val1, Coord val2) throws Exception {
            return new Tuple3<>(val1.id,val2.id,Math.sqrt(Math.pow(val1.x-val2.x,2)+Math.pow(val1.y-val2.y,2)));
        }
    }
}
