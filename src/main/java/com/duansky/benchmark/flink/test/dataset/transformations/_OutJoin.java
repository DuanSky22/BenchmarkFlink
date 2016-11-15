package com.duansky.benchmark.flink.test.dataset.transformations;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/14.
 */
public class _OutJoin {
    public static class User{
        public String name;
        public String zip;
        public User(){}
        public User(String name,String zip){
            this.name = name;
            this.zip = zip;
        }

        @Override
        public String toString() {
            return "User{" +
                    "name='" + name + '\'' +
                    ", zip='" + zip + '\'' +
                    '}';
        }
    }

    public static class Store{
        public String mgr;
        public String zip;

        public Store(){}
        public Store(String mgr,String zip){
            this.mgr = mgr;
            this.zip = zip;
        }

        @Override
        public String toString() {
            return "Store{" +
                    "mgr='" + mgr + '\'' +
                    ", zip='" + zip + '\'' +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        List<_JoinDefault.User> users = new ArrayList<>(4);
        users.add(new _JoinDefault.User("ua","1")); users.add(new _JoinDefault.User("ub","3"));
        List<_JoinDefault.Store> stores = new ArrayList<>(4);
        stores.add(new _JoinDefault.Store("sa","1")); stores.add(new _JoinDefault.Store("sb","2"));

        DataSet<_JoinDefault.User> dataSet1 = env.fromCollection(users);
        DataSet<_JoinDefault.Store> dataSet2 = env.fromCollection(stores);

        dataSet1
//                .join(dataSet2)
                .fullOuterJoin(dataSet2)
                .where("zip")
                .equalTo("zip")///
                .with(new JoinFunction<_JoinDefault.User, _JoinDefault.Store, Tuple3<String,String,String>>() {
                    @Override
                    public Tuple3<String, String, String> join(_JoinDefault.User first, _JoinDefault.Store second) throws Exception {
                        if(second == null)
                            return new Tuple3<String, String, String>(first.name,"null",first.zip);
                        else if(first == null)
                            return new Tuple3<String, String, String>("null",second.mgr,second.zip);
                        else
                            return new Tuple3<String, String, String>(first.name,second.mgr,first.zip);

                    }
                })
                .print();
    }
}
