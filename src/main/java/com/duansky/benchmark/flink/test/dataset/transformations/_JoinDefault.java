package com.duansky.benchmark.flink.test.dataset.transformations;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/11.
 */
public class _JoinDefault {

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

        List<User> users = new ArrayList<>(4);
        users.add(new User("ua","1")); users.add(new User("ub","1"));
        List<Store> stores = new ArrayList<>(4);
        stores.add(new Store("sa","1")); stores.add(new Store("sb","1"));

        DataSet<User> dataSet1 = env.fromCollection(users);
        DataSet<Store> dataSet2 = env.fromCollection(stores);

        dataSet1
                .join(dataSet2)
                .where("zip")
                .equalTo("zip")
                .print();
    }
}

