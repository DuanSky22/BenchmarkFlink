package com.duansky.benchmark.flink.test.dataset.transformations;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by DuanSky on 2016/11/14.
 */
public class _JoinFunction {

    public static class Product{
        public String name;
        public double price;
        public Product(){}
        public Product(String name,double price){
            this.name = name;
            this.price = price;
        }

        @Override
        public String toString() {
            return "Product{" +
                    "name='" + name + '\'' +
                    ", price=" + price +
                    '}';
        }
    }
    public static class Discount{
        public String name;
        public double discount;
        public Discount(){}
        public Discount(String name,double discount){
            this.name = name;
            this.discount = discount;
        }

        @Override
        public String toString() {
            return "Discount{" +
                    "discount=" + discount +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        List<Product> products = new ArrayList<>(4);
        products.add(new Product("p1",100)); products.add(new Product("p2",10));
        List<Discount> discounts = new ArrayList<>(4);
        discounts.add(new Discount("p1",0.9)); discounts.add(new Discount("p2",0.8));

        DataSet<Product> dataSet1 = env.fromCollection(products);
        DataSet<Discount> dataSet2 = env.fromCollection(discounts);

        dataSet1.join(dataSet2)
                .where("name")
                .equalTo("name")
                .with(new MyJoinFunction())
                .print();
    }

    public static class MyJoinFunction implements JoinFunction<Product,Discount,Product>{
        @Override
        public Product join(Product product, Discount discount) throws Exception {
            return new Product(product.name,product.price * discount.discount);
        }
    }
}
