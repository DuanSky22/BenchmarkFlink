package com.duansky.benchmark.flink.test.components;

/**
 * GraphTemplate declares the basic elements of
 * a graph, in other words, its defined what a
 * graph looks like.
 *
 * Created by DuanSky on 2016/10/30.
 */
public interface GraphTemplate {

    /**
     * get the number of vertex.
     * @return the number of vertex.
     */
    int getVertexNumber();

    /**
     * get the probability of two vertex forms an edge.
     * @return the probability of two vertex forms an edge.
     */
    double getProbability();
}
