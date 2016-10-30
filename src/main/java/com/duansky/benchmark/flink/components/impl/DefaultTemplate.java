package com.duansky.benchmark.flink.components.impl;

import com.duansky.benchmark.flink.components.GraphTemplate;

/**
 * Created by DuanSky on 2016/10/30.
 */
public class DefaultTemplate implements GraphTemplate {

    /**
     * The total number of vertex.
     */
    private int vertexNumber = 10;

    /**
     * The probability that arbitrary two vertex form an edge.
     */
    private double probability = 0.5;

    public DefaultTemplate(){super();}

    public DefaultTemplate(int vertexNumber, double probability){
        super();
        this.vertexNumber = vertexNumber;
        this.probability = probability;

    }

    public double getProbability() {
        return probability;
    }

    public void setProbability(double probability) {
        this.probability = probability;
    }

    public int getVertexNumber() {
        return vertexNumber;
    }

    public void setVertexNumber(int vertexNumber) {
        this.vertexNumber = vertexNumber;
    }


}
