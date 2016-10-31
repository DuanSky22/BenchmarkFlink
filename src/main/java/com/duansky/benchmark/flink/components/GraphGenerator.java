package com.duansky.benchmark.flink.components;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;

import java.util.List;

/**
 * Graph Generator is used to generate the specific graph.
 * Here we use strategy design. See the {@link GraphContext}
 * Created by DuanSky on 2016/10/29.
 */
public interface GraphGenerator {

    Graph generateGraph(ExecutionEnvironment env,GraphTemplate template);
}
