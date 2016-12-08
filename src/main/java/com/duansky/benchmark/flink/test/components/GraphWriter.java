package com.duansky.benchmark.flink.test.components;

/**
 * GraphWriter write the graph defined by {@link GraphTemplate}
 * to the specific folder.
 * It will generate two files. On stores the edge of this graph,
 * the other stores the vertex of this graph.
 *
 * Created by DuanSky on 2016/10/31.
 */
public interface GraphWriter {

    /**
     * write this graph defined by {@link GraphTemplate} to the
     * specific folder.
     * @param folder the folder which stores this graph.
     * @param template the defination of the graph.
     */
    void writeAsFile(String folder,GraphTemplate template);
}
