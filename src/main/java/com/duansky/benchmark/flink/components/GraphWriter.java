package com.duansky.benchmark.flink.components;

/**
 * Created by DuanSky on 2016/10/31.
 */
public interface GraphWriter {

    /**
     * write this graph defined by {@link GraphTemplate} to a file.
     * @param template
     */
    void writeAsFile(String path,GraphTemplate template);
}
