package com.duansky.benchmark.flink.components;

/**
 * PathTransformer is used to transform a template to a file path.
 * This interface is used by {@link GraphWriter}.
 * Created by SkyDream on 2016/11/1.
 */
public interface PathTransformer {
    /**
     * get the file path which stored the verities of a graph defined
     * by {@link GraphTemplate} .
     * @param folder the folder that holds this verities file.
     * @param template the definition of this graph.
     * @return the file path which stored the verities of a graph defined
     * by this template.
     */
    String getVertexPath(String folder, GraphTemplate template);

    /**
     * get the file path which stored the edges of a graph defined
     * by {@link GraphTemplate} .
     * @param folder the folder that holds this edges file.
     * @param template the definition of this graph.
     * @return the file path which stored the edges of a graph defined
     * by this template.
     */
    String getEdgePath(String folder,GraphTemplate template);
}
