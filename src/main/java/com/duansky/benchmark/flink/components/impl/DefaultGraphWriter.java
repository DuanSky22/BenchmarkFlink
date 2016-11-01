package com.duansky.benchmark.flink.components.impl;

import com.duansky.benchmark.flink.components.GraphTemplate;
import com.duansky.benchmark.flink.components.GraphWriter;
import com.duansky.benchmark.flink.components.PathTransformer;
import com.duansky.benchmark.flink.util.Files;
import com.duansky.benchmark.flink.util.Maths;

import java.io.PrintWriter;
import java.util.Random;

/**
 * Created by DuanSky on 2016/10/31.
 */
public class DefaultGraphWriter implements GraphWriter {

    private static Random RND = new Random();
    private static int THRESHOLD = 1000;

    private PathTransformer pathTransformer = DefaultPathTransformer.getInstance();

    private static DefaultGraphWriter INSTANCE = new DefaultGraphWriter();

    public static DefaultGraphWriter getInstance(){return INSTANCE;}

    private DefaultGraphWriter(){}

    @Override
    public void writeAsFile(String folder, GraphTemplate template) {
        //check folder exists.
        Files.checkAndCreateFolder(folder);

        //write the edge of this graph.
        PrintWriter edgeWriter =
                Files.asPrintWriter(pathTransformer.getEdgePath(folder,template));
        if(edgeWriter != null){
            int n = template.getVertexNumber();
            if(n <= THRESHOLD) writeEdgeDirectly(edgeWriter,template);
            else writeEdgeUseProbability(edgeWriter,template);
        }

        //write the vertex of this graph.
        PrintWriter vertexWriter =
                Files.asPrintWriter(pathTransformer.getVertexPath(folder,template));
        if(vertexWriter != null){
            writeVertex(vertexWriter,template);
        }
    }

    private void writeVertex(PrintWriter writer,GraphTemplate template){
        int n = template.getVertexNumber();
        for(int i = 0; i < n; i++){
            writer.write(i+"\n");
        }
        writer.close();
    }

    private void writeEdgeUseProbability(PrintWriter writer, GraphTemplate template){
        int n = template.getVertexNumber();
        double p = template.getProbability();
        for(int i = 0; i < n-1; i++){
            for(int j = i+1; j < n; j++){
                if(RND.nextDouble() <= p) {
                    writer.write(String.format("%s,%s\n", i, j));
                    writer.flush();
                }
            }
        }
        writer.close();
    }

    private void writeEdgeDirectly(PrintWriter writer, GraphTemplate template){
        int n = template.getVertexNumber();
        double p = template.getProbability();
        //the random edge.
        int[][] e = Maths.getRandomUndirectedPairs(n, (int)(Maths.getCombinationsNumber(n,2) * p));
        for(int i = 0; i < e.length; i++){
            writer.write(String.format("%s,%s\n", e[i][0],e[i][1]));
            writer.flush();
        }
        writer.close();
    }


}
