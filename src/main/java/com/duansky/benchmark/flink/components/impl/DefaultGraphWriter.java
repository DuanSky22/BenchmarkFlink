package com.duansky.benchmark.flink.components.impl;

import com.duansky.benchmark.flink.components.GraphTemplate;
import com.duansky.benchmark.flink.components.GraphWriter;
import com.duansky.benchmark.flink.util.Maths;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

/**
 * Created by DuanSky on 2016/10/31.
 */
public class DefaultGraphWriter implements GraphWriter {

    private static Random RND = new Random();
    private static int THRESHOLD = 1000;

    private static DefaultGraphWriter INSTANCE = new DefaultGraphWriter();

    public static DefaultGraphWriter getInstance(){return INSTANCE;}

    private DefaultGraphWriter(){}

    @Override
    public void writeAsFile(String path, GraphTemplate template) {
        PrintWriter writer = asPrintWriter(path);
        if(writer != null){
            int n = template.getVertexNumber();
            if(n <= THRESHOLD) writeDirectly(writer,path,template);
            else writeUseProbability(writer,path,template);
        }

    }

    private void writeUseProbability(PrintWriter writer,String path, GraphTemplate template){
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

    private void writeDirectly(PrintWriter writer,String path,GraphTemplate template){
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

    private PrintWriter asPrintWriter(String path){
        File file = new File(path);
        try {
            if(!file.exists()) {
                file.createNewFile();
            }
            return new PrintWriter(file);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
