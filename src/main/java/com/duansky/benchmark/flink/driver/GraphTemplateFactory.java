package com.duansky.benchmark.flink.driver;

import com.duansky.benchmark.flink.components.GraphTemplate;
import com.duansky.benchmark.flink.components.impl.DefaultTemplate;
import com.duansky.benchmark.flink.util.Contract;

import java.io.File;

/**
 * Created by DuanSky on 2016/10/31.
 */
public class GraphTemplateFactory {

    public static String FOLDER = Contract.DATA_FOLDER;
    public static boolean checked = false;
    public static int[] VERTEX_NUMBERS = {1000,10000,100000,1000000,10000000};
    public static double[] PROBABILITYS = {0.1,0.2,0.5,0.8,1.0};

    public static GraphTemplate[] generateTemplates(){
        int vn = VERTEX_NUMBERS.length, pn = PROBABILITYS.length, total = vn * pn;
        GraphTemplate[] templates = new GraphTemplate[total];
        int curr = 0;
        for(int i = 0; i < VERTEX_NUMBERS.length; i++){
            for(int j = 0; j <PROBABILITYS.length; j++){
                templates[curr++] = new DefaultTemplate(VERTEX_NUMBERS[i],PROBABILITYS[j]);
            }
        }
        return templates;
    }

    public static String createPath(GraphTemplate template){
        if(checked == false) createFolder();
        return String.format("%s%sgraph-%s-%s.txt", FOLDER, File.separator,template.getVertexNumber(),template.getProbability());
    }

    private static void createFolder(){
        File file = new File(FOLDER);
        if(!file.exists()) file.mkdir();
        checked = true;
    }
}
