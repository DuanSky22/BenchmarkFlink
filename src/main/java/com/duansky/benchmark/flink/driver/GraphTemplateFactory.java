package com.duansky.benchmark.flink.driver;

import com.duansky.benchmark.flink.components.GraphTemplate;
import com.duansky.benchmark.flink.components.impl.DefaultTemplate;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by DuanSky on 2016/10/31.
 */
public class GraphTemplateFactory {

    public static int[] VERTEX_NUMBERS = {1000,2000,5000,8000,10000,20000,50000,80000,100000,200000,500000,800000};
    public static double[] PROBABILITIES = {0.1,0.2,0.5,0.8,1.0};

    public static GraphTemplate[] generateTemplates(){
        int vn = VERTEX_NUMBERS.length, pn = PROBABILITIES.length, total = vn * pn;
        GraphTemplate[] templates = new GraphTemplate[total];
        int curr = 0;
        for(int i = 0; i < VERTEX_NUMBERS.length; i++){
            for(int j = 0; j < PROBABILITIES.length; j++){
                templates[curr++] = new DefaultTemplate(VERTEX_NUMBERS[i], PROBABILITIES[j]);
            }
        }
        return templates;
    }

    public static GraphTemplate[] generateTemplates(String propertiesPath){
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(propertiesPath));
            VERTEX_NUMBERS = getVertexNumbers(properties);
            PROBABILITIES = getProbabilities(properties);
            return generateTemplates();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static double[] getProbabilities(Properties properties) {
        if(properties.containsKey("probabilities")){
            String[] ps = properties.getProperty("probabilities").split(",");
            double[] res = new double[ps.length];
            for(int i = 0; i < res.length; i++){
                res[i] = Double.parseDouble(ps[i]);
            }
            return res;
        }
        return PROBABILITIES;
    }

    private static int[] getVertexNumbers(Properties properties) {
        if(properties.containsKey("verities")){
            String[] vs = properties.getProperty("verities").split(",");
            int[] res = new int[vs.length];
            for(int i = 0; i < res.length; i++){
                res[i] = Integer.parseInt(vs[i]);
            }
            return res;
        }
        return VERTEX_NUMBERS;
    }


}
