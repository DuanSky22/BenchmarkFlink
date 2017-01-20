package com.duansky.benchmark.flink.test.driver;

import com.duansky.benchmark.flink.test.components.GraphTemplate;
import com.duansky.benchmark.flink.test.components.GraphWriter;
import com.duansky.benchmark.flink.test.components.impl.DefaultGraphWriter;
import com.duansky.benchmark.flink.test.components.impl.DefaultTemplate;
import com.duansky.benchmark.flink.test.util.Contract;

import java.io.File;

/**
 * Write graph data into files.
 * Created by DuanSky on 2016/10/31.
 */
public class DataDriver {

    private GraphWriter writer = DefaultGraphWriter.getInstance();

    /**
     * generate graphs use {@link GraphTemplateFactory} and write them
     * into files.
     */
    public void generateAndWriteGraphs(){
        //get all the templates.
        GraphTemplate[] templates = GraphTemplateFactory.generateTemplates();
        generateAndWriteGraphs(templates);
    }

    public void generateAndWriteGraphs(String propertiesPath){
        //get all the templates.
        GraphTemplate[] templates = GraphTemplateFactory.generateTemplates(propertiesPath);
        generateAndWriteGraphs(templates);
    }

    public void generateAndWriteGraphs(GraphTemplate template){
        GraphTemplate[] templates = new GraphTemplate[]{template};
        generateAndWriteGraphs(templates);
    }

    /**
     * generate graphs use {@link GraphTemplate}s you input and write them into
     * files.
     * @param templates the graph templates you want to generate.
     */
    public void generateAndWriteGraphs(GraphTemplate... templates){
        //write the graphs.
        writeGraphs(templates);
    }

    private void writeGraphs(GraphTemplate[] templates){
        for(GraphTemplate template : templates){
            writer.writeAsFile(Contract.DATA_FOLDER_GELLY,template);
            System.out.println(String.format("write graph(%s,%s) done.",
                    template.getVertexNumber(),
                    template.getProbability()));
        }
    }

    public static void main(String args[]){
        DataDriver driver = new DataDriver();
        if(args == null || args.length == 0) driver.generateAndWriteGraphs();
        else if(args.length == 1) driver.generateAndWriteGraphs(System.getProperty("user.dir")+ File.separator+args[0]);
        else if(args.length == 2) driver.generateAndWriteGraphs(new DefaultTemplate(Integer.parseInt(args[0]),Double.parseDouble(args[1])));
        else throw new IllegalArgumentException("Invalid Parameters!");
    }
}
