package com.duansky.benchmark.flink.driver;

import com.duansky.benchmark.flink.components.GraphTemplate;
import com.duansky.benchmark.flink.components.GraphWriter;
import com.duansky.benchmark.flink.components.impl.DefaultGraphWriter;
import com.duansky.benchmark.flink.util.Contract;

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
            writer.writeAsFile(Contract.DATA_FOLDER,template);
            System.out.println(String.format("write graph(%s,%s) done.",
                    template.getVertexNumber(),
                    template.getProbability()));
        }
    }

    public static void main(String args[]){
        DataDriver driver = new DataDriver();
        if(args != null && args.length == 1)
            driver.generateAndWriteGraphs(System.getProperty("user.dir")+ File.separator+args[0]);
        else
            driver.generateAndWriteGraphs();
    }
}
