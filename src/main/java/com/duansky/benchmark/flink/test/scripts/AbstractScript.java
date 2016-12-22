package com.duansky.benchmark.flink.test.scripts;

import com.duansky.benchmark.flink.test.components.GraphGenerator;
import com.duansky.benchmark.flink.test.components.GraphTemplate;
import com.duansky.benchmark.flink.test.components.PathTransformer;
import com.duansky.benchmark.flink.test.components.impl.DefaultGraphGenerator;
import com.duansky.benchmark.flink.test.components.impl.DefaultPathTransformer;
import com.duansky.benchmark.flink.test.driver.GraphTemplateFactory;
import com.duansky.benchmark.flink.test.util.Contract;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.File;
import java.io.PrintWriter;

/**
 * Created by DuanSky on 2016/12/22.
 */
public abstract class AbstractScript implements Script{

    /** tools **/
    public static final GraphGenerator graphGenerator = DefaultGraphGenerator.getInstance();
    public static final PathTransformer transformer = DefaultPathTransformer.getInstance();

    /**environment**/
    public static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    static{
        env.getConfig().disableSysoutLogging();
    }

    /** input data **/
    protected final GraphTemplate[] templates;

    /** output result **/
    protected String resPath = Contract.BASE_FOLD + File.separator + "abstract-script-result";

    protected String scriptName = "abstract script";

    public AbstractScript(){
        this.templates = GraphTemplateFactory.generateTemplates();
    }

    public AbstractScript(GraphTemplate[] templates){
        this.templates = templates;
    }

    public AbstractScript(String templatePath){
        this.templates = GraphTemplateFactory.generateTemplates(templatePath);
    }


    @Override
    public void run() throws Exception{
        System.out.println("Start test["+ getScriptName()+"]...");
        for(GraphTemplate template : templates)
            writeResult(runInternal(template));
    }

    public String getScriptName() {
        return scriptName;
    }

    public void setScriptName(String scriptName) {
        this.scriptName = scriptName;
    }

    public String getResPath() {
        return resPath;
    }

    public void setResPath(String resPath) {
        this.resPath = resPath;
    }

    protected abstract String runInternal(GraphTemplate template) throws Exception;

    protected void writeResult(String result) throws Exception{
        PrintWriter writer = new PrintWriter(resPath);
        writer.write(result);
    }
}
