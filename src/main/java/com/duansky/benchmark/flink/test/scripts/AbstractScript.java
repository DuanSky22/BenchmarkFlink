package com.duansky.benchmark.flink.test.scripts;

import com.duansky.benchmark.flink.test.components.GraphGenerator;
import com.duansky.benchmark.flink.test.components.GraphTemplate;
import com.duansky.benchmark.flink.test.components.PathTransformer;
import com.duansky.benchmark.flink.test.components.impl.DefaultGraphGenerator;
import com.duansky.benchmark.flink.test.components.impl.DefaultPathTransformer;
import com.duansky.benchmark.flink.test.driver.GraphTemplateFactory;
import com.duansky.benchmark.flink.test.util.Contract;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.io.FileNotFoundException;
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
    public static final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

    static{
        env.getConfig().disableSysoutLogging();
        senv.getConfig().disableSysoutLogging();
    }

    /** input data **/
    protected final GraphTemplate[] templates;

    protected String scriptName = "abstract script";

    /** output result **/
    protected String resPath = Contract.BASE_FOLD + File.separator + scriptName;
    PrintWriter writer;


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
<<<<<<< HEAD
        System.out.println("Start[ "+ getScriptName()+" ]...");
=======
        System.out.println("Start test["+ getScriptName()+"]...");
>>>>>>> f5c121b6145faeb1dea33d104d9b3213dfde929c
        String res;
        for(GraphTemplate template : templates) {
            res = runInternal(template);
            if(res != null){
                System.out.println(res);
                writeResult(res);
            }else{
                System.out.println("the result is null");
            }

        }
    }

    public String getScriptName() {
        return scriptName;
    }

    public void setScriptName(String scriptName) {
        this.scriptName = scriptName;
        this.resPath = Contract.BASE_FOLD + File.separator + scriptName + ".txt";
    }

    public String getResPath() {
        return resPath;
    }

    public void setResPath(String resPath) {
        this.resPath = resPath;
        try {
            writer = new PrintWriter(resPath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    protected abstract String runInternal(GraphTemplate template) throws Exception;

    protected void writeResult(String result) throws Exception{
        writer.write(result);
        writer.flush();
    }
}
