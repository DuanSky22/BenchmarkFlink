package com.duansky.benchmark.flink.test.scripts;

/**
 * Created by DuanSky on 2017/1/12.
 */
public class ScriptDriver {

    public static void main(String[] args) throws Exception {
        if(args == null || args.length == 0) throw new IllegalArgumentException("Invalid Parameters");
        else{
            String scriptName = args[0];
            String[] pars = new String[args.length-1];
            for(int i = 1; i < args.length; i++)
                pars[i-1] = args[i];
            runScript(scriptName,pars);
        }
    }

    private static void runScript(String scriptName, String[] pars) throws Exception {
        if(scriptName.contains("triangle_count"))
            TestTriangleCount.main(pars);
        else if(scriptName.contains("memeroy_use"))
            TestMemoryUse.main(pars);
    }
}
