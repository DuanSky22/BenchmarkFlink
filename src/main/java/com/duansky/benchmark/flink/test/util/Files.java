package com.duansky.benchmark.flink.test.util;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by SkyDream on 2016/11/1.
 */
public class Files {

    /**
     * get the {@link PrintWriter} of this path.
     * @param path the PrintWriter you want to create to.
     * @return the PrintWriter of your given path.
     */
    public static PrintWriter asPrintWriter(String path){
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

    /**
     * check this folder exists. if not create a new folder.
     * @param folder the folder that will be checked.
     * @return true if this folder alreay exists, otherwise false.
     */
    public static boolean checkAndCreateFolder(String folder){
        File file = new File(folder);
        if(!file.exists()){
            file.mkdir();
            return false;
        }
        return true;
    }
}
