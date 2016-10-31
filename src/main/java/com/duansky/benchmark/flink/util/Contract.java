package com.duansky.benchmark.flink.util;

import java.io.File;

/**
 * Created by DuanSky on 2016/10/31.
 */
public interface Contract {
    /**
     * the remote host.
     */
    String host = "133.133.169.122";

    /**
     * the remote port.
     */
    int port = 6123;

    /**
     * the folder of the graph csv file.
     */
    String DATA_FOLDER = System.getProperty("user.dir") +
            File.separator + "flink-gelly";
}
