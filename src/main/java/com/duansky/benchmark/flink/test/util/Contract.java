package com.duansky.benchmark.flink.test.util;

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
     * the folder store the graph csv file.
     */
    String DATA_FOLDER_GELLY = System.getProperty("user.dir") +
            File.separator + "flink-gelly";

    /**
     * the file that store the triangle count test result.
     */
    String TRIANGLE_COUNT_RESULT = System.getProperty("user.dir")
            + File.separator + "triangle_count_test.txt";
}
