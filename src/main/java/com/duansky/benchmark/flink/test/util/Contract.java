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

    String BASE_FOLD = System.getProperty("user.dir");

    /**
     * the folder store the graph csv file.
     */
    String DATA_FOLDER_GELLY = BASE_FOLD +
            File.separator + "flink-gelly";

    /**
     * the file that store the triangle count test result.
     */
    String TRIANGLE_COUNT_RESULT = BASE_FOLD
            + File.separator + "triangle_count_test.txt";
}
