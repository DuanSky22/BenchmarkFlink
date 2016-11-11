package com.duansky.benchmark.flink.test.dataset.transformations;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Created by DuanSky on 2016/11/10.
 */
public class _Filter {



    public static class NaturalNumberFilter implements FilterFunction<Integer>{
        public boolean filter(Integer value) throws Exception {
            return value >= 0;
        }
    }
}
