package com.duansky.benchmark.flink.test.util;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by DuanSky on 2016/10/30.
 */
public class MathsTest {
    @Test
    public void getCombinationsNumberTest() throws Exception {
        int n = 10;
        int k = 3;
        assertEquals(Maths.getCombinationsNumber(n,k),120);
    }

    @Test
    public void getCombinationsTest(){
        int n = 10;
        int k = 2;
        List<List<Integer>> res = Maths.getCombinations(n,k);
        assertEquals(res.size(),(int) Maths.getCombinationsNumber(n,k));
    }

    @Test
    public void getRamdomTest(){
        int n = 10;
        int k = 10;
        int[] res = Maths.getRandom(n,k);
        print(res);
        assertEquals(res.length,k);
    }

    public void print(int[] objs){
        System.out.println(objs.length+":");
        for(long o : objs)
            System.out.print(o+" ");
    }

}