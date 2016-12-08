package com.duansky.benchmark.flink.analysis.dataset.transformations.pojo;

/**
 * Created by DuanSky on 2016/11/10.
 */
public class WC {

    private String word;
    private int count;

    public WC(){}

    public WC(String word,int count){
        this.word = word;
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public String toString(){
        return word + ":" + count;
    }
}
