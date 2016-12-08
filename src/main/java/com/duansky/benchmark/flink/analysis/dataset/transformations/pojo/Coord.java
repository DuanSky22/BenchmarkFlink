package com.duansky.benchmark.flink.analysis.dataset.transformations.pojo;

/**
 * Created by DuanSky on 2016/11/15.
 */
public class Coord {
    public int id;
    public int x;
    public int y;

    public Coord(){}

    public Coord(int id,int x,int y){
        this.id = id;
        this.x = x;
        this.y = y;
    }

    @Override
    public String toString() {
        return "Coord{" +
                "id=" + id +
                ", x=" + x +
                ", y=" + y +
                '}';
    }
}
