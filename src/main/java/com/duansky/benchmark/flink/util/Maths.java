package com.duansky.benchmark.flink.util;

import org.apache.commons.lang3.ArrayUtils;

import java.util.*;

/**
 * Created by DuanSky on 2016/10/30.
 */
public class Maths {

    private static Random random = new Random();
    private static double threshold = 0.02;

    private static Map<Integer,List<List<Integer>>> combinationsCache = new HashMap<>();
    private static Map<Integer,Long> combinationNumCache = new HashMap<>();

    /**
     * get the number of combinations(n,k)
     * @param n the total number.
     * @param k the picked up number.
     * @return the result of c(n,k)
     */
    public static long getCombinationsNumber(int n, int k){
        int hashCode = Arrays.hashCode(new int[]{n,k});
        if(combinationNumCache.containsKey(hashCode))
            return combinationNumCache.get(hashCode);
        else{
            long res = 1;
            for(int i = 0;i < k; i++){
                res *= (n-i);
            }
            for(int i = 1; i <=k; i++){
                res /= i;
            }
            combinationNumCache.put(hashCode,res);
            return res;
        }

    }

    /**
     * get all the combinations of the given n and k.
     * @param n the total number.
     * @param k the picked up number.
     * @return all the combinations of n,k.
     */
    public static List<List<Integer>> getCombinations(int n, int k){
        int hashCode = Arrays.hashCode(new int[]{n,k});
        if(combinationsCache.containsKey(hashCode)) return combinationsCache.get(hashCode);
        else{
            List<List<Integer>> res = new ArrayList<List<Integer>>((int) getCombinationsNumber(n,k));
            getCombinations(n,k,new ArrayList<Integer>(k),res);
            combinationsCache.put(hashCode,res);
            return res;
        }

    }
    private static void getCombinations(int n,int k,List<Integer> curr,List<List<Integer>> res){
        if(curr == null || curr.size() != k){ //its not finished yet.
            int size = curr.size();
            //the last element of the curr.
            if(curr == null) curr = new ArrayList<Integer>(k);
            int last = curr.size() == 0 ? -1 : curr.get(curr.size()-1);
            for(int i = last + 1; i <= n -(k-size); i++){
                curr.add(i);
                getCombinations(n,k,curr,res);
                curr.remove(curr.size()-1);
            }
        }else{
            res.add(new ArrayList<Integer>(curr));
        }
    }

    /**
     * get the m random combination of c(n,k).
     * @param n the total number.
     * @param k the picked up number.
     * @param m the number of how many combination will be returned.
     * @return the m random combination of c(n,k).
     */
    public static int[][] getRandomCombination(int n,int k,int m){
        int[][] res = new int[m][k];
        List<List<Integer>> total = getCombinations(n,k);
        int[] pos = getRandom(total.size(),m);
        for(int i = 0; i < m; i++){
            for(int j = 0; j < k; j++){
                res[i][j] = total.get(pos[i]).get(j);
            }
        }
        return res;
    }

    /**
     * generate k numbers of Integer ranged from 1 to n.
     * @param n the upper bound of the generated integer.
     * @param m how many numbers we need.
     * @return the k numbers of Integer ranged form 1 to n.
     */
    private static int[] getRandom0(int n, int m){
        Set<Integer> set = new HashSet<Integer>();
        Random random = new Random();
        while(set.size() != m){
            int next = random.nextInt(n);
            set.add(next);
        }
        return ArrayUtils.toPrimitive((Integer[]) set.toArray());
    }

    /**
     * generate m numbers of Integer ranged from 1 to n.
     * @param n the upper bound of the generated integer.
     * @param m how many numbers we need.
     * @return the k numbers of Integer ranged form 1 to n.
     */
    public static int[] getRandom(int n,int m){
        double p = m * 1.0 / n;
        if(p <= threshold) return getRandom0(n,m);
        else{
            Integer[] total = new Integer[n];
            for(int i = 0; i < n; i++) total[i]=i;
            List<Integer> list = Arrays.asList(total);
            Collections.shuffle(list);
            Integer[] tmp = list.subList(0,m).toArray(new Integer[m]);
            return ArrayUtils.toPrimitive(tmp);
        }
    }

    /**
     * generate k undirected pairs(which means pair (a,b) equals (b,a),they are same pairs)
     * of Integer ranged from 0(inclusive) to n(exclusive).
     * @param n the upper bound of the generated integer.
     * @param m how many pairs we need.
     * @return k pairs of Integer ranged from 0(inclusive) to n(exclusive).
     */
    public static int[][] getRandomUndirectedPairs(int n,int m){
        double p = m * 1.0 / Maths.getCombinationsNumber(n,2);
        if(p <= threshold)
            return getRandomUndirectedPairs0(n,m);
        else{
            return getRandomCombination(n,2,m);
        }
    }

    /**
     * generate k undirected pairs(which means pair (a,b) equals (b,a),they are same pairs)
     * of Integer ranged from 0(inclusive) to n(exclusive).Here we use random algorithm to
     * generate the pairs.
     * @param n the upper bound of the generated integer.
     * @param k how many pairs we need.
     * @return k pairs of Integer ranged from 0(inclusive) to n(exclusive).
     */
    private static int[][] getRandomUndirectedPairs0(int n,int k){
        Set<Integer> set = new HashSet<Integer>();
        int[][] res = new int[k][2]; int i = 0;
        while(i != k){
            int a = random.nextInt(n);
            int b = random.nextInt(n);
            int hc1 = Arrays.hashCode(new int[]{a,b});
            int hc2 = Arrays.hashCode(new int[]{b,a});
            if(a != b && !set.contains(hc1)){
                res[i][0] = a;
                res[i][1] = b;
                i++;
                set.add(hc1); set.add(hc2);
            }
        }
        return res;
    }


    public static int[][] getRandomDirectedPairs(int n,int m){
        double p = 1.0 * m / Maths.getCombinationsNumber(n,2);
        if(p <= threshold) return getRandomDirectedPairs0(n,m);
        else return getRandomCombination(n,2,m);
    }

    private static int[][] getRandomDirectedPairs0(int n,int m){
        Set<Integer> set = new HashSet<Integer>();
        int[][] res = new int[m][2]; int i = 0;
        while(i != m){
            int a = random.nextInt(n), b = random.nextInt(n);
            int hc = Arrays.hashCode(new int[]{a,b});
            if(a != b && !set.contains(hc)){
                set.add(hc);
                res[i][0] = a;
                res[i][1] = b;
                i++;
            }
        }
        return res;
    }
}
