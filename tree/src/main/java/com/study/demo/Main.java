package com.study.demo;


import com.study.demo.tree.AVLTree;
import com.study.demo.tree.BST;
import com.study.demo.tree.pirnter.BinaryTrees;


/**
 * @ClassName Main
 * @Description TODO
 * @Author Administrator
 * @Date 2020/3/30 0030 15:33
 * @Version 1.0
 */
public class Main {
    static void test1() {
        Integer[] data = new Integer[]{
                7, 4, 9, 2, 5, 8, 11, 3, 12, 1
        };
        BST<Integer> tree = new BST<>();
        for (int i = 0; i < data.length; i++) {
            tree.add(data[i]);
        }
        BinaryTrees.println(tree, BinaryTrees.PrintStyle.LEVEL_ORDER);
    }
    static void test2() {
        Integer[] data = new Integer[]{
                7, 4, 9, 2, 5, 8, 11, 3, 12, 1
        };
        AVLTree<Integer> tree = new AVLTree<>();
        for (int i = 0; i < data.length; i++) {
            tree.add(data[i]);
        }
        BinaryTrees.println(tree, BinaryTrees.PrintStyle.LEVEL_ORDER);
    }

    public static void main(String[] args) {


        String s = "abcfdb";
        String[] split = s.split("");
        for (int i = 0; i < split.length; i++) {
           split[i]="\"\\\""+split[i]+ "\\\"\"";
        }
        System.out.println(String.join(",", split));
//        test1();
    }

}
