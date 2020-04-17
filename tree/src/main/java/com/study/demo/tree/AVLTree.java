package com.study.demo.tree;

import java.util.Comparator;

/**
 * @ClassName AVLTree
 * @Description TODO
 * @Author Administrator
 * @Date 2020/4/7 0007 15:52
 * @Version 1.0
 */
public class AVLTree<E> extends BST<E> {
    public AVLTree() {
        this(null);
    }

    public AVLTree(Comparator<E> comparator) {
        super(comparator);
    }

    @Override
    protected void afterAdd(Node<E> node) {
        while ((node = node.parent) != null) {
            if (isBalanced(node)){
                //更新高度
            }else {
                //恢复平衡
            }
        }
    }

    @Override
    protected Node createNode(Object element, Node parent) {
        return new AVLNode<>(element, parent);
    }

    private boolean isBalanced(Node<E> node) {
       return Math.abs(((AVLNode<E>)node).balanceFactor()) <= 1;
    }


    private static class AVLNode<E> extends Node<E> {
        int height = 1;

        public AVLNode(E element, Node<E> parent) {
            super(element, parent);
        }

        public int balanceFactor() {
            int leftHeight = left == null ? 0 : ((AVLNode) left).height;
            int rightHeight = right == null ? 0 : ((AVLNode) right).height;
            return leftHeight - rightHeight;
        }
    }
}
