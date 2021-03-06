package com.study.demo.tree;

import com.sun.corba.se.spi.ior.IdentifiableFactory;

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
            if (isBalanced(node)) {
                //更新高度
                updateHeight(node);
            } else {
                //恢复平衡
                reBalance(node);
                //整棵树回复平衡
                break;
            }
        }
    }

    @Override
    protected Node createNode(Object element, Node parent) {
        return new AVLNode<>(element, parent);
    }

    /*
     *回复平衡
     * @Description //TODO 高度最低的那个不平衡节点
     **/
    private void reBalance(Node<E> grand) {
        Node<E> parent = ((AVLNode) grand).tallerChild();
        Node<E> node = ((AVLNode) grand).tallerChild();
        if (parent.isLeftChild()) {//L
            if (node.isLeftChild()) {//LL
                rotateRight(grand);
            } else {//LR
                rotateLeft(parent);
                rotateRight(grand);
            }
        } else {//R
            if (node.isLeftChild()) {//RL
                rotateRight(parent);
                rotateLeft(grand);
            } else {//RR
                rotateLeft(grand);
            }
        }
    }

    private void rotateLeft(Node<E> node) {

    }

    private void rotateRight(Node<E> node) {

    }

    private boolean isBalanced(Node<E> node) {
        return Math.abs(((AVLNode<E>) node).balanceFactor()) <= 1;
    }

    private void updateHeight(Node<E> node) {
        ((AVLNode<E>) node).updateHeight();
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

        public void updateHeight() {
            int leftHeight = left == null ? 0 : ((AVLNode) left).height;
            int rightHeight = right == null ? 0 : ((AVLNode) right).height;
            height = 1 + Math.max(leftHeight, rightHeight);
        }

        public Node<E> tallerChild() {
            int leftHeight = left == null ? 0 : ((AVLNode) left).height;
            int rightHeight = right == null ? 0 : ((AVLNode) right).height;
            if (leftHeight > rightHeight) return left;
            if (leftHeight < rightHeight) return right;
            return isLeftChild() ? left : right;
        }
    }
}
