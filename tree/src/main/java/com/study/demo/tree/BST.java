package com.study.demo.tree;

import java.util.Comparator;

/**
 * @ClassName
 * @Description TODO
 * @Author Administrator
 * @Date 2020/3/30 0030 15:33
 * @Version 1.0
 */
@SuppressWarnings("unused")
public class BST<E> extends BinaryTree {

    private Comparator<E> comparator;

    public BST() {
        this(null);
    }

    public BST(Comparator comparator) {
        this.comparator = comparator;
    }


    public void add(E element) {
        elementNotNullCheck(element);
        //添加第一个元素
        if (root == null) {
            root = createNode(element,null);
            size++;
            //新添加节点之后的处理
            afterAdd(root);
            return;
        }
        //添加的不是第一个节点
        Node<E> parent = root;
        Node<E> node = root;
        int compare = 0;
        while (node != null) {
            compare = compare(element, node.element);
            parent = node;
            if (compare > 0) {
                node = node.right;
            } else if (compare < 0) {
                node = node.left;
            } else {//相等,等于父节点,覆盖
                node.element = element;
                return;
            }
        }
        //插入到父节点那个位置
        Node<E> newNode = createNode(element,parent);
        if (compare > 0) {
            parent.right = newNode;
        } else {
            parent.left = newNode;
        }
        size++;
        //新添加节点之后的处理
        afterAdd(newNode);
    }

    /*
     * @Author lisheng
     * @Description //TODO 添加node之后的调整
     **/
    protected void afterAdd(Node<E> node) {

    }

    public void remove(E element) {
        remove(node(element));
    }

    public boolean contains(E element) {
        return node(element) != null;
    }

    private void remove(Node<E> node) {
        if (node == null) return;
        size--;
        if (node.hasTwoChildren()) {//度为2的节点
            //找到后继节点
            Node<E> s = successor(node);
            //用后继节点的值覆盖度为2 的节点的值
            node.element = s.element;
            //删除后继节点
            node = s;
        }
        //删除node节点，node的度是1或者0
        Node<E> replacement = node.left != null ? node.left : node.right;
        if (replacement != null) {//node的度为1
            //更改paren
            replacement.parent = node.parent;
            //更改parent的left、right的指向
            if (node.parent == null) {//node是度为1的节点且是根节点
                root = replacement;
            } else if (node == node.parent.left) {
                node.parent.left = replacement;
            } else if (node == node.parent.right) {
                node.parent.right = replacement;
            }

        } else if (node.parent == null) {//node是叶子结点且是根节点
            root = null;
        } else {//node是叶子结点，但不是根节点
            if (node == node.parent.left) {
                node.parent.left = null;
            } else {//node == node.parent.right
                node.parent.right = null;
            }
        }
    }

    private Node<E> node(E element) {
        Node<E> node = root;
        while (node != null) {
            int cmp = compare(element, node.element);
            if (cmp == 0) return node;
            if (cmp > 0) {
                node = node.right;
            } else {
                node = node.left;
            }
        }
        return node;
    }

    /*
     * @return int 返回0相等，返回值大于0 e1大于e2，返回值小于0 e1小于e2
     **/
    private int compare(E e1, E e2) {
        if (comparator != null) {
            return comparator.compare(e1, e2);
        }
        return ((Comparable<E>) e1).compareTo(e2);
    }

    private void elementNotNullCheck(E element) {
        if (element == null) {
            throw new IllegalArgumentException("element must be not null");
        }
    }

    @Override
    public Object string(Object node) {
        Node<E> myNode = (Node<E>) node;
        String parentString = "null";
        if (myNode.parent != null) {
            parentString = myNode.parent.element.toString();
        }
        return ((Node<E>) node).element + "_" + parentString;
    }

}
