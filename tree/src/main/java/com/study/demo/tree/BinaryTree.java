package com.study.demo.tree;



import com.study.demo.tree.pirnter.BinaryTreeInfo;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @ClassName BinaryTree
 * @Description TODO
 * @Author Administrator
 * @Date 2020/4/7 0007 13:55
 * @Version 1.0
 */
@SuppressWarnings("unused")
public class BinaryTree<E> implements BinaryTreeInfo {
    protected int size;
    protected Node<E> root;

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public void clear() {
        root = null;
        size = 0;
    }


    /*
     * @Author lisheng
     * @Description //TODO 前序遍历,自定义处理规则
     * @Date 11:09 2020/4/2 0002
     * @Param [visitor]
     * @return void
     **/
    public void preorder(Visitor<E> visitor) {
        if (visitor == null) return;
        preorder(root, visitor);
    }

    private void preorder(Node<E> node, Visitor<E> visitor) {
        if (node == null || visitor.stop) return;
        visitor.stop = visitor.visit(node.element);
        preorder(node.left, visitor);
        preorder(node.right, visitor);
    }

    /*
     * @Author lisheng
     * @Description //TODO 中序遍历,自定义处理规则
     * @Date 11:09 2020/4/2 0002
     * @Param [visitor]
     * @return void
     **/
    public void inorder(Visitor<E> visitor) {
        if (visitor == null) return;
        inorder(root, visitor);
    }

    private void inorder(Node<E> node, Visitor<E> visitor) {
        if (node == null || visitor.stop) return;
        inorder(node.left, visitor);
        if (visitor.stop) return;
        visitor.stop = visitor.visit(node.element);
        inorder(node.right, visitor);
    }

    /*
     * @Author lisheng
     * @Description //TODO 后序遍历,自定义处理规则
     * @Date 11:09 2020/4/2 0002
     * @Param [visitor]
     * @return void
     **/
    public void postorder(Visitor<E> visitor) {
        if (visitor == null) return;
        postorder(root, visitor);
    }

    private void postorder(Node<E> node, Visitor<E> visitor) {
        if (node == null || visitor.stop) return;
        postorder(node.left, visitor);
        postorder(node.right, visitor);
        if (visitor.stop) return;
        visitor.stop = visitor.visit(node.element);
    }

    /*
     * @Author lisheng
     * @Description //TODO 层序遍历，自定义处理逻辑
     * @Date 11:02 2020/4/2 0002
     * @Param [visitor]
     * @return void
     **/
    public void levelOrder(Visitor<E> visitor) {
        if (root == null || visitor == null) return;
        Queue<Node<E>> queue = new LinkedList<>();
        //将根节点如队enQueue
        queue.offer(root);
        while (!queue.isEmpty()) {
            //出队
            Node<E> poll = queue.poll();
            //返回值为true结束遍历
            boolean visit = visitor.visit(poll.element);
            if (visit) return;
            //判断左节点是否为空
            if (poll.left != null) {
                queue.offer(poll.left);
            }
            if (poll.right != null) {
                queue.offer(poll.right);
            }
        }
    }

    protected Node<E> createNode(E element, Node<E> parent) {
        return new Node<>(element, parent);
    }

    /*
     * @Author lisheng
     * @Description //TODO 前驱节点
     **/
    protected Node<E> predecessor(Node<E> node) {
        if (node == null) return null;
        //前驱节点在左子树中
        if (node.left != null) {
            Node<E> p = node.left;
            while (p.right != null) {
                p = p.right;
            }
            return p;
        }
        //从父节点、祖父节点中寻找前驱节点
        while (node.parent != null && node == node.parent.left) {
            node = node.parent;
        }

        return node.parent;
    }

    /*
     * @Author lisheng
     * @Description //TODO 后继节点,protected自己和子类都可以使用
     **/
    protected Node<E> successor(Node<E> node) {
        if (node == null) return null;
        //前驱节点在左子树中
        Node<E> p = node.right;
        if (p != null) {
            while (p.left != null) {
                p = p.left;
            }
            return p;
        }
        //从父节点、祖父节点中寻找前驱节点
        while (node.parent != null && node == node.parent.right) {
            node = node.parent;
        }

        return node.parent;
    }

    /*
     * @Author lisheng
     * @Description //TODO 判断是否完全二叉树
     **/
    public boolean isComplete() {
        if (root == null) return false;
        LinkedList<Node<E>> queue = new LinkedList<>();
        queue.offer(root);
        boolean leaf = false;
        while (!queue.isEmpty()) {
            Node<E> node = queue.poll();
            if (leaf && !node.isLeaf()) return false;
            if (node.left != null) {
                queue.offer(node.left);
            } else if (node.right != null) {
                //node.left == null &&　node.right !=null
                return false;
            }
            if (node.right != null) {
                queue.offer(node.right);
            } else {
                leaf = true;
            }
        }

        return true;
    }

    /*
     * @Author lisheng
     * @Description //TODO  迭代计算二叉树的高度（层序遍历）
     **/
    public int height() {
        if (root == null) return 0;
        int height = 0;//树的高度
        int levelSize = 1;//存储每一层的数量

        Queue<Node<E>> queue = new LinkedList<>();
        //将根节点如队enQueue
        queue.offer(root);
        while (!queue.isEmpty()) {
            //出队
            Node<E> poll = queue.poll();
            levelSize--;
            //判断左节点是否为空
            if (poll.left != null) {
                queue.offer(poll.left);
            }
            if (poll.right != null) {
                queue.offer(poll.right);
            }
            if (levelSize == 0) {//即将访问下一层
                levelSize = queue.size();
                height++;
            }
        }
        return height;
    }

    /*
     * @Author lisheng
     * @Description //TODO 获取树的高度(递归方式)
     **/
    public int height2() {
        return height(root);
    }

    //节点的高度
    private int height(Node<E> node) {
        if (node == null) return 0;
        return 1 + Math.max(height(node.left), height(node.right));
    }


    public static abstract class Visitor<E> {
        //控制遍历的终止
        boolean stop;

        /*
         * 返回true代表停止，faulse表示继续
         **/
        abstract boolean visit(E element);
    }

    @Override
    public Object root() {
        return root;
    }

    @Override
    public Object left(Object node) {
        return ((Node<E>) node).left;
    }

    @Override
    public Object right(Object node) {
        return ((Node<E>) node).right;
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

    protected static class Node<E> {
        E element;
        Node<E> left;
        Node<E> right;
        Node<E> parent;

        public Node(E element, Node<E> parent) {
            this.element = element;
            this.parent = parent;
        }

        public boolean isLeaf() {
            return left == null && right == null;
        }

        public boolean hasTwoChildren() {
            return left != null && right != null;
        }

        public boolean isLeftChild(){
            return parent != null && this == parent.left;
        }

        public boolean isRightChild(){
            return parent != null && this == parent.right;
        }
    }
}
