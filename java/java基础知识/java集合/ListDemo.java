import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;

/**
 * List里面有一个人更强大的迭代器，ListIterator
 * 该接口可以遍历集合的时候添加修改集合
 */

public class ListDemo {
    public static void main(String[] args) {

        ArrayList al = new ArrayList();

        al.add("java01");
        al.add("java02");
        al.add("java03");

        // 如果用Iterator进行接收的话，多态的性质，父类引用不能访问子类特有的方法和属性
        // 所以我们需要如下写法
        ListIterator listIt = al.listIterator();

        while(listIt.hasNext()) {
            Object obj =  listIt.next();

            if(obj.equals("java02")) {
                listIt.set("java08");;
            }
        }
        System.out.println(al);
        /*
        Iterator it = al.iterator();

        while(it.hasNext()) {
            Object obj = it.next();
            // 因为在iterator类的操作方法中调用了数组的修改方法
            if(obj.equals("java02")) {
                // al.add("java08");//ConcurrentModificationException
                it.remove();
            }
            System.out.println("obj=" + obj);
        }
        System.out.println(al);
        */
    }
}