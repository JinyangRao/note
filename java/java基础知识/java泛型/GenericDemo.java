import java.util.ArrayList;
import java.util.Iterator;

// 泛型：jdk1.5出现的安全机制
/**
 * <E>其中E是泛型的类型，就是引用数据类型 
 * 好处：
 * 1. 将运行时期出现的问题ClassCastException，转移到了编译时期
 * 方便于程序员解决问题，让运行事情问题减少，安全。
 * 
 * 2. 避免了强制转换的麻烦
 */

class GenericDemo {
    public static void main(String[] args) {
        ArrayList<String> al = new ArrayList<String>();

        a1.add("abc1");
        a1.add("abc2");
        a1.add("abc3");
        a1.add("abc4");
        a1.add("abc5");

        // ArrayList虽然声明是泛型，但是迭代器相应的也应该需要声明为泛型
        Iterator<String> it = al.iterator();

        while(it.hasNext()) {
            String s = it.next();
            System.out.println(s + ":" + s.length());
        }
    }
}