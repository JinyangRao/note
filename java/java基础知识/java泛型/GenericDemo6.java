import java.util.*;
/**
 * 泛型的高级应用：
 * 泛型限定~！
 */
// 首先看一个程序
// ArrayList<Object>和ArrayList<String>没有继承关系，
// 所以我们以下的程序把PrintColl(ArrayList<Object> al)定义成这样
// 还说会报错。复用性不高。
/*
class GenericDemo6 {
    public static void main(String[] args) {

        ArrayList<String> al = new ArrayList<String>();

        al.add("abc1");
        al.add("abc1");
        al.add("abc1");

        ArrayList<Integer> al1 = new ArrayList<Integer>();

        al1.add(1);
        al1.add(2);
        al1.add(3);

        printColl(al);

    }

    public static void printColl(ArrayList<Object> al) {
        Iterator<Object> it = al.iterator();
        while(it.hasNext()) {
            System.out.println(it.next());
        }
    }
}
*/

// ----------------------------------------------------

/*
// ?是占位符:表示你以你传的类型来定义，也可以用T类型进行替换
// ?占位符和T类型的区别：T类型可以在函数中进行使用，而占位符不能
class GenericDemo6 {
    public static void main(String[] args) {

        ArrayList<String> al = new ArrayList<String>();
        al.add("abc1");
        al.add("abc1");
        al.add("abc1");

        ArrayList<Integer> al1 = new ArrayList<Integer>();
        al1.add(1);
        al1.add(2);
        al1.add(3);

        printColl(al);
        printColl(al1);

        printColl2(al);
        printColl2(al1);
    }

    public static void printColl(ArrayList<?> al) {
        Iterator<?> it = al.iterator();
        while(it.hasNext()) {
            System.out.println(it.next());
        }
    }

    // 要是把？占位符换成T，需要换成泛型函数的写法
    // 泛型函数的泛型类型需要写在函数返回值前面
    public static <T> void printColl2(ArrayList<T> al) {
        Iterator<?> it = al.iterator();
        while(it.hasNext()) {
            System.out.println(it.next());
        }
    }
}
*/

/**
 * ? extends E:上限
 * ? super E: 下限
 */
class GenericDemo6 {
    public static void main(String[] args) {
        ArrayList<Person> al = new ArrayList<Person>();
        al.add(new Person("abc1"));
        al.add(new Person("abc2"));
        al.add(new Person("abc3"));

        ArrayList<Student> al1 = new ArrayList<Student>();
        al1.add(new Student("abc-1"));
        al1.add(new Student("abc-2"));
        al1.add(new Student("abc-3"));

        printColl2(al);
        printColl2(al1);
    }

    public static void printColl2(ArrayList<? extends Person> al) {
        Iterator<? extends Person> it = al.iterator();

        while(it.hasNext()) {
            System.out.println(it.next());
        }
    }
}

class Person {
    String name;
    public Person(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
    public String toString() {
        return "[" + name + "]";
    }
}

class Student extends Person {
    public Student(String name) {
        super(name);
    }
}
