import java.util.Iterator;
import java.util.TreeSet;

import com.sun.corba.se.spi.orbutil.threadpool.Work;

import java.util.Comparator;

/** 
 * 源码模拟~！
 * 
*/
/*
class Person {}

class Student extends Person {
}

class TreeSet<E> {
    TreeSet(Comparator<? super E> comparator) {
    } 
}

// 该Comp比较器类，可以比较Person和Person的子类
class Comp implements Comparator<Person> {
    public int compareTo(Person p1, Person p2) {
    }
}
*/
// 我可以这样声明
/**
 * TreeSet<Student> ts = new TreeSet<Student>(new Comp());
 */
class GenericDemo07 {
    public static void main(String[] args) {
        
        // TreeSet<Student> ts1 = new TreeSet<Student>(new StuComp());
        TreeSet<Student> ts1 = new TreeSet<Student>(new Comp());
        ts1.add(new Student("abc1"));
        ts1.add(new Student("abc3"));
        ts1.add(new Student("abc2"));

        Iterator<Student> it1 = ts1.iterator();
        while(it1.hasNext()) {
            System.out.println(it1.next().getName());
        }

        // TreeSet<Worker> ts2 = new TreeSet<Worker>(new WorkerComp());
        TreeSet<Worker> ts2 = new TreeSet<Worker>(new Comp());
        ts2.add(new Worker("abc-1"));
        ts2.add(new Worker("abc-3"));
        ts2.add(new Worker("abc-2"));

        Iterator<Worker> it2 = ts2.iterator();
        while(it2.hasNext()) {
            System.out.println(it2.next().getName());
        }

    }
}
/**
 * 我可以定义父类型的比较器，复用性好
 */
class Comp implements Comparator<Person> {
    @Override
    public int compare(Person p1, Person p2) {
        // 这里面只能有父类的方法
        return p1.getName().compareTo(p2.getName());
    } 
}

class StuComp implements Comparator<Student> {
    public int compare(Student s1, Student s2) {
        return s1.getName().compareTo(s2.getName());
    } 
}

class WorkerComp implements Comparator<Worker> {
    public int compare(Worker w1, Worker w2) {
        return w1.getName().compareTo(w2.getName());
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
}

class Student extends Person {
    public Student(String name) {
        super(name);
    }
}

class Worker extends Person {
    public Worker(String name) {
        super(name);
    } 
}
