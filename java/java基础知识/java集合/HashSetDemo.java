/**
 * HashSet存储数据的时候，会利用到HashTable
 * HashTable的存储原理是：
 * 1.首先算出hash值，利用hash值来找到需要存储的位置。
 * 2.然后会判断相同的hashcode值的对象，是否是相同的对象，利用equals判断。
 * 3.如果hashcode和equals判断都是相同的，我们就判断两个对象是相同的，就不需要重复存储。
 * 4.如果对象不一样，但是hashcode一样，我们就把他存储到相同hashcode的地方，然后用链表存起来。
 */
public class HashSetDemo {
    public static void main(String[] args) {
        HashSet hs =  new HashSet();
    }
}

class Person {
    private String name;
    private int age;

    public String getName() {
        return this.name;
    }
    public int getAge() {
        return this.age;
    }
    // 重写hashCode方法
    @Override
    public int hashCode() {
        return 0x3c;
    } 

    // 重写equals方法
    @Override
    public boolean equals(Object person) {
        return this.name = (Person)person.name;
    }
}