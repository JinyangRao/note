/**
 * 需求：利用treeset存储自定义学生，
 * 想按照学生的年龄进行排序。
 * TreeSet存进去的对象需要继承Compareable接口
 * 
 * TreeSet第一种比较方式：
 * 让元素具备比较性，比如下面的Student类实现了Comparable接口
 * 
 * 第二种比较方式：
 * 如果元素不能改变，且元素没有比较性
 * 让集合具备比较性~！
 * 在集合初始化时，
 * TreeSet ts = new TreeSet(new Comparator() {
 *      //比较代码~！
 *      // 实现compare()方法
 *      // 或者覆盖equals方法       
 * });
 * 
 * 当上述两种比较方式同事存在的时候，最后是以第二种比较器为准~！！！！
 * 
 * 两个接口：
 * comparator是需要实现compare(Object o1, Object o2)
 * comparable是需要实现comparaTo(Object o1) 方法
 */
class TreeSetDemo {
    public static void main(String[] args) {

    }
}


class Student implements Comparable {
    private String name;
    private int age;

    Student (String name, int age) {
        this.name = name;
        this.age = age;
    }

    public int compareTo(Object obj) {
        return 1;
    }
}