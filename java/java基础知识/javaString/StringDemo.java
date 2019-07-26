// String在java.lang包
// String是final类

class StringDemo {
    public static void main(String[] args) {
        String s1 = "abc";

        String s2 = new String("abc");
        /**
         * s1和s2有什么区别
         * s1表示一个对象。
         * 撇开s1，s2在内存中有两个对象
         */
        String s3 = "abc";
        /**
         * abc是在常量池中的对象，如果new String，该对象就在堆内存中
         */

        System.out.println(s1 == s2); // flase
        System.out.println(s1.equals(s2)); // true, String里面重写了equals方法
    }
}