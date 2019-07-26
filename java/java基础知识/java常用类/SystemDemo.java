import java.util.Properties;

/**
 * 1.
 * Properities类，继承自Hashtable();
 */
class SystemDemo {
    public static void main(String[] args) {
        // 1.
        Properties prop = System.getProperties();

        for(Object obj: prop.keySet()) {
            String value = (String)prop.get(obj);
            // user.dir : c:\Users\57378\note\java\java基础知识\java常用类
            // 可以得到user.dir
            System.out.println(obj + " : " + value);
        }
    }
}