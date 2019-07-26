import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

/** Interface Map<K,V>
 * Map接口和Collection接口一样是一个顶层接口
 * Map
 *  |---Hashtable
 *  |---HashMap
 *  |---TreeMap
*/ 
class MapDemo {
    public static void main(String[] args) {
        Map<String, String> map = new HashMap<String, String>();

        /**
         * values():返回的是一个Collection<E>集合，
         * 理解返回的是视图的概念，因为容器存储的都是对象的引用。
         * 所以在返回的视图中，改变其存储的内容，就是改变了原容器中对象的内容。
         */
        Collection<String> colVal = map.values();
        Set<String> keyS = map.keySet();
        // Executors
    }
}
