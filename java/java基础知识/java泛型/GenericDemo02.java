
/**
 * 自定义泛型~！
 * 泛型只能支持引用数据类型
 * 也可以定义多个泛型类型
 * 静态方法不能访问类对象上的泛型
 */

 class Utils<T, E, Q> {
     private T obj;
     public void setObj(T t) {
         this.obj = t;
     }
     public T getObj() {
         return obj;
     }
 }

 /**
  * 把泛型定义在方法上
  * 泛型定义放在函数返回值的前面
  */
  class DemoT {
      public  <T> void show(T t) {
          /**
           * 在调用的时候，只需要调用方法就像，T会根据传入的参数类型进行适配
           * d.show("Test");
           */
      }
  }
  /**
   * 泛型也可以定义在接口上
   */
  interface Inter<T> {
      void show(T t);
  }

  // 1.第一种方式
  class InterImpl implements Inter<String> {
      public void show(String t) {//方法写具体的泛型类型

      }
  }
  // 2.第二种方式
  class InterImpl2<T> implements Inter<T> {
      public void show(T t) {//这种可以继续带着泛型类型

      }
  }


 class GenericDemo3 {
     public static void main(String[] args) {
         Utils<Integer> t = new Utils<Integer>();
     }
 }