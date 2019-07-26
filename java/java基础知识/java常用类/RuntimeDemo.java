/**
 * RunTime对象，是一个单例模式对象。
 * 每个Java应用程序都有一个Runtime类的Runtime，
 * 允许应用程序与运行应用程序的环境进行接口。
 *  当前运行时可以从getRuntime方法获得。 
 */

 class RunTimeDemo {
     public static void main(String[] args) throws Exception {
         Runtime r = Runtime.getRuntime();
        //  r.exec("dir");
     }
 }