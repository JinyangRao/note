import java.io.*;

public class FileWriterDemo {
    public static void main(String[] args) {
        // 创建一个文件，如果该目录下已经有同名文件，那么就覆盖

        FileWriter fw = null;

        try {
            // 在try外建立引用，在try中进行初始化
            // fw = new FileWriter("Demo.txt");

            // 在已有的文件末尾处追加内容！
            fw = new FileWriter("Demo.txt", true);

            fw.write("abc");// 写到了内存，流中

            fw.flush();// 刷新流的缓冲到文件里面

            fw.write("cde");

            fw.flush();

            // fw.close();//关闭了流资源，在关闭之前会刷新内部的缓冲中的数据。
        } catch (IOException e) {

        } finally {
            try {
                // 这句话也会发生异常
                if (fw != null) {
                    fw.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}