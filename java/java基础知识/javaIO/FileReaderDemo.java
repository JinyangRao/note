import java.io.*;

public class FileReaderDemo {
    public static void main(String[] args) throws Exception {
        // 创建一个文件读取流对象
        // 文件不存在会抛出FileNotFoundException
        FileReader fr = new FileReader("Demo.txt");

        // read一次读一个字符，并且会自动下读
        // 当读到文件末尾就返回-1

        // 1.第一种方式
        /*
        while (true) {
            int ch = fr.read();
            if(ch == -1) {
                break;
            }
            System.out.println((char)ch);
        }
        */
        // 2.第二种方式
        int ch = 0;
        while((ch = fr.read()) != -1) {
            System.out.println((char)ch);
        }

        // System.out.println(ch);

        fr.close();
    }
}