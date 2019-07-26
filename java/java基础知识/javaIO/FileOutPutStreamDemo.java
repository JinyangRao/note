/**
 * 字节流：
 * 需求：操作图片数据，就要用到字节流。
 */
import java.io.*;

class FileOutPutStreamDemo {
    public static void main(String[] args) throws IOException {
        // writeFile();
        readFile_1();
    }

    public static void readFile_2() throws IOException {
        FileInputStream fis = new FileInputStream("fos.txt");

        // available()这个函数返回文件的字节数大小
        // 1KB的空间
        byte[] buf = new byte[1024];
        int len = 0;
        while((len = fis.read(buf)) != -1) {
            System.out.println(new String(buf, 0, len));
        }
        fis.close();
    }

    public static void readFile_1() throws IOException {
        FileInputStream fis = new FileInputStream("fos.txt");

        int ch = 0;
        // 返回一个字节的ascii码，当读取到文件末尾的时候，返回-1
        while((ch = fis.read()) != -1) {
            System.out.println((char)ch);
        }
        fis.close();
    }

    public static void writeFile() throws IOException {
        FileOutputStream fos = new FileOutputStream("fos.txt");

        /**
         * 字符流：底层走的字节，但是会提供码表以供处理。
         * 字节流，如果没有提供缓冲，然后一个字节一个字节的处理。就是读一个字节，就存一个字节，
         * 是对字节的最小单位操作
         * 提供缓冲就是批处理。
         */
        fos.write("abcdefs".getBytes());

        fos.close();
    }
}