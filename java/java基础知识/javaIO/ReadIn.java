/**
 * 读取键盘输入
 */
import java.io.*;

class ReadIn {
    public static void main(String[] args) throws IOException {
        InputStream in = System.in;

        int by = in.read();

        System.out.println(by);
    }
}