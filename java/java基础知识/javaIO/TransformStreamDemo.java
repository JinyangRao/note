/**
 * 我们能不能直接使用readline方法来完成键盘录入的一行数据的读取呢？
 * 
 * readLine方法是字符流BufferedReader类中的方法
 * 而键盘录入的read方法时InputStream的方法
 * InputStream in = System.in;
 * int by = in.read();
 * 
 * solution:
 * 把字节流转换为字符流~！
 * 
 * InputStreamReader是字节转字符
 * OutputStreamWriter是字符转字节
 */
import java.io.*;
import java.util.logging.Logger;
class TransformStreamDemo {
    public static void main(String[] args) throws IOException {
        // 输入字节流
        InputStream in = System.in;
        // Logger log = Logger.getLogger(name)
        // 转换流 
        InputStreamReader isr = new InputStreamReader(in);
        // 转换为字符流
        BufferedReader bufr = new BufferedReader(isr);

        // 把字符转换为字节流

        // 字节流
        OutputStream out = System.out;

        // 转换流
        OutputStreamWriter osw = new OutputStreamWriter(out);

        // 字符流
        BufferedWriter bufw = new BufferedWriter(osw);

        String line = null;

        // line是接受的字符
        while((line = bufr.readLine()) != null) {
            // System.out.println(line.toUpperCase());
            bufw.write(line.toUpperCase());
            bufw.newLine();
            bufw.flush();
        }

    }
}