/*
字符流中的缓冲区：
缓冲区可以提高数据的读写效率，
对应的类：
BufferedWriter
BufferedReader
*/
import java.io.*;
class BufferedWriterDemo {
    public static void main(String[] args) throws IOException {
        // 创建一个字符写入流对象
        FileWriter fw = new FileWriter("buf.txt");

        // 为了提高字符写入流效率，加入了缓冲技术。
        // 只要将需要被提高效率的流对象作为参数传递给缓冲区的构造函数即可
        BufferedWriter bufw = new BufferedWriter(fw);

        bufw.write("abcdefg");

        // 只要用到缓冲区就要刷新
        bufw.flush();
        // 关闭缓冲区就是在关闭缓冲区中的流对象
        bufw.close();
        // fw.close();
    }
}