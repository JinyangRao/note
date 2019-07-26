import java.io.*;

class BufferedReaderDemo  {
    public static void main(String[] args) throws IOException{
        // 创建一个读取流对象和文件相关联
        FileReader fr = new FileReader("buf.txt");

        // 为了提高效率，加入缓冲区技术，
        // 将字符读取流对象作为参数传递给缓冲对象的构造函数
        BufferedReader bufr = new BufferedReader(fr);

        String line = null;

        // 当读取到文件末尾的时候，返回null
        while((line = bufr.readLine()) != null) {
            System.out.println(line);
        }
        // 只关闭缓冲区就行，文件字符读取流不需要关闭
        bufr.close();
    } 
}