import java.io.FileReader;

class FileReaderDemo02 {
    public static void main(String[] args) throws Exception {
        FileReader fr = new FileReader("Demo.txt");

        // 存储的数组，通常定义1024的整数倍
        char[] buf = new char[3];

        // 读取文件内容到字符数组中，返回读取字符数，num返回-1表示读取完毕
        int num = 0;
        while ((num = fr.read(buf)) != -1) {
            System.out.println(num + "_" + new String(buf, 0, num));
        }
        fr.close();
    }
}