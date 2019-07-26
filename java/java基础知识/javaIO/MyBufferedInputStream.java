/**
 * 自己封装一个buffered stream类
 */

import java.io.*;

class MyBufferedInputStream {
    private InputStream in;

    private byte[] buf = new byte[1024];
    private int count = 0, pos = 0;

    MyBufferedInputStream(InputStream in) {
        this.in = in;
    } 

    public int myReader() throws IOException {
        if(count == 0) {
            count = in.read(buf);
            if(count < 0) {
                return -1;
            }
            pos = 0;
            byte b = buf[pos];
            count--;
            pos++;

            // 返回的字节（是文件中的数据）被提升为int的-1,
            // 所有我们需要返回原始数据
            return b & 0xff;
        }else if(count > 0) {
            byte b = buf[pos];
            count--;
            pos++;
            return b & 0xff;
        }
        return -1;
    }

    public void myClose() throws IOException {
        in.close();
    }

}

class MyBufferedInputStreamDemo {
    public static void main(String[] args) throws IOException {
        copyDemo();
    }

    public static void copyDemo() throws IOException {

        // 定义和文件关联的流，输入输出流
        FileInputStream fis = new FileInputStream("JavaIOPic.png");
        FileOutputStream fos = new FileOutputStream("JavaIOPic_copy_byMybuffer.png");

        // 把定义的流包装到缓冲区里面
        MyBufferedInputStream mbufis = new MyBufferedInputStream(fis);
        BufferedOutputStream bufos = new BufferedOutputStream(fos);

        int by = 0;

        // 从输入流中读取，输出到输出流中
        while((by = mbufis.myReader()) != -1) {

            // 虽然我们的字节数据提升为了int by，但是write会只保留int数据的最后8位数据
            bufos.write(by);
        }


        // 关闭流
        mbufis.myClose();
        bufos.close();
    }
}