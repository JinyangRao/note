
/**
 * 复制一个图片的思路：
 * 用字节流读取流对象和图片关联，
 * 用字节写入流对象，创建一个图片文件，用于存储获取到的图片数据
 * 通过循环读写，完成数据的存储，
 * 关闭资源
 */

import java.io.*;

class CopyPictureDemo {
    public static void main(String[] args) {
         FileInputStream fis = null;
         FileOutputStream fos = null;

         try {

            fis = new FileInputStream("JavaIOPic.png");
            fos = new FileOutputStream("JavaIOPic_copy.png");

            byte[] buf = new byte[1024];

            int len = 0;

            while((len = fis.read(buf)) != -1) {
                fos.write(buf, 0, len);
            }

         } catch (IOException e) {
             throw new RuntimeException("复制文件失败");
         } finally {
             try {
                 if(fos != null)
                    fos.close();
             }catch(Exception e) {
                 e.printStackTrace();
             }
             try {
                if(fis != null)
                    fis.close();
            }catch(Exception e) {
                e.printStackTrace();
            }
         }
        
     }
}