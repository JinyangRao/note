public class DaemonThreadDemo {
    public static void main(String[] args) {
        Thread t1 = new Thread(new Runnable(){
        
            @Override
            public void run() {
                while(true) {
                    System.out.println("thread run~!");
                }
            }
        });

        /**
         * 当程序中只有守护线程的时候，jvm退出
         * 守护线程又名后台线程
         * 守护线程在线程启动前设置
         */
        t1.setDaemon(true);
        t1.start();
    }
}