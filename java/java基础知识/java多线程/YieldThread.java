class Demo implements Runnable {
    public void run() {
        for(int x = 0; x < 70; x++) {
            System.out.println(Thread.currentThread().getName() + "..." + x);

            /**
             * 当前线程放弃执行权，并给其它线程抢占cpu资源
             * 所以yield可以降低线程占用的频率。
             */
            Thread.yield();
        }
    }
}

public class YieldThread {
    public static void main(String[] args) {
        Demo demo = new Demo();
        Thread t1 = new Thread(demo);

        // 设置其线程的优先级
        t1.setPriority(Thread.MAX_PRIORITY);

        /**
         * 线程toString方法打印：
         * Thread-0：表示线程的名字
         * 10：表示线程的优先级
         * main：表示线程所属的线程组
         * Thread[Thread-0,10,main]
         */
        System.out.println(t1.toString());

    }
}