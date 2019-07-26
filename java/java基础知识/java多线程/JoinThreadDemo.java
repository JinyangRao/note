class Demo implements Runnable {
    public void run() {
        // this.wait();
        for(int x = 0; x < 70; x++) {
            System.out.println(Thread.currentThread().getName() + "..." + x);
        }
    }
}

public class JoinThreadDemo {
    public static void main(String[] args) throws InterruptedException{
        Demo demo = new Demo();
        Thread t1 = new Thread(demo);
        Thread t2 = new Thread(demo);

        t1.start();
        // Waits for this thread to die.
        // t1.join();// 会抛出InterruptedException

        // t2.start();
        t1.join();
        /**
         * 如果是以下的写法：
         * mian线程会放弃执行权，并等待t1结束后再去执行。
         * 所以t1和t2两个线程的执行并不会收到影响，t2并不会去放弃
         * 执行权，而是会和t1进行交替执行。
         * main {
         *  t1.start();
         *  t2.start();
         *  t1.join();
         * }
         * t1如果wait了，其会抛出InterruptedException异常
         * 然后主线程会继续执行
         */

        for(int i = 0; i <= 80; i++) {
            System.out.println("main..." + i);
        }
        System.out.println("Over");
    }
}