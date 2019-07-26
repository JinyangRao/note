class StopThread implements Runnable {
    
    private boolean flag = true;
    
    public synchronized void run() {
        while(flag) {
            try {
                wait();
            } catch (InterruptedException e) {
                System.out.println(Thread.currentThread().getName() + "-Exception~!");

                /**
                 * 这时候再去设置flag标志位
                 */
                flag = false;
            }

            System.out.println(Thread.currentThread().getName() + "-run");
        }
    }

    public void changeFlag() {
        flag = false;
    }
}

public class StopThreadDemo {
    public static void main(String[] args) {
        StopThread st = new StopThread();

        Thread t1 = new Thread(st);
        Thread t2 = new Thread(st);

        t1.start();
        t2.start();

        int num = 0;

        while(true) {
            if(num ++ == 60) {
                // st.changeFlag();
                /**
                 * interrupt：当线程调用该方法的时候， 会强制从他的wait或者是sleep状态中恢复，但是会报
                 * InterruptedException异常。
                 * 当没有指定的方式让冻结的线程恢复到运行状态时，
                 * 这是需要对冻结的进行清除。
                 * 强制让线程恢复到运行中来。这样就可以让线程结束。
                 * sleep，wait，join都可以被中断
                 */
                t1.interrupt();
                t2.interrupt();
                break;
            }
            System.out.println(Thread.currentThread().getName() + "..." + num);
        }
        System.out.println("main over");
    }
}