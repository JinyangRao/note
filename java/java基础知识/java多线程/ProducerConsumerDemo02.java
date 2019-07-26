
/** 
 * notifyAll如果不唤醒自己类的线程，就可以使得程序运行的效率得到提高
 * jdk1.5可以进行优化，利用Lock和Condition 来进行优化
*/

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class Resource {
    private String name;
    private int count = 1;
    private boolean flag = false;

    private Lock lock = new ReentrantLock();

    private Condition conditionProductor = lock.newCondition();
    private Condition conditionConsumer = lock.newCondition();

    // 同步set操作
    public void set(String name) throws InterruptedException {
        lock.lock(); // lock
        try {
            while (flag) {
                conditionProductor.await();
            }
            this.name = name + "-" + count++;
            System.out.println(Thread.currentThread().getName() + "...生产者..." + this.name);
            flag = true;
            conditionConsumer.signal();
        } finally {
            lock.unlock(); // unlock
        }
    }

    // 同步的out操作
    public void out() throws InterruptedException {
        lock.lock();
        try {
            while (!flag) {
                conditionConsumer.await();
            }
            System.out.println(Thread.currentThread().getName() + "...消费者..." + this.name);
            flag = false;
            conditionProductor.signal();
        } finally {
            lock.unlock();
        }
    }
}

class Producer implements Runnable {

    private Resource res;

    public Producer(Resource res) {
        this.res = res;
    }

    public void run() {
        while (true) {
            res.set("商品");
        }
    }
}

class Consumer implements Runnable {
    private Resource res;

    public Consumer(Resource res) {
        this.res = res;
    }

    public void run() {
        while (true) {
            res.out();
        }
    }
}

public class ProducerConsumerDemo {
    public static void main(String[] args) {

        Resource res = new Resource();

        Producer pro = new Producer(res);
        Consumer con = new Consumer(res);

        Thread t1 = new Thread(pro);
        Thread t2 = new Thread(pro);
        Thread t3 = new Thread(pro);

        Thread t4 = new Thread(con);
        Thread t5 = new Thread(con);
        Thread t6 = new Thread(con);

        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();
        t6.start();

    }
}