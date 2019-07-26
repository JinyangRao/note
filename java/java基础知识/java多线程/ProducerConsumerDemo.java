class Resource {
    private String name;
    private int count = 1;
    private boolean flag = false;

    // 一些同步函数
    // 同步set操作
    public synchronized void set(String name) {
        while (flag) {
            try {
                this.wait();
            } catch (Exception e) {
            }
        }
        this.name = name + "-" + count++;
        System.out.println(Thread.currentThread().getName() + "...生产者..." + this.name);
        flag = true;
        this.notifyAll();
    }

    // 同步的out操作
    public synchronized void out() {
        while (!flag) {
            try {
                this.wait();
            } catch (Exception e) {
            }
        }
        System.out.println(Thread.currentThread().getName() + "...消费者..." + this.name);
        flag = false;
        this.notifyAll();
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

        Resource res =  new Resource();

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