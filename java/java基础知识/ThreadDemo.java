/**
 * 创建线程
 */

public class ThreadDemo {
    public static void main (String[] args) {
        Ticket t = new Ticket();
        t.start();
        t.start();
    }
}

class Ticket extends Thread {
    private int ticket = 100;
    public void run() {
        if (ticket > 0) {
            System.out.println(Thread.currentThread().getName() + ":" + ticket--);
        }
    }
}