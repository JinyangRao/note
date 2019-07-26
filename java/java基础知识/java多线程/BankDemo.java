/**
 * 同步函数
 * 
 * synchronized 修饰run方法会有问题
 * synchronized 修饰成员函数用的是this当前对象为锁
 * 如果同步函数是用static修饰，那么该函数就是用该类Class对象作为锁
 **/

 class Ticket implements Runnable {
     public void run() {
         synchronized (Ticket.class) {//class对象最为锁
             // do something
         }
     }
 }

class Bank {
    private int sum;
    public synchronized void add(int n) {

        try {
            Thread.sleep(10);
        }catch(Exception e) {
            e.printStackTrace();
        }
        sum =  sum + n;
        System.out.println("sum" + sum);
    }
}

class Cus implements Runnable {
    private Bank b = new Bank();

    public void run() {
        for(int x = 0; x < 3; x++) {
            b.add(100);
        }
    }
}

public class BankDemo {
    public static void main(String[] args) {
        Cus c = new Cus();

        Thread t1 = new Thread(c);
        Thread t2 = new Thread(c);

        t1.start();
        t2.start();
    }
}