/**
 * 继承Thread和实现Runnable接口两种方式的区别：
 * 避免了单继承的局限性：
 * 比如一个学生类，他已经是继承了person类，
 * 但是学生类有代码需要进程并行执行，所以我们就不能继续集成Thread类了。
 * 所以我们可以去实现Runnable接口来进行。
 * 
 * 继承Thread：线程代码存放在Thread子类的run方法中
 * 实现Runable：线程代码存在实现Runnable接口的子类中
*/

class TicketImp implements Runnable {
        private int tick = 100;
        public void run() {
            while (true) {
                synchronized (this) {
                if(tick > 0) {
                    try {
                        Thread.sleep(10);
                    }catch(InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName() + ":" + tick--);
                }
            }
            }
        }
}

public class ThreadDemo02 {
    public static void main(String[] args) {
        TicketImp ti = new TicketImp();

        Thread t1 = new Thread(ti);
        Thread t2 = new Thread(ti);
        Thread t3 = new Thread(ti);
        Thread t4 = new Thread(ti);
        t1.start();
        t2.start();
        t3.start();
        t4.start();
    }
}