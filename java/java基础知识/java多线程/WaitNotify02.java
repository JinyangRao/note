class Resource {
    private String name;
    private String sex;

    private boolean tag = false;

    public synchronized void set(String name, String sex) {
        if (flage) {
            try {
                this.wait();
            } catch (Exception e) {
            }
        }
        this.name = name;
        this.sex = sex;
        tag = true;
        this.notify();
    }

    public synchronized void out() {
        if (!tag)
            try {
                this.wait();
            } catch (Exception e) {
            }
        System.out.println(name + "--" + sex);
        tag = false;
        this.notify();
    }
}

/**
 * wait会让当前的线程释放所持有的锁
 */
class Input implements Runnable {
    private Resource res;

    public Input(Resource res) {
        this.res = res;
    }

    public void run() {
        int flag = 0;
        while (true) {

            if (flag == 0)
                res.set("mike", "man");
            else
                res.set("丽丽", "女");
            flag = (flag + 1) % 2;
        }
    }
}

public class WaitNotify02 {
    public static void main(String[] args) {
        Resource res = new Resource();

        new Thread(new Input(res)).start();

        new Thread(new Output(res)).start();

        // Input in = new Input(res);
        // Output out = new Output(res);

        // Thread tIn = new Thread(in);
        // Thread tOut = new Thread(out);

        // tIn.start();
        // tOut.start();
    }
}