class Resource {
    String name;
    String sex;

    boolean tag = false;
}
/**
 * wait会让当前的线程释放所持有的锁，在唤醒后，会执行wait后面的代码
 */
class Input implements Runnable {
    private Resource res;

    public Input(Resource res) {
        this.res = res;
    }

    public void run() {
        int flag = 0;
        while (true) {
            synchronized (res) {

                if(res.tag) {
                    try {
                        res.wait();
                    }catch (Exception e) {}
                }
                if (flag == 0) {
                    res.name = "mike";
                    res.sex = "man";
                } else {
                    res.name = "小明";
                    res.sex = "女";
                }
                flag = (flag + 1) % 2;
                res.tag = true;
                res.notify();
            }
        }
    }
}

class Output implements Runnable {

    private Resource res;

    public Output(Resource res) {
        this.res = res;
    }

    public void run() {
        while (true) {
            synchronized (res) {

                if(!res.tag) {
                    try {
                        res.wait();
                    }catch(Exception e) {}
                }
                System.out.println(res.name + "..." + res.sex);
                res.tag = false;
                res.notify();
            }
        }
    }
}

public class WaitNotify {
    public static void main(String[] args) {
        Resource res = new Resource();

        Input in = new Input(res);
        Output out = new Output(res);

        Thread tIn = new Thread(in);
        Thread tOut = new Thread(out);

        tIn.start();
        tOut.start();
    }
}