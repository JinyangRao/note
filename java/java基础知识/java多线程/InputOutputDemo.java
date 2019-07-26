class Resource {
    String name;
    String sex;
}

class Input implements Runnable {
    private Resource res;

    public Input(Resource res) {
        this.res = res;
    }

    public void run() {
        int flag = 0;
        while (true) {
            // int flag = 0;
            synchronized (res) {
                if (flag == 0) {
                    res.name = "mike";
                    res.sex = "man";
                } else {
                    res.name = "小明";
                    res.sex = "女";
                }

                flag = (flag + 1) % 2;
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
                System.out.println(res.name + "..." + res.sex);
            }
        }
    }
}

public class InputOutputDemo {
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