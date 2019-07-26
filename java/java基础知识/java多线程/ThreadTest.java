class TreadTest {
    public static void main(String[] args) {
        //1.写法匿名内部类
        new Thread() {
            public void run() {
                // code
            }
        }.start();

        //2.匿名内部类写法
        new Thread(new Runnable(){
        
            @Override
            public void run() {
                // code
            }
        }).start();
    }
}