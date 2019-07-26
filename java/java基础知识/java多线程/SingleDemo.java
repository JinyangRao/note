// 懒汉式1
class Single {
    private static Single s = null;

    private Single() {
    }

    public static Single getInstance() {
        if (s == null) {
            s = new Single();
        }
        retuen s;
    }
}

// 懒汉式2
class Single2 {
    private static Single2 s = null;

    private Single2() {
    }

    public static synchronized Single2 getInstance() {
        if (s == null) {
            s = new Single2();
        }
        retuen s;
    }
}

// 懒汉式3
class Single3 {
    private static Single3 s = null;

    private Single3() {
    }

    public static Single3 getInstance() {
        if (s == null) {//double
            synchronized (Single3.class) {
                if (s == null) {
                    s = new Single3();
                }
            }
        }
        retuen s;
    }
}