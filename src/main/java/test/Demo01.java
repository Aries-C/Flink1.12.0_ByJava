package test;

public class Demo01 {
    public static void main(String[] args) {

       String sql1 = "select " +
                "userId," +
                "count(*)," +
                "max(money)," +
                "min(money)" +
                "from t_order" +
                "group by userId," +
                "tumble(createTime, interval '5' second)";
        String sql2 = "select " +
                "userId," +
                "count(*)," +
                "max(money)," +
                "min(money)" +
                "from t_order " +
                "group by userId," +
                "tumble(createTime, interval '5' second)";
        boolean b = sql1.equals(sql2);
        System.out.println(b);
    }
}
