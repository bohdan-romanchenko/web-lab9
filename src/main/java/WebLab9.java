import java.sql.*;
import java.util.Random;

public class WebLab9 {

    private static final String DB_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_CONNECTION = "jdbc:mysql://localhost:3306/web_lab9";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "root";

    private static Integer iterations = 500000;

    private static Connection dbConnection;
    private static PreparedStatement preparedStatement;

    public static void main(String[] args) {
        dbConnection = getDBConnection();
        long start = System.currentTimeMillis();
        String table1Name = "WEB_STUDENTS_SMALL";

        try {
            ex1(table1Name);
            ex2(table1Name);
            ex3(table1Name);
            ex4(table1Name);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }

        System.out.println("    ----    ----    TOTAL : " + (System.currentTimeMillis() - start) / 60000 + " min    ----    ----");
    }

    private static void ex4(String tableName) throws SQLException {
        preparedStatement = dbConnection.prepareStatement("ALTER TABLE " + tableName + " ADD avg_zno DOUBLE");
        preparedStatement.execute();
        long start = System.currentTimeMillis();
        preparedStatement = dbConnection.prepareStatement(
                "SELECT * FROM " + tableName + " WHERE form_student = 12");
        ResultSet resultSet = preparedStatement.executeQuery();
        String query = "";
        Integer size = 300;
        Integer counter = 1;
        Integer zno1, zno2, zno3;
        while (resultSet.next()) {
            zno1 = resultSet.getInt("zno1");
            zno2 = resultSet.getInt("zno2");
            zno3 = resultSet.getInt("zno3");
            query += "("
                    + resultSet.getInt("id") + ", "
                    + resultSet.getInt("area_student") + ", "
                    + resultSet.getInt("form_student") + ", "
                    + (double) (zno1 + zno2 + zno3) / 3 + "),";
            counter++;

            if (counter.equals(size)) {
                query = query.substring(0, query.length() - 1);
                preparedStatement = dbConnection.prepareStatement("");
                preparedStatement.execute("INSERT INTO " + tableName +
                        " (id, area_student, form_student, avg_zno) " +
                        "VALUES " + query + " ON DUPLICATE KEY UPDATE " +
                        "area_student = VALUES(area_student)," +
                        "form_student = VALUES(form_student)," +
                        "avg_zno = VALUES(avg_zno);");
                query = "";
                counter = 1;

                System.out.println(
                        "Record is inserted!     --  " +
                                "\n                     TOTAL TIME : " + (System.currentTimeMillis() - start) / 1000 + " ms for 300");
            }
        }
        query = query.substring(0, query.length() - 1);
        preparedStatement = dbConnection.prepareStatement("");
        preparedStatement.execute("INSERT INTO " + tableName +
                " (id, area_student, form_student, avg_zno) " +
                "VALUES " + query + " ON DUPLICATE KEY UPDATE " +
                "area_student = VALUES(area_student)," +
                "form_student = VALUES(form_student)," +
                "avg_zno = VALUES(avg_zno);");
    }

    private static void ex3(String tableName) throws SQLException {
//        preparedStatement = dbConnection.prepareStatement("ALTER TABLE " + tableName + " ADD zno1 INTEGER, ADD zno2 INTEGER, ADD zno3 INTEGER");
//        preparedStatement.execute();
        long start = System.currentTimeMillis();
        preparedStatement = dbConnection.prepareStatement(
                "SELECT * FROM " + tableName + " WHERE form_student = 12");
        ResultSet resultSet = preparedStatement.executeQuery();
        String query = "";
        Integer size = 300;
        Integer counter = 1;
        Random random = new Random();
        Integer zno1, zno2, zno3;
        while (resultSet.next()) {
            zno1 = (random.nextInt(101) + 100);
            zno2 = (random.nextInt(101) + 100);
            zno3 = (random.nextInt(101) + 100);
            query += "("
                    + resultSet.getInt("id") + ", "
                    + resultSet.getInt("area_student") + ", "
                    + resultSet.getInt("form_student") + ", "
                    + zno1 + ", "
                    + zno2 + ", "
                    + zno3 + "),";
            counter++;

            if (counter.equals(size)) {
                query = query.substring(0, query.length() - 1);
                preparedStatement = dbConnection.prepareStatement("");
                preparedStatement.execute("INSERT INTO " + tableName +
                        " (id, area_student, form_student, zno1, zno2, zno3) " +
                        "VALUES " + query + " ON DUPLICATE KEY UPDATE " +
                        "area_student = VALUES(area_student)," +
                        "form_student = VALUES(form_student)," +
                        "zno1 = VALUES(zno1)," +
                        "zno2 = VALUES(zno2)," +
                        "zno3 = VALUES(zno3);");
                query = "";
                counter = 1;

                System.out.println(
                        "Record is inserted!     --  " +
                                "\n                     TOTAL TIME : " + (System.currentTimeMillis() - start) / 1000 + " ms for 300");
            }
        }
        query = query.substring(0, query.length() - 1);
        preparedStatement = dbConnection.prepareStatement("");
        preparedStatement.execute("INSERT INTO " + tableName +
                " (id, area_student, form_student, zno1, zno2, zno3) " +
                "VALUES " + query + " ON DUPLICATE KEY UPDATE " +
                "area_student = VALUES(area_student)," +
                "form_student = VALUES(form_student)," +
                "zno1 = VALUES(zno1)," +
                "zno2 = VALUES(zno2)," +
                "zno3 = VALUES(zno3);");
    }

    private static void ex2(String tableName) throws SQLException {
//        preparedStatement = dbConnection.prepareStatement("ALTER TABLE " + tableName + " ADD average_mark FLOAT");
//        preparedStatement.execute();
        long start = System.currentTimeMillis();
        long current = System.currentTimeMillis();

        String query = "";
        Integer size = 300;
        Integer mark1, mark2, mark3;
        for (int id = 1; id <= 500000; ) {
            preparedStatement = dbConnection.prepareStatement(
                    "SELECT id, area_student, form_student, mark_subject_1, mark_subject_2, mark_subject_3 FROM " + tableName + " WHERE id >= " + id + " AND id < " + (id + size));
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                mark1 = resultSet.getInt("mark_subject_1");
                mark2 = resultSet.getInt("mark_subject_2");
                mark3 = resultSet.getInt("mark_subject_3");
                query += "(" + id++ + ", "
                        + resultSet.getInt("area_student") + ", "
                        + resultSet.getInt("form_student") + ", "
//                        + mark1 + ", "
//                        + mark2 + ", "
//                        + mark3 + ", "
                        + (double) (mark1 + mark2 + mark3) / 3 + "),";

            }
            query = query.substring(0, query.length() - 1);

            preparedStatement = dbConnection.prepareStatement("");
            preparedStatement.execute("INSERT INTO " + tableName +
                    " (id, area_student, form_student,/* mark_subject_1, mark_subject_2, mark_subject_3,*/ average_mark) " +
                    "VALUES " + query + " ON DUPLICATE KEY UPDATE " +
                    "area_student = VALUES(area_student)," +
                    "form_student = VALUES(form_student)," +
//                    "mark_subject_1 = VALUES(mark_subject_1)," +
//                    "mark_subject_2 = VALUES(mark_subject_2)," +
//                    "mark_subject_3 = VALUES(mark_subject_3)," +
                    "average_mark = VALUES(average_mark);");

            query = "";

            if (id % 100000 < 300) {
                System.out.println(
                        "Record is inserted!     --  " + id +
                                "\n                     CURRENT TIME : " + (System.currentTimeMillis() - current) + " sec for 1m" +
                                "\n                     TOTAL TIME : " + (System.currentTimeMillis() - start) / 1000 + " min");
                current = System.currentTimeMillis();

            }
        }
    }

    private static void ex1(String tableName) throws SQLException {

        long start = System.currentTimeMillis();
        long current = System.currentTimeMillis();

        Random random = new Random();
        String query = "";
        Integer size = 300;
        preparedStatement = dbConnection.prepareStatement("");

        for (int id = 1; id <= iterations; id++) {
            String subQuery = "(" + id + ", "
                    + (random.nextInt(100) + 1) + ", "
                    + (random.nextInt(11) + 2) + ", "
                    + (random.nextInt(12) + 1) + ", "
                    + (random.nextInt(12) + 1) + ", "
                    + (random.nextInt(12) + 1) + ")";
            query += subQuery;
            if ((id % size) != 0)
                query += ",";
            else {
                preparedStatement.execute("INSERT INTO " + tableName +
                        " (id, area_student, form_student, mark_subject_1, mark_subject_2, mark_subject_3) VALUES " + query + ";");
                query = "";
            }

            if (id % 100000 == 0) {
                System.out.println(
                        "Record is inserted!     --  " + id +
                                "\n                     CURRENT TIME : " + (System.currentTimeMillis() - current) + " ms for 100k" +
                                "\n                     TOTAL TIME : " + (System.currentTimeMillis() - start) / 1000 + " sec");
                current = System.currentTimeMillis();
            }
        }
        query = query.substring(0, query.length() - 1);
        preparedStatement.execute("INSERT INTO " + tableName +
                " (id, area_student, form_student, mark_subject_1, mark_subject_2, mark_subject_3) VALUES " + query + ";");
    }

    private static Connection getDBConnection() {

        Connection dbConnection = null;

        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }

        try {
            dbConnection = DriverManager.getConnection(
                    DB_CONNECTION, DB_USER, DB_PASSWORD);
            return dbConnection;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return dbConnection;

    }

}
