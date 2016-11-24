import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

public class WebLab9 {

    private static final String DB_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_CONNECTION = "jdbc:mysql://127.0.0.1:3306/JW?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "root";

    public static void main(String[] args) {
        System.out.println("Valysha sexy");
        long start = System.currentTimeMillis();
        try {

            Connection dbConnection;
            PreparedStatement preparedStatement;
            dbConnection = getDBConnection();
            String insertTableSQL = "INSERT INTO W_STUDENTS"
                    + "(id, area_student, form_student, mark_subject_1, mark_subject_2, mark_subject_3) VALUES"
                    + "(?,?,?,?,?,?)";
            preparedStatement = dbConnection.prepareStatement(insertTableSQL);

            insertRecordIntoTable(preparedStatement, dbConnection);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }

        System.out.println("    ----    ----    TOTAL : " + (System.currentTimeMillis() - start) / 60000 + " min    ----    ----");

    }

    private static void insertRecordIntoTable(PreparedStatement preparedStatement, Connection dbConnection) throws SQLException {

        int id = 1;
        long start = System.currentTimeMillis();
        try {
            long current = System.currentTimeMillis();
            for (; id < 5000000; id++) {
                String content = "";
                //-------------
                Random random = new Random();
                content += id + " ";
                content += (random.nextInt(100) + 1) + " ";
                content += (random.nextInt(11) + 2) + " ";
                content += (random.nextInt(12) + 1) + " ";
                content += (random.nextInt(12) + 1) + " ";
                content += (random.nextInt(12) + 1) + " \n";
                File file = new File("/home/nadman/Study/WebFillDataBase/data.csv");

                // if file doesnt exists, then create it
                try{

                    if (!file.exists() && !file.createNewFile()) {
                        break;
                    }

                    FileWriter fileWritter = new FileWriter(file.getAbsoluteFile(),true);
                    BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
                    bufferWritter.write(content);
                    bufferWritter.close();

                }catch (IOException e) {
                    e.printStackTrace();
                }

                if (id % 1000000 == 0) {
                    System.out.println(
                            "Record is inserted!     --  " + id +
                                    "\n                     CURRENT TIME : " + (System.currentTimeMillis() - current) / 1000 + " sec for 1m" +
                                    "\n                     TOTAL TIME : " + (System.currentTimeMillis() - start) / 60000 + " min");
                    current = System.currentTimeMillis();

                }

            }
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (dbConnection != null) {
                dbConnection.close();
            }
        }

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
