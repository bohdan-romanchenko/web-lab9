package com.kpi.web.service;

import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Random;

@Service
public class DbService {
    private final String DB_DRIVER = "com.mysql.jdbc.Driver";
    private final String DB_CONNECTION = "jdbc:mysql://localhost:3306/JW";
    private final String DB_USER = "root";
    private final String DB_PASSWORD = "root";
    private Connection dbConnection;

    private String table1Name = "students";
    private Integer iterations = 500000;

    //------------------------String constants-----------------------------------------
    private String korrelation =
            "(SELECT " +
                    "((AVG(average_mark*average_zno) - AVG(average_mark)*AVG(average_zno)) / (STDDEV(average_mark) * STDDEV(average_zno))) " +
                    "FROM " + table1Name + " WHERE form = 12 ";

    private String averageMark2and5Deviation =
            "((ABS(average_mark - mark_subject_1) > 2.5) OR " +
                    "(ABS(average_mark - mark_subject_2) > 2.5) OR " +
                    "(ABS(average_mark - mark_subject_3) > 2.5))";

    private String averageMark5Deviation = averageMark2and5Deviation.replace("2.5", "5");

    private String averageZno25Deviation =
            "((ABS(average_zno - zno1) > 25) OR " +
                    "(ABS(average_zno - zno2) > 25) OR " +
                    "(ABS(average_zno - zno3) > 25))";

    private String averageZno50Deviation = averageZno25Deviation.replace("25", "50");
//----------------------------------------------------------------------------------------------

    //main function of service
    public String run() {

        String output = "";

        dbConnection = getDBConnection();
        long start = System.currentTimeMillis();

        try {
            ex1(table1Name);
            System.out.println("---- task1 time : " + (System.currentTimeMillis() - start) / 1000 + " sec ---- ");
            output += "---- task1 time : " + (System.currentTimeMillis() - start) / 1000 + " sec ---- \n";
            ex2(table1Name);
            System.out.println("---- task2 time : " + (System.currentTimeMillis() - start) / 1000 + " sec ---- ");
            output += "---- task2 time : " + (System.currentTimeMillis() - start) / 1000 + " sec ---- \n";
            ex3(table1Name);
            System.out.println("---- task3 time : " + (System.currentTimeMillis() - start) / 1000 + " sec ---- ");
            output += "---- task3 time : " + (System.currentTimeMillis() - start) / 1000 + " sec ---- \n";
            ex4(table1Name);
            System.out.println("---- task4  time : " + (System.currentTimeMillis() - start) / 1000 + " sec ---- ");
            output += "---- task4 time : " + (System.currentTimeMillis() - start) / 1000 + " sec ---- \n";
            String table2Name = "table2";
            ex5(table1Name, table2Name);
            System.out.println("---- task5  time : " + (System.currentTimeMillis() - start) / 1000 + " sec ---- ");
            output += "---- task5 time : " + (System.currentTimeMillis() - start) / 1000 + " sec ---- \n";
            ex6(table1Name, table2Name);
            System.out.println("---- task6  time : " + (System.currentTimeMillis() - start) / 1000 + " sec ---- ");
            output += "---- task6 time : " + (System.currentTimeMillis() - start) / 1000 + " sec ---- \n";
            String table3Name = "table3";
            ex7(table3Name);
            System.out.println("---- task7  time : " + (System.currentTimeMillis() - start) / 1000 + " sec ---- ");
            output += "---- task7 time : " + (System.currentTimeMillis() - start) / 1000 + " sec ---- \n";
            ex8(table3Name, "5");
            System.out.println("---- task8  time : " + (System.currentTimeMillis() - start) / 1000 + " sec ---- ");
            output += "---- task8 time : " + (System.currentTimeMillis() - start) / 1000 + " sec ---- \n";
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        System.out.println("---- TOTAL TIME : " + (System.currentTimeMillis() - start) / 1000 + " sec ----");
        output += "---- TOTAL TIME : " + (System.currentTimeMillis() - start) / 1000 + " sec ----\n";

        return output;
    }

    private void ex8(String tableName, String area) throws SQLException {
        dbConnection.prepareStatement(
                "ALTER TABLE " + tableName + "" +
                        "   ADD 7_1_area FLOAT," +
                        "   ADD 7_2_area FLOAT," +
                        "   ADD 7_3_area FLOAT," +
                        "   ADD 7_4_area FLOAT," +
                        "   ADD 7_5_area FLOAT," +
                        "   ADD 7_6_area FLOAT," +
                        "   ADD 7_7_area FLOAT;").execute();

        dbConnection.prepareStatement(
                "UPDATE " + tableName + " " +
                        "SET " +
                        "7_1_area = " + korrelation + "), " +
                        "7_2_area = " + korrelation + " AND area = " + area + " AND " + averageMark2and5Deviation + "), " +
                        "7_3_area = " + korrelation + " AND area = " + area + " AND " + averageZno25Deviation + "), " +
                        "7_4_area = " + korrelation + " AND area = " + area + " AND " + averageMark2and5Deviation + " AND " + averageZno25Deviation + "), " +
                        "7_5_area = " + korrelation + " AND area = " + area + " AND " + averageMark5Deviation + "), " +
                        "7_6_area = " + korrelation + " AND area = " + area + " AND " + averageZno50Deviation + "), " +
                        "7_7_area = " + korrelation + " AND area = " + area + " AND " + averageMark5Deviation + " AND " + averageZno50Deviation + ")" +
                        "LIMIT 1;").execute();
    }

    private void ex7(String table3Name) throws SQLException {
        dbConnection.prepareStatement("DROP TABLE IF EXISTS " + table3Name + ";").execute();
        dbConnection.prepareStatement("CREATE TABLE " + table3Name + "(" +
                "`7_1` FLOAT," +
                "`7_2` FLOAT," +
                "`7_3` FLOAT," +
                "`7_4` FLOAT," +
                "`7_5` FLOAT," +
                "`7_6` FLOAT," +
                "`7_7` FLOAT)" +
                "ENGINE = MyISAM;").execute();

        dbConnection.prepareStatement(
                "INSERT INTO " +
                        table3Name + "(" +
                        "   7_1, " +
                        "   7_2, " +
                        "   7_3, " +
                        "   7_4, " +
                        "   7_5, " +
                        "   7_6, " +
                        "   7_7) " +
                        "VALUES(" +
                        korrelation + "), " +
                        korrelation + " AND " + averageMark2and5Deviation + "), " +
                        korrelation + " AND " + averageZno25Deviation + "), " +
                        korrelation + " AND " + averageMark2and5Deviation + " AND " + averageZno25Deviation + "), " +
                        korrelation + " AND " + averageMark5Deviation + "), " +
                        korrelation + " AND " + averageZno50Deviation + "), " +
                        korrelation + " AND " + averageMark5Deviation + " AND " + averageZno50Deviation + "));"
        ).execute();

    }

    private void ex6(String table1Name, String table2Name) throws SQLException {
        dbConnection.prepareStatement("ALTER TABLE " + table2Name + " ADD zno_dev_25 INT, ADD zno_dev_50 INT;").execute();
        String request = "UPDATE " +
                table2Name + " " +
                "SET " +
                "   zno_dev_25 = (" +
                "SELECT " +
                "   COUNT(id) " +
                "FROM " +
                table1Name + " " +
                "WHERE " +
                "   form = 12 AND" +
                "   ((ABS(average_zno - zno1) > 25) OR " +
                "   (ABS(average_zno - zno2) > 25) OR " +
                "   (ABS(average_zno - zno3) > 25))) " +
                "LIMIT 1";

        dbConnection.prepareStatement(request).execute();
        request = request.replace("zno_dev_25", "zno_dev_50");
        request = request.replace("25", "50");
        dbConnection.prepareStatement(request).execute();
    }

    private void ex5(String table1Name, String table2Name) throws SQLException {
        dbConnection.prepareStatement("DROP TABLE IF EXISTS " + table2Name + ";").execute();
        dbConnection.prepareStatement("CREATE TABLE " + table2Name +
                "(`general_dev` INT," +
                "`2_dev` INT," +
                "`3_dev` INT," +
                "`4_dev` INT," +
                "`5_dev` INT," +
                "`6_dev` INT," +
                "`7_dev` INT," +
                "`8_dev` INT," +
                "`9_dev` INT," +
                "`10_dev` INT," +
                "`11_dev` INT," +
                "`12_dev` INT)" +
                "ENGINE = MyISAM;").execute();


        dbConnection.prepareStatement(
                "INSERT INTO " +
                        table2Name + "(general_dev)" +
                        "SELECT " +
                        "   COUNT(students.id) " +
                        "FROM " +
                        table1Name + " " +
                        "WHERE " +
                        "   (ABS(average_mark - mark_subject_1) > 2.5) OR " +
                        "   (ABS(average_mark - mark_subject_2) > 2.5) OR " +
                        "   (ABS(average_mark - mark_subject_3) > 2.5);").execute();

        for (int form = 2; form <= 12; form++) {
            dbConnection.prepareStatement(
                    "UPDATE " +
                            table2Name + " " +
                            "SET "
                            + form + "_dev = (" +
                            "SELECT " +
                            "   COUNT(id) " +
                            "FROM " +
                            table1Name + " " +
                            "WHERE " +
                            "   form = " + form + " AND" +
                            "   ((ABS(average_mark - mark_subject_1) > 2.5) OR " +
                            "   (ABS(average_mark - mark_subject_2) > 2.5) OR " +
                            "   (ABS(average_mark - mark_subject_3) > 2.5))) " +
                            "LIMIT 1").execute();
        }

    }

    private void ex4(String tableName) throws SQLException {
        dbConnection.prepareStatement("ALTER TABLE " + tableName + " ADD average_zno FLOAT").execute();
        dbConnection.prepareStatement("UPDATE " + tableName + " SET " +
                "average_zno = (zno1 + zno2 + zno3) / 3 " +
                "WHERE form = 12").execute();
    }

    private void ex3(String tableName) throws SQLException {
        dbConnection.prepareStatement("ALTER TABLE " + tableName + " ADD zno1 INTEGER, ADD zno2 INTEGER, ADD zno3 INTEGER").execute();
        dbConnection.prepareStatement("UPDATE " + tableName + " SET " +
                "zno1 = FLOOR(RAND() * 100) + 101," +
                "zno2 = FLOOR(RAND() * 100) + 101," +
                "zno3 = FLOOR(RAND() * 100) + 101 " +
                "WHERE form = 12;").execute();
    }

    private void ex2(String tableName) throws SQLException {
        dbConnection.prepareStatement("ALTER TABLE " + tableName + " ADD average_mark FLOAT").execute();
        dbConnection.prepareStatement("UPDATE " + tableName + " SET average_mark = (mark_subject_1 + mark_subject_2 + mark_subject_3) / 3").execute();
    }

    private void ex1(String tableName) throws SQLException {

        dbConnection.prepareStatement("DROP TABLE IF EXISTS " + tableName + ";").execute();
        dbConnection.prepareStatement("CREATE TABLE " + tableName +
                "(`id` INT NOT NULL AUTO_INCREMENT," +
                "`area` INT NOT NULL," +
                "`form` INT NOT NULL," +
                "`mark_subject_1` INT NULL," +
                "`mark_subject_2` INT NULL," +
                "`mark_subject_3` INT NULL," +
                "PRIMARY KEY (`id`)," +
                "UNIQUE INDEX `id_UNIQUE` (`id` ASC))" +
                "ENGINE = MyISAM;").execute();

        Random random = new Random();
        StringBuilder query = new StringBuilder();

        for (int id = 1; id <= iterations; id++) {
            String subQuery = "(" + id + ", "
                    + (random.nextInt(100) + 1) + ", "
                    + (random.nextInt(11) + 2) + ", "
                    + (random.nextInt(12) + 1) + ", "
                    + (random.nextInt(12) + 1) + ", "
                    + (random.nextInt(12) + 1) + ")";
            query.append(subQuery);

            Integer packSize = 1000;
            if ((id % packSize) != 0 && id != iterations)
                query.append(",");
            else {
                dbConnection.prepareStatement("INSERT INTO " + tableName +
                        " (id, area, form, mark_subject_1, mark_subject_2, mark_subject_3) VALUES " + query + ";").execute();
                query.delete(0, query.length());
            }
        }
    }

    private Connection getDBConnection() {

        Connection dbConnection = null;

        try {
            Class.forName(DB_DRIVER);
            dbConnection = DriverManager.getConnection(
                    DB_CONNECTION, DB_USER, DB_PASSWORD);

        } catch (ClassNotFoundException | SQLException e) {
            System.out.println(e.getMessage());
        }

        return dbConnection;
    }
}
