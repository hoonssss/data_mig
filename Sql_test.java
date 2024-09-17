package com.example.sql_mig;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class SqlMigApplication {

    public static void main(String[] args) {
        Connection srcConn = null;
        Connection trcConn = null;
        List<String> arrScrTbl = new ArrayList<>();
        arrScrTbl.add("BATCH_JOB_INSTANCE");
        arrScrTbl.add("BATCH_JOB_EXECUTION");

        List<String> arrTarTbl = new ArrayList<>();
        arrTarTbl.add("BATCH_JOB_INSTANCE_1");
        arrTarTbl.add("BATCH_JOB_EXECUTION_1");
/*        String arrScrTbl[] = {
            "BATCH_JOB_INSTANCE",r
            "BATCH_JOB_EXECUTION"
        };*/

        //String scrTbl = "BATCH_JOB_INSTANCE";
        //String tarTbl = "BATCH_JOB_INSTANCE_1";
        String scrTbl = "", tarTbl = "";
        for (var k = 0; k < arrScrTbl.size(); k++) {
            scrTbl = arrScrTbl.get(k);
            tarTbl = arrTarTbl.get(k);
            try {
                srcConn = DriverManager.getConnection("jdbc:mysql://localhost:3307/springbatch", "root", "password");
                trcConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/testDB", "root", "password");

                DatabaseMetaData metaData = srcConn.getMetaData();
                ResultSet columns = metaData.getColumns(null, null, scrTbl, null);

                StringBuilder selectColumns = new StringBuilder();
                StringBuilder insertColumns = new StringBuilder();
                StringBuilder insertValues = new StringBuilder();

                int columnCount = 0;

                LocalDate currentDate = LocalDate.now();
                Date sqlDate = Date.valueOf(currentDate);

                // Append BS_YMD to SELECT and INSERT statements
                selectColumns.append("'").append(sqlDate).append("' AS BS_YMD");
                selectColumns.append(", ");
                insertColumns.append("BS_YMD");
                insertColumns.append(", ");
                insertValues.append("?");
                insertValues.append(", ");

                columnCount++;

                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");

                    if (columnCount > 1) {
                        selectColumns.append(", ");
                        insertColumns.append(", ");
                        insertValues.append(", ");
                    }

                    selectColumns.append(columnName);
                    insertColumns.append(columnName);
                    insertValues.append("?");

                    columnCount++;
                }

                String selectQuery = "SELECT " + selectColumns.toString() + " FROM " + scrTbl;
                String deleteQuery = "DELETE FROM " + tarTbl;
                String insertQuery =
                    "INSERT INTO " + tarTbl + " (" + insertColumns.toString() + ") VALUES ("
                        + insertValues.toString() + ")";

                PreparedStatement selectStmt = srcConn.prepareStatement(selectQuery);
                PreparedStatement insertStmt = trcConn.prepareStatement(insertQuery);
                PreparedStatement deleteStmt = trcConn.prepareStatement(deleteQuery);

                log.info(selectStmt.toString());
                log.info(insertStmt.toString());
                log.info(deleteStmt.toString());

                ResultSet rs = selectStmt.executeQuery();

                // 데이터 삭제
                deleteStmt.executeUpdate();

                while (rs.next()) {
                    insertStmt.setDate(1, sqlDate);  // BS_YMD
                    log.info("Setting parameter " + 1 + " to value: " + sqlDate + " type: "
                        + sqlDate.getClass().getName());
                    for (int i = 2; i <= columnCount; i++) {
                        Object value = rs.getObject(i);

                        // 데이터 타입을 명시적으로 설정
                        if (value instanceof Long) {
                            insertStmt.setLong(i, (Long) value);
                        } else if (value instanceof Integer) {
                            insertStmt.setLong(i, ((Integer) value).longValue()); // Long으로 변환
                        } else if (value instanceof String) {
                            insertStmt.setString(i, (String) value);
                        } else if (value instanceof Date) {
                            insertStmt.setDate(i, (Date) value);
                        } else {
                            insertStmt.setObject(i, value);
                        }

                        //log.info("Setting parameter " + i + " to value: " + value + " type: " + value.getClass().getName());
                    }

                    insertStmt.executeUpdate();
                }

                srcConn.close();
                trcConn.close();
            } catch (Exception e) {
                log.error("에러 발생: " + e.getMessage(), e);
            } finally {
                try {
                    if (srcConn != null && !srcConn.isClosed()) {
                        srcConn.close();
                    }
                    if (trcConn != null && !trcConn.isClosed()) {
                        trcConn.close();
                    }
                } catch (Exception e) {
                    log.error("에러 발생: " + e.getMessage(), e);
                }
            }
        }
    }
}

