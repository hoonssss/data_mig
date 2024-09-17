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

                LocalDate currentDate = LocalDate.now();
                Date sqlDate = Date.valueOf(currentDate);

                // Append BS_YMD to SELECT and INSERT statements
                selectColumns.append("'").append(sqlDate).append("' AS BS_YMD");
                selectColumns.append(", ");
                insertColumns.append("BS_YMD");
                insertColumns.append(", ");
                insertValues.append("?");
                insertValues.append(", ");

                // Append LAST_USER to SELECT and INSERT statements
                selectColumns.append("'MIG' AS LAST_USER");
                selectColumns.append(", ");
                insertColumns.append("LAST_USER");
                insertColumns.append(", ");
                insertValues.append("?");
                insertValues.append(", ");

                // Append actual columns from the source table
                int columnCount = 2; // Already added BS_YMD and LAST_USER
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");

                    if (columnCount > 2) {
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
                    insertStmt.setString(2, "MIG");  // LAST_USER

                    for (int i = 1; i <= columnCount; i++) { // 3부터 시작
                        Object value = rs.getObject(i);  // 인덱스 조정

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
