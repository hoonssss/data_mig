package com.example.sql_mig.comaprison;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

public class col_comparison {

    public static void main(String[] args) {
        Connection srcConn = null;
        Connection trcConn = null;
        String srcTable = "BATCH_JOB_INSTANCE";
        String tarTable = "BATCH_JOB_INSTANCE_1";

        try {
            // 소스와 타겟 데이터베이스 연결 설정
            srcConn = DriverManager.getConnection("jdbc:mysql://localhost:3307/springbatch", "root", "password");
            trcConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/testDB", "root", "password");

            // 소스 테이블 컬럼 목록 가져오기
            Map<String, String> srcColumns = getTableColumns(srcConn, srcTable);

            // 타겟 테이블 컬럼 목록 가져오기
            Map<String, String> tarColumns = getTableColumns(trcConn, tarTable);

            // 소스 테이블에만 있는 컬럼
            Set<String> srcOnly = new HashSet<>(srcColumns.keySet());
            srcOnly.removeAll(tarColumns.keySet());

            // 타겟 테이블에만 있는 컬럼
            Set<String> tarOnly = new HashSet<>(tarColumns.keySet());
            tarOnly.removeAll(srcColumns.keySet());

            System.out.println("소스 테이블에만 있는 컬럼: " + srcOnly);
            System.out.println("타겟 테이블에만 있는 컬럼: " + tarOnly);

            // 타겟 테이블에만 있는 컬럼의 타입과 자리수 출력
            System.out.println("타겟 테이블에만 있는 컬럼 타입 및 자리수:");
            for (String column : tarOnly) {
                System.out.println(column + " : " + tarColumns.get(column));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (srcConn != null && !srcConn.isClosed()) {
                    srcConn.close();
                }
                if (trcConn != null && !trcConn.isClosed()) {
                    trcConn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // 테이블의 컬럼 목록과 데이터 타입을 가져오는 메서드
    private static Map<String, String> getTableColumns(Connection conn, String tableName) throws Exception {
        Map<String, String> columns = new HashMap<>();
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getColumns(null, null, tableName, null);

        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            String columnType = rs.getString("TYPE_NAME"); // 데이터 타입
            int columnSize = rs.getInt("COLUMN_SIZE");    // 자리수 또는 길이

            // 데이터 타입과 자리수를 조합하여 저장
            columns.put(columnName, columnType + "(" + columnSize + ")");
        }

        return columns;
    }
}
