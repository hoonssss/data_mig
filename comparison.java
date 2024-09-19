package com.example.sql_mig.comaprison;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

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
            Set<String> srcColumns = getTableColumns(srcConn, srcTable);

            // 타겟 테이블 컬럼 목록 가져오기
            Set<String> tarColumns = getTableColumns(trcConn, tarTable);

            // 소스 테이블에만 있는 컬럼
            Set<String> srcOnly = new HashSet<>(srcColumns);
            srcOnly.removeAll(tarColumns); // 타겟 컬럼들과 중복 제거

            // 타겟 테이블에만 있는 컬럼
            Set<String> tarOnly = new HashSet<>(tarColumns);
            tarOnly.removeAll(srcColumns); // 소스 컬럼들과 중복 제거

            System.out.println("소스 테이블에만 있는 컬럼: " + srcOnly);
            System.out.println("타겟 테이블에만 있는 컬럼: " + tarOnly);

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
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // 테이블의 컬럼 목록을 가져오는 메소드
    private static Set<String> getTableColumns(Connection conn, String tableName) throws Exception {
        Set<String> columns = new HashSet<>();
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getColumns(null, null, tableName, null);

        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            columns.add(columnName);
        }

        return columns;
    }
}
