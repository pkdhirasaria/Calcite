package com.example.demo;

import java.lang.*;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.springframework.boot.SpringApplication;
import org.apache.commons.dbcp.BasicDataSource;


import java.sql.*;

public class DemoApplication {

	public static void main(String[] args) throws ClassNotFoundException, SQLException, ValidationException, RelConversionException {
		SpringApplication.run(DemoApplication.class, args);
		BasicDataSource dataSource = new BasicDataSource();
		ReadFromSqlList(dataSource);
		Class.forName("org.apache.calcite.jdbc.Driver");
		Connection connection =
				DriverManager.getConnection("jdbc:calcite:");
		CalciteConnection calciteConnection =
				connection.unwrap(CalciteConnection.class);
		SchemaPlus rootSchema = calciteConnection.getRootSchema();
		JdbcSchema jdbcSchema = JdbcSchema.create(rootSchema, dataSource.getDefaultCatalog(), dataSource, null, null);
		rootSchema.add(dataSource.getDefaultCatalog(), jdbcSchema);
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery("select * from \"test_db\".\"EMP\"");
		DatabaseMetaData metaData = calciteConnection.getMetaData();
		ResultSet tables = metaData.getTables(dataSource.getDefaultCatalog(), null, null, null);
		while (tables.next()) {
			System.out.println("Table name: " + tables.getString("Table_NAME"));
			System.out.println("Table type: " + tables.getString("TABLE_TYPE"));
			System.out.println("Table schema: " + tables.getString("TABLE_SCHEM"));
			System.out.println("Table catalog: " + tables.getString("TABLE_CAT"));
			System.out.println(" ");
		}
		System.out.println(rs);
		rs.close();
		statement.close();
		connection.close();

	}

	private static void ReadFromSqlList(BasicDataSource dataSource) {
		String url = "jdbc:sqlite:C:\\Users\\OneDrive - SAP SE\\Desktop\\SAP Blue\\DB\\demo.db";
		dataSource.setUrl(url);
		dataSource.setDefaultCatalog("test_db");
		dataSource.setDriverClassName("org.sqlite.JDBC");
	}

}