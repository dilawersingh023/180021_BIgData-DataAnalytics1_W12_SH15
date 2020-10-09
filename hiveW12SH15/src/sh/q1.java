package sh;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
// Write a HiveQL using Java API to create a table for Employee and Salary data set
public class q1 {

	private static String driverClass = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverClass);
		} catch (ClassNotFoundException exception) {
			System.out.println(exception.toString());
			System.exit(1);
		}
				Connection connection = DriverManager.getConnection(
						"jdbc:hive2://localhost:10000/default", "hive", "");

				Statement statement = connection.createStatement();
				
				String createemployee = "create table employee ("
						+ "emp_id	int, birthday string, first_name string, last_name string, gender string, work_day string"
						+ ")" + " row format delimited" + " fields terminated by ','";

				try {
					statement.execute(createemployee);
					System.out.println("Created table employee.");
				} catch (SQLException ex) {
					System.out.println(ex.toString());
				}

							String createSalary = "create table employee_salary ("
						+ "emp_id int, salary string, start_date string, end_date string"
						+ ")" + " row format delimited" + " fields terminated by ','";
				try {
					statement.execute(createSalary);
					System.out.println("Created table employee_salary.");
				} catch (SQLException ex) {
					System.out.println(ex.toString());
				}

				statement.close();
				connection.close();
	}
}

				