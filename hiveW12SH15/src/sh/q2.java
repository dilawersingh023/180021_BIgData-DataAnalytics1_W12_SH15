package sh;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
//Write a HiveQL using Java API for loading data into Employee and Salary Tables from the given the data set files.
public class q2 {

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

		String loademployee = "load data local inpath '/home/cloudera/Downloads/employee.csv' overwrite into table employee";
		try {
			statement.execute(loademployee);
			System.out.println("Data loaded into table employee.");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}
		String loademployeeSalary = "load data local inpath '/home/cloudera/Downloads/salary.csv' overwrite into table employee_salary";
		try {
			statement.execute(loademployeeSalary);
			System.out.println("Data loaded into table employee_salary.");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}

		statement.close();
		connection.close();
	}
}
	
	
