package sh;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
//  Write a HiveQL using Java API to find the top 10 employees earned the highest average salary.
public class q4 {

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

		String query = "select e.first_name, e.last_name , avg(s.salary) as average"
				+ " from employee_salary s full outer join employee e on (s.emp_id = e.emp_id)"
				+ " group by s.emp_id, e.first_name, e.last_name"
				+ " order by average desc limit 10";
		ResultSet employeeres = null;
		try {
			employeeres = statement.executeQuery(query);
			ResultSetMetaData meta = employeeres.getMetaData();

			int numCols = meta.getColumnCount();

			System.out.println("Top 10 highest earning employees:");
			System.out.println("First Name, Last Name, Average Salary");
			while (employeeres.next()) {
				for (int i = 1; i <= numCols; i++) {
					if (i != numCols)
						System.out.print(employeeres.getString(i) + ", ");
					else
						System.out.print(employeeres.getString(i) + "\n");
				}
			}
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}

		employeeres.close();
		statement.close();
		connection.close();
	}
}
			