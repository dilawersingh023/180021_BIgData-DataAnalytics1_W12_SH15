package sh;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
//Write a HiveQL using Java API to retrieve the top 10 oldest employees.
public class q3 {

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

		String query = "select e.first_name, e.last_name, s.start_date"
				+ " from employee e inner join employee_salary s on (e.emp_id = s.emp_id)"
				+ " order by s.start_date asc limit 10";

	
		ResultSet employeeres = null;
		try {
			employeeres = statement.executeQuery(query);
			ResultSetMetaData meta = employeeres.getMetaData();

			int numCols = meta.getColumnCount();

			System.out.println("Top 10 oldest employees:");
			System.out.println("First Name, Last Name, Start Date");
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
			