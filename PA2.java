/**
* Name: Adrian Jacinto
* PID: A12779639
* 
* This Java program exemplifies the basic usage of JDBC.
* Requirements:
* (1) JDK 8.0+
* (2) SQLite3.
* (3) SQLite3 JDBC jar (https://bitbucket.org/xerial/sqlitejdbc/downloads/sqlite-jdbc-3.8.7.jar).
*/

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;  
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PA2 {

	public static void main(String[] args) {
		 Connection conn = null; // Database connection.
		 
		 try {
			 // Load the JDBC class.
			 Class.forName("org.sqlite.JDBC");
			 // Get the connection to the database.
			 // - "jdbc" : JDBC connection name prefix.
			 // - "sqlite" : The concrete database implementation
			 // (e.g., sqlserver, postgresql).
			 // - "pa2.db" : The name of the database. In this project,
			 // we use a local database named "pa2.db". This can also
			 // be a remote database name.
			 conn = DriverManager.getConnection("jdbc:sqlite:pa2.db");

			 Statement stmt = conn.createStatement();
			 
			 // Initialize T table
			 stmt.executeUpdate("DROP TABLE IF EXISTS Flights;");
			 stmt.executeUpdate("CREATE TABLE Flights(Airline char(32), Origin char(32), Destination char(32), Stops INT);");
			 stmt.executeUpdate("INSERT INTO Flights(Airline, Origin, Destination) " +
			 		 			"SELECT Airline, Origin, Destination FROM Flight;");			 
			 stmt.executeUpdate("INSERT INTO Flights SELECT *, 0 FROM Flight;");
			 stmt.executeUpdate("DELETE FROM Flights WHERE Stops IS NULL;");			 
			
			 // Initialize Delta table
			 stmt.executeUpdate("DROP TABLE IF EXISTS Delta;");
			 stmt.executeUpdate("CREATE TABLE Delta(Airline, Origin, Destination);");
			 stmt.executeUpdate("INSERT INTO Delta(Airline, Origin, Destination) " + 
					 			"SELECT Airline, Origin, Destination FROM Flight;");
			 
			 // Initialize T_old table
			 stmt.executeUpdate("DROP TABLE IF EXISTS oldFlights;");
			 stmt.executeUpdate("CREATE TABLE oldFlights(Airline, Origin, Destination, Stops);");

			 // Get the initial size of Delta
			 ResultSet rset = stmt.executeQuery("SELECT COUNT(*) FROM Delta;");
			 
			 // Counter for number of stops from every Origin to Destination
			 int numStops = 1;
			 while (rset.getInt(1) > 0) {
				 // T_old = T
				 stmt.executeUpdate("DELETE FROM oldFlights;");
				 stmt.executeUpdate("INSERT INTO oldFlights SELECT * FROM Flights;");
				 
				 // T = (SELECT * FROM T)
				 //     UNION
				 //     (SELECT x.A, y.B FROM G x, Delta y
				 //     WHERE x.B = y.A)
				 stmt.executeUpdate("INSERT INTO Flights " +
						 			"SELECT * FROM Flights " +
						 			"UNION " +
						 			"SELECT d.Airline, d.Origin, f.Destination, " + numStops + " " +
						 			"FROM Flight f, Delta d " + 
						 			"WHERE f.Airline = d.Airline AND f.Origin = d.Destination AND f.Destination <> d.Origin;");
				 
				 // Delta = T - T_old
				 stmt.executeUpdate("DELETE FROM Delta;");
				 stmt.executeUpdate("INSERT INTO Delta " + 
						 			"SELECT Airline, Origin, Destination FROM Flights " +
						 			"EXCEPT SELECT Airline, Origin, Destination FROM oldFlights;");
				 
				 // Check the size of delta table
				 rset = stmt.executeQuery("SELECT COUNT(*) FROM Delta;");
				 numStops++;
			 }
			 
			 // Create Connected Table as output
			 stmt.executeUpdate("DROP TABLE IF EXISTS Connected;");
			 stmt.executeUpdate("CREATE TABLE Connected(Airline char(32), Origin char(32), Destination char(32), Stops INT);");
			 stmt.executeUpdate("INSERT INTO Connected " + 
					 			"SELECT Airline, Origin, Destination, MIN(Stops) FROM Flights " +
					 			"GROUP BY Airline, Origin, Destination;");
			 
			 System.out.println("Program executed successfully.");
			 
			 // Remove helper tables from database
			 stmt.executeUpdate("DROP TABLE IF EXISTS Delta;");
			 stmt.executeUpdate("DROP TABLE IF EXISTS Flights;");
			 stmt.executeUpdate("DROP TABLE IF EXISTS oldFlights;");
			 
			 // Close the ResultSet and Statement objects.
			 rset.close();
			 stmt.close();
		 } catch (Exception e) {
			 throw new RuntimeException("There was a runtime problem!", e);
		 } finally {
			 try {
				 if (conn != null) conn.close();
			 } catch (SQLException e) {
				 throw new RuntimeException("Cannot close the connection!", e);
			 }
		 }
	}
}
