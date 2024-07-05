package com.oracle.tutorial.jdbc;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;



public class MyQueries {
  
  Connection con;
  JDBCUtilities settings;  
  
  public MyQueries(Connection connArg, JDBCUtilities settingsArg) {
    this.con = connArg;
    this.settings = settingsArg;
  }

  public static void getMyData(Connection con) throws SQLException {
    Statement stmt = null;
    String query =
      "SELECT SUPPLIERS.SUP_NAME, COUNT(COFFEES.COF_NAME) AS Quantidade_Tipos_Cafe " +
      "FROM SUPPLIERS " +
      "LEFT JOIN COFFEES ON SUPPLIERS.SUP_ID = COFFEES.SUP_ID " +
      "GROUP BY SUPPLIERS.SUP_NAME";

    try {
      stmt = con.createStatement();
      ResultSet rs = stmt.executeQuery(query);
      System.out.println("Fornecedores de café e quantidade de tipos de café vendidos: ");
      while (rs.next()) {
        String supplierName = rs.getString("SUP_NAME");
        int coffeeCount = rs.getInt("Quantidade_Tipos_Cafe");
        System.out.println("Fornecedor: " + supplierName + ", Quantidade de Tipos de Café: " + coffeeCount);
      }
    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } finally {
      if (stmt != null) { stmt.close(); }
    }
}

public static void populateTable(Connection con) throws SQLException, IOException {
        Statement stmt = null;
        BufferedReader reader = null;

        try {
            stmt = con.createStatement();
            System.out.println("Executando DDL/DML:");
            
            // Truncar a tabela antes de inserir novos dados
            stmt.executeUpdate("TRUNCATE TABLE debito");

            reader = new BufferedReader(new FileReader("/home/railson/Downloads/debito-populate-table.txt"));
            String line;
            String create = ""; // String para armazenar os comandos de inserção

            while ((line = reader.readLine()) != null) {
                String[] values = line.split("\t");
                int numero_debito = Integer.parseInt(values[0]);
                double valor_debito = Double.parseDouble(values[1]);
                int motivo_debito = Integer.parseInt(values[2]);
                String data_debito = values[3];
                int numero_conta = Integer.parseInt(values[4]);
                String nome_agencia = values[5];
                String nome_cliente = values[6];

                String insertQuery = "INSERT INTO debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) " +
                        "VALUES (" + numero_debito + ", " + valor_debito + ", " + motivo_debito + ", '" + data_debito + "', " +
                        numero_conta + ", '" + nome_agencia + "', '" + nome_cliente + "')";

                // Concatenando os comandos de inserção
                create += insertQuery + ";\n";
            }
            
            // Executando os comandos de inserção
            stmt.executeUpdate(create);

        } catch (SQLException e) {
            JDBCUtilities.printSQLException(e);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (reader != null) {
                reader.close();
            }
        }
    }



  public static void main(String[] args) {
    JDBCUtilities myJDBCUtilities;
    Connection myConnection = null;
    if (args[0] == null) {
      System.err.println("Properties file not specified at command line");
      return;
    } else {
      try {
        myJDBCUtilities = new JDBCUtilities(args[0]);
      } catch (Exception e) {
        System.err.println("Problem reading properties file " + args[0]);
        e.printStackTrace();
        return;
      }
    }

    try {
      myConnection = myJDBCUtilities.getConnection();

      // Chamada comentada
      // MyQueries.getMyData(myConnection);
      
      // Chamada ao método populateTable
      MyQueries.populateTable(myConnection);

    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      JDBCUtilities.closeConnection(myConnection);
    }

  }
}

