package com.oracle.tutorial.jdbc;



import java.sql.Connection;

import java.sql.ResultSet;

import java.sql.SQLException;

import java.sql.Statement;



public class MyQueries2 {

  

  Connection con;

  JDBCUtilities settings;  

  

  public MyQueries2(Connection connArg, JDBCUtilities settingsArg) {

    this.con = connArg;

    this.settings = settingsArg;

  }



  public static void getMyData(Connection con) throws SQLException {



    Statement stmt = null;



    String query =

      "SELECT c.nome_cliente, SUM(d.saldo_deposito) AS soma_de_depositos " +

      "FROM cliente c " +

      "INNER JOIN deposito d ON c.nome_cliente = d.nome_cliente " +

      "LEFT JOIN emprestimo e ON c.nome_cliente = e.nome_cliente " +

      "WHERE e.numero_emprestimo IS NULL " +

      "GROUP BY c.nome_cliente;";



    try {

      stmt = con.createStatement();

      ResultSet rs = stmt.executeQuery(query);



      System.out.println("Clientes com depósitos e suas somas de depósitos:");



      while (rs.next()) {

        String clienteNome = rs.getString("nome_cliente");

        double somaDepositos = rs.getDouble("soma_de_depositos");



        System.out.println("Cliente: " + clienteNome + " - Soma de Depósitos: " + somaDepositos);

      }



    } catch (SQLException e) {

      JDBCUtilities.printSQLException(e);

    } finally {

      if (stmt != null) {

        stmt.close();

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



 	MyQueries2.getMyData(myConnection);



    } catch (SQLException e) {

      JDBCUtilities.printSQLException(e);

    } finally {

      JDBCUtilities.closeConnection(myConnection);

    }



  }

}