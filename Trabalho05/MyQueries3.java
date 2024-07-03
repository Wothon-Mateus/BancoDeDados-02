package com.oracle.tutorial.jdbc;



import java.sql.Connection;

import java.sql.ResultSet;

import java.sql.SQLException;

import java.sql.Statement;



public class MyQueries3 {

  

  Connection con;

  JDBCUtilities settings;  

  

  public MyQueries3(Connection connArg, JDBCUtilities settingsArg) {

    this.con = connArg;

    this.settings = settingsArg;

  }



  public static void getMyData(Connection con) throws SQLException {

    Statement stmt = null;

    String query =

        "WITH SomaDepositos AS (SELECT c.nome_cliente,COALESCE(SUM(d.saldo_deposito), 0) AS soma_de_depositos FROM cliente c LEFT JOIN deposito d ON c.nome_cliente = d.nome_cliente    GROUP BY c.nome_cliente),SomaEmprestimos AS (SELECT c.nome_cliente,        COALESCE(SUM(e.valor_emprestimo), 0) AS soma_de_emprestimos FROM cliente c LEFT JOIN        emprestimo e ON c.nome_cliente = e.nome_cliente GROUP BY c.nome_cliente)SELECT    d.nome_cliente, d.soma_de_depositos, e.soma_de_emprestimos FROM SomaDepositos d JOIN    SomaEmprestimos e ON d.nome_cliente = e.nome_cliente WHERE d.soma_de_depositos > 0    AND e.soma_de_emprestimos > 0;";



    try {

        stmt = con.createStatement();

        ResultSet rs = stmt.executeQuery(query);



        System.out.println("Clientes com depósitos e empréstimos e suas somas:");



        while (rs.next()) {

            String clienteNome = rs.getString("nome_cliente");

            double somaDepositos = rs.getDouble("soma_de_depositos");

            double somaEmprestimos = rs.getDouble("soma_de_emprestimos");



            System.out.println("Cliente: " + clienteNome + " - Soma de Depósitos: " + somaDepositos + " - Soma de Empréstimos: " + somaEmprestimos);

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



 	MyQueries3.getMyData(myConnection);



    } catch (SQLException e) {

      JDBCUtilities.printSQLException(e);

    } finally {

      JDBCUtilities.closeConnection(myConnection);

    }



  }

}