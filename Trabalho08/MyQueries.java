package com.oracle.tutorial.jdbc;

import java.sql.DatabaseMetaData;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.Scanner;
import java.sql.Date;
import java.sql.PreparedStatement;

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

public static void getMyData3(Connection con) throws SQLException {
    Statement stmt = null;
    String query = "SELECT " +
                       "c.nome_cliente, " +
                       "COALESCE(SUM(d.saldo_deposito), 0) AS soma_depositos, " +
                       "COALESCE(SUM(e.valor_emprestimo), 0) AS soma_emprestimos " +
                   "FROM " +
                       "cliente c " +
                   "LEFT JOIN " +
                       "deposito d ON c.nome_cliente = d.nome_cliente " +
                   "LEFT JOIN " +
                       "emprestimo e ON c.nome_cliente = e.nome_cliente " +
                   "GROUP BY " +
                       "c.nome_cliente, d.nome_agencia, d.numero_conta";

    try {
        stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        System.out.println("Dados dos Clientes:");
        while (rs.next()) {
        
            // Usando índices numéricos
            String nomeCliente = rs.getString(1);
            double somaDepositos = rs.getDouble(2);
            double somaEmprestimos = rs.getDouble(3);
            
            System.out.println("\n\nUsando índices numéricos\n\n");
            System.out.println("Nome do Cliente: " + nomeCliente);
            System.out.println("Soma dos Depósitos: " + somaDepositos);
            System.out.println("Soma dos Empréstimos: " + somaEmprestimos);

            // Usando alias
            nomeCliente = rs.getString("nome_cliente");
            somaDepositos = rs.getDouble("soma_depositos");
            somaEmprestimos = rs.getDouble("soma_emprestimos");

	    System.out.println("\n\nUsando alias\n\n");
	    System.out.println("Nome do Cliente: " + nomeCliente);
            System.out.println("Soma dos Depósitos: " + somaDepositos);
            System.out.println("Soma dos Empréstimos: " + somaEmprestimos);

            // Usando nomes dos campos das tabelas alvo
	    nomeCliente = rs.getString("nome_cliente");
            somaDepositos = rs.getDouble("soma_depositos");
            somaEmprestimos = rs.getDouble("soma_emprestimos");
            
            
            System.out.println("\n\nUsando nomes dos campos das tabelas alvo\n\n");
	    System.out.println("Nome do Cliente: " + nomeCliente);
            System.out.println("Soma dos Depósitos: " + somaDepositos);
            System.out.println("Soma dos Empréstimos: " + somaEmprestimos);
        }
    } catch (SQLException e) {
        JDBCUtilities.printSQLException(e);
    } finally {
        if (stmt != null) {
            stmt.close();
        }
    }
}

    public static void cursorHoldabilitySupport(Connection conn) throws SQLException {
        DatabaseMetaData dbMetaData = conn.getMetaData();

        System.out.println("ResultSet.HOLD_CURSORS_OVER_COMMIT = " + ResultSet.HOLD_CURSORS_OVER_COMMIT);
        System.out.println("ResultSet.CLOSE_CURSORS_AT_COMMIT = " + ResultSet.CLOSE_CURSORS_AT_COMMIT);
        System.out.println("Default cursor holdability: " + dbMetaData.getResultSetHoldability());
        System.out.println("Supports HOLD_CURSORS_OVER_COMMIT? " + dbMetaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
        System.out.println("Supports CLOSE_CURSORS_AT_COMMIT? " + dbMetaData.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
    }
    
    public static void verificarSuporteConcorrenciaResultSet(Connection conn) throws SQLException {
        DatabaseMetaData metaDataBanco = conn.getMetaData();

        // Tipos de ResultSet
        int[] tiposResultSet = { 
            ResultSet.TYPE_FORWARD_ONLY, 
            ResultSet.TYPE_SCROLL_INSENSITIVE, 
            ResultSet.TYPE_SCROLL_SENSITIVE 
        };

        // Tipos de concorrência
        int[] concorrenciasResultSet = {
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CONCUR_UPDATABLE
        };

        // Verificar suporte para cada combinação de tipo de ResultSet e concorrência
        for (int tipoResultSet : tiposResultSet) {
            for (int concorrenciaResultSet : concorrenciasResultSet) {
                boolean suporte = metaDataBanco.supportsResultSetConcurrency(tipoResultSet, concorrenciaResultSet);
                String tipoResultSetStr;
                String concorrenciaResultSetStr;

                switch (tipoResultSet) {
                    case ResultSet.TYPE_FORWARD_ONLY:
                        tipoResultSetStr = "ResultSet.TYPE_FORWARD_ONLY";
                        break;
                    case ResultSet.TYPE_SCROLL_INSENSITIVE:
                        tipoResultSetStr = "ResultSet.TYPE_SCROLL_INSENSITIVE";
                        break;
                    case ResultSet.TYPE_SCROLL_SENSITIVE:
                        tipoResultSetStr = "ResultSet.TYPE_SCROLL_SENSITIVE";
                        break;
                    default:
                        tipoResultSetStr = "Desconhecido";
                        break;
                }

                switch (concorrenciaResultSet) {
                    case ResultSet.CONCUR_READ_ONLY:
                        concorrenciaResultSetStr = "ResultSet.CONCUR_READ_ONLY";
                        break;
                    case ResultSet.CONCUR_UPDATABLE:
                        concorrenciaResultSetStr = "ResultSet.CONCUR_UPDATABLE";
                        break;
                    default:
                        concorrenciaResultSetStr = "Desconhecido";
                        break;
                }

                System.out.println("Suporta " + tipoResultSetStr + " com " + concorrenciaResultSetStr + "? " + suporte);
            }
        }
    }
    


public static void adicionarJurosDepositos(Connection conn) throws SQLException {
    Statement stmt = null;
    Scanner in = new Scanner(System.in);
    
    try {
        System.out.println("Entre com a porcentagem de juros que deseja aplicar (5% = entre com 1,05): ");
        double porcentagem = in.nextDouble();

        stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM deposito");

        while (rs.next()) {
            double saldo = rs.getDouble("saldo_deposito");
            double saldoNovo = saldo * porcentagem; 
            rs.updateDouble("saldo_deposito", saldoNovo);
            rs.updateRow(); 
        }
        System.out.println("\n");
	System.out.println("Realizado com sucesso");
	System.out.println("\n");
        in.close();
    } catch (SQLException e) {
        JDBCTutorialUtilities.printSQLException(e);
    } finally {
        if (stmt != null) {
            stmt.close();
        }
        
    }
}

public static void populateTable(Connection con) throws SQLException, IOException {
    Statement stmt = null;
    BufferedReader reader = null;

    try {
        stmt = con.createStatement();
        con.setAutoCommit(false); // desativa commit 

        stmt.executeUpdate("TRUNCATE TABLE debito");

        reader = new BufferedReader(new FileReader("/home/railson/Downloads/debito-populate-table.txt"));
        String line;
        String create = ""; 

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
            create += insertQuery + ";\n";


        }
        
            stmt.executeUpdate(create);
            con.commit(); // confirma 

    } catch (SQLException e) {
        con.rollback(); // reverte em caso de erro
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
        con.setAutoCommit(true); // ativa denovo 
    }
}

public static void insertRow(Connection con, int numero_debito, double valor_debito, int motivo_debito, String data_debito, int numero_conta, String nome_agencia, String nome_cliente)
throws SQLException {
    Statement stmt = null;
    try {
        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet uprs = stmt.executeQuery("SELECT * FROM debito");
        uprs.moveToInsertRow(); // posicionar no ponto de inserção da tabela
        uprs.updateInt("numero_debito", numero_debito);
        uprs.updateDouble("valor_debito", valor_debito);
        uprs.updateInt("motivo_debito", motivo_debito);
        uprs.updateDate("data_debito", Date.valueOf(data_debito));
        uprs.updateInt("numero_conta", numero_conta);
        uprs.updateString("nome_agencia", nome_agencia);
        uprs.updateString("nome_cliente", nome_cliente);
        uprs.insertRow(); // inserir a linha na tabela
        uprs.beforeFirst(); // posicionar-se novamente na posição anterior ao primeiro registro
    } catch (SQLException e) {
        JDBCTutorialUtilities.printSQLException(e);
    } finally {
        if (stmt != null) {
            stmt.close();
        }
    }
}

public static void insertMyData1(Connection con) throws SQLException {
    Statement stmt = null;
    String query = null;
    query = "insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) " +
            "values (3000,3000,5,'2014-02-06',36593,'UFU','Pedro Alvares Sousa');";
    try {
        long startTime = System.currentTimeMillis();

        stmt = con.createStatement();
        stmt.executeUpdate(query);

        long endTime = System.currentTimeMillis();
        System.out.println("Débito inserido pela classe insertMyData1 no banco em " + (endTime - startTime) + " milissegundos");

    } catch (SQLException e) {
        JDBCUtilities.printSQLException(e);
    } finally {
        if (stmt != null) {
            stmt.close();
        }
    }
}

public static void insertMyData2(Connection con) throws SQLException {
    PreparedStatement stmt = null;
    String query = null;
    query = "insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) " +
            "values (?, ?, ?, ?, ?, ?, ?);";
    try {
        long startTime = System.currentTimeMillis();

        stmt = con.prepareStatement(query);
        stmt.setInt(1, 3001);
        stmt.setDouble(2, 3001);
        stmt.setInt(3, 4);
        stmt.setDate(4, Date.valueOf("2014-02-06"));
        stmt.setInt(5, 36593);
        stmt.setString(6, "UFU");
        stmt.setString(7, "Pedro Alvares Sousa");
        stmt.executeUpdate();

        long endTime = System.currentTimeMillis();
        System.out.println("Débito inserido pela classe insertMyData2 no banco em " + (endTime - startTime) + " milissegundos");

    } catch (SQLException e) {
        JDBCUtilities.printSQLException(e);
    } finally {
        if (stmt != null) {
            stmt.close();
        }
    }
}

public static void insertMyData1000(Connection con) throws SQLException {
    Statement stmt = null;
    String query = null;
    try {
        long startTime = System.currentTimeMillis();
        System.out.println("Iniciando InsertMyData1.\n\n ");
        for (int numdeb = 3002; numdeb < 4002; numdeb++) {
            query = "insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) " +
                    "values (" + numdeb + "," + numdeb + ",5,'2014-02-06',36593,'UFU','Pedro Alvares Sousa');";

            stmt = con.createStatement();
            stmt.executeUpdate(query);

            if ((numdeb % 50) == 0) {
                long endTime = System.currentTimeMillis();
                System.out.println(numdeb - 3000 + "\t" + (endTime - startTime));
            }
        }
        System.out.println("\n\nInsertMyData1 concluido.");

    } catch (SQLException e) {
        JDBCUtilities.printSQLException(e);
    } finally {
        if (stmt != null) {
            stmt.close();
        }
    }
}

public static void insertMyData2000(Connection con) throws SQLException {
    PreparedStatement stmt = null;
    String query = null;
    try {
        long startTime = System.currentTimeMillis();

        System.out.println("Iniciando InsertMyData2.\n\n ");
        for (int numdeb = 5002; numdeb < 6002; numdeb++) {
            query = "insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) " +
                    "values (?, ?, 5, '2014-02-06', 36593, 'UFU', 'Pedro Alvares Sousa');";

            stmt = con.prepareStatement(query);
            stmt.setInt(1, numdeb);
            stmt.setDouble(2, numdeb);
            stmt.executeUpdate();

            if ((numdeb % 50) == 0) {
                long endTime = System.currentTimeMillis();
                System.out.println(numdeb - 5000 + "\t" + (endTime - startTime));
            }
        }
        System.out.println("\n\nInsertMyData2 concluido.");

    } catch (SQLException e) {
        JDBCUtilities.printSQLException(e);
    } finally {
        if (stmt != null) {
            stmt.close();
        }
    }
}

public static void insertMyData3000(Connection con) throws SQLException {
    PreparedStatement stmt = null;
    String query = null;
    try {
        con.setAutoCommit(false); 

        query = "insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) " +
                "values (?, ?, 5, '2014-02-06', 36593, 'UFU', 'Pedro Alvares Sousa');";
        stmt = con.prepareStatement(query);

        con.setAutoCommit(false); 
        int numdeb;
        long startTime = System.currentTimeMillis(); 

        for (numdeb = 5002; numdeb < 6002; numdeb++) {
            stmt.setInt(1, numdeb);
            stmt.setDouble(2, numdeb);
            stmt.addBatch();

            if ((numdeb % 50) == 0) {
                long endTime = System.currentTimeMillis(); 
                System.out.println(numdeb - 5000 + "\t" + (endTime - startTime)); 
            }
        }

        stmt.executeBatch(); 

        con.commit(); 
        long endTime = System.currentTimeMillis(); 
        System.out.println("\n\nCommit concluido em: " + (endTime - startTime)  + " milissegundos \n\n"); 

    } catch (SQLException e) {
        JDBCUtilities.printSQLException(e);
        if (con != null) {
            con.rollback(); 
        }
    } finally {
        if (stmt != null) {
            stmt.close();
        }
        con.setAutoCommit(true); 
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
        // MyQueries.insertMyData1(myConnection);
        // MyQueries.insertMyData2(myConnection);
        // MyQueries.insertMyData1000(myConnection);
        // MyQueries.insertMyData2000(myConnection);
        MyQueries.insertMyData3000(myConnection);


        // Chamada comentada
        // MyQueries.getMyData(myConnection);

        // Chamada ao método populateTable
        // MyQueries.populateTable(myConnection);
        /*
        MyQueries.getMyData3(myConnection);
        
        System.out.println("\n");
        System.out.println("-----------------------------------");
        System.out.println("\n");
        MyQueries.cursorHoldabilitySupport(myConnection);
	    System.out.println("\n");
        System.out.println("-----------------------------------");
        System.out.println("\n");
        MyQueries.verificarSuporteConcorrenciaResultSet(myConnection);
	    System.out.println("\n");
        System.out.println("-----------------------------------");
        System.out.println("\n");
        MyQueries.adicionarJurosDepositos(myConnection);

	    System.out.println("\n");
        System.out.println("-----------------------------------");
        System.out.println("\n");
      	try {
       	    MyQueries.populateTable(myConnection);
            System.out.println("Inserts Realizados");
	       } catch (IOException e) {
	           e.printStackTrace();
	       }
        System.out.println("\n");
        System.out.println("-----------------------------------");
        System.out.println("\n");
        insertRow(myConnection, 2000, 150, 1, "2014-01-23", 46248, "UFU", "Carla Soares Sousa");
        insertRow(myConnection, 2001, 200, 2, "2014-01-23", 26892, "Glória", "Carolina Soares Souza");
        insertRow(myConnection, 2002, 500, 3, "2014-01-23", 70044, "Cidade Jardim", "Eurides Alves da Silva");
        System.out.println("Linhas incluidas");
        System.out.println("\n");
        System.out.println("-----------------------------------");
        System.out.println("\n");
        */



    } catch (SQLException e) {
        JDBCUtilities.printSQLException(e);
    } finally {
        JDBCUtilities.closeConnection(myConnection);
    }
}

}


