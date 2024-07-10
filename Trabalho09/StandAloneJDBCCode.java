import java.util.Properties; //Objeto genérico que armazena propriedades com usuário e senha
import java.sql.DriverManager; //Objeto que criará a conexão do sistema de banco de dados
import java.sql.Connection;//Objeto que armazenará o objeto de conexão ao banco de dados
import java.sql.Statement; //Objeto para disparar um comando para o SGBD
import java.sql.ResultSet;//Objeto que armazenará as tuplas resultantes de um comando SQL
import java.sql.SQLException; //Objeto para capturar eventos de erro no acesso ao banco de dados


public class StandAloneJDBCCode {

    public static Connection getConnection() {
        Connection con = null;
        String currentUrlString = null;
        Properties connectionProps = new Properties();
        
        connectionProps.put("user", "postgres");

        connectionProps.put("password", "postgres");
        currentUrlString = "jdbc:postgresql://localhost:5432/IB2";
        //atenção para os símbolos de barra "/" na linha acima, não confunda com a letra l
        try {
            con = DriverManager.getConnection(currentUrlString, connectionProps);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return con;
    }

    public static void myquery(Connection con) throws SQLException {
        Statement stmt = null;
        String query = "select * from deposito"; 
        try {
            stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            System.out.println("Table list: ");
            while (rs.next()) {
                String nomeCliente = rs.getString("nome_cliente");
                int numeroDeposito = rs.getInt("numero_deposito");
                int numeroConta = rs.getInt("numero_conta");
                String nomeAgencia = rs.getString("nome_agencia");
                double saldoDeposito = rs.getDouble("saldo_deposito");

                String formattedSaldo = String.format("%.2f", saldoDeposito); 

                System.out.println("Nome do Cliente: " + nomeCliente);
                System.out.println("Número do Depósito: " + numeroDeposito);
                System.out.println("Número da Conta: " + numeroConta);
                System.out.println("Nome da Agência: " + nomeAgencia);
                System.out.println("Saldo do Depósito: " + formattedSaldo);
                System.out.println("-----------------------");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    public static void closeConnection(Connection con) {
        try {
            if (con != null) {
                con.close();
            }
            System.out.println("Released all database resources.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("No arguments.");
        }

        Connection myConnection = null;
        try {
            myConnection = StandAloneJDBCCode.getConnection();
            StandAloneJDBCCode.myquery(myConnection);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (myConnection != null) {
                StandAloneJDBCCode.closeConnection(myConnection);
            }
        }
    }
}
