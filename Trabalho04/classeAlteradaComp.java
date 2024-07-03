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