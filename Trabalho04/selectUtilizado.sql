-- Tabela de Cafes

SELECT * FROM COFFEES

-- Tabela de Fornecedores

SELECT * FROM SUPPLIERS

-- Fornecedores de café acompanhados da quantidade de tipos de cafés vendidos para a loja

SELECT s.SUP_NAME AS Nome_Fornecedor, COUNT(c.COF_NAME) AS Quantidade_Tipos_Cafe
FROM SUPPLIERS s
JOIN COFFEES c ON s.SUP_ID = c.SUP_ID
GROUP BY s.SUP_NAME;
