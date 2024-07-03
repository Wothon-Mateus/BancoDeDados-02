-- Script para criar tabela SUPPLIERS
CREATE TABLE SUPPLIERS (
    ID SERIAL PRIMARY KEY,
    SUP_ID INTEGER UNIQUE,
    SUP_NAME VARCHAR(100),
    STREET VARCHAR(100),
    CITY VARCHAR(100)
);

-- Script para criar tabela COFFEES
CREATE TABLE COFFEES (
    ID SERIAL PRIMARY KEY,
    COF_NAME VARCHAR(100),
    SUP_ID INTEGER,
    PRICE DOUBLE PRECISION,
    SALES INT,
    TOTAL DOUBLE PRECISION,
    FOREIGN KEY (SUP_ID) REFERENCES SUPPLIERS(SUP_ID)
);

-- Script para inserir dados na tabela SUPPLIERS
INSERT INTO SUPPLIERS (SUP_ID, SUP_NAME, STREET, CITY) VALUES
(49, 'Superior Coffee', '1 Party Place', 'Mendocino'),
(101, 'Acme, Inc.', '99 Maraket Street', 'Groundsville'),
(150, 'The High Ground', '100 Coffee Lane', 'Meadows'),
(456, 'Restaurant Supplies, Inc', '200 Magnolia Street', 'Meadows'),
(927, 'Professional Kitchen', '300 Daisy Avenue', 'Groundsville');

-- Script para inserir dados na tabela COFFEES
INSERT INTO COFFEES (COF_NAME, SUP_ID, PRICE, SALES, TOTAL) VALUES
('Colombian', 101, 7.99, 0, 0),
('French_Roast', 49, 8.99, 0, 0),
('Espresso', 150, 9.99, 0, 0),
('Colombian_Decaf', 101, 8.99, 0, 0),
('French_Roast_Decaf', 49, 9.99, 0, 0);
