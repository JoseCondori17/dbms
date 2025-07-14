-- ===============================================================================
-- SCRIPT DE PRUEBA - SOLO CONSULTAS QUE FUNCIONAN CORRECTAMENTE
-- ===============================================================================
-- Este script contiene únicamente las consultas que funcionan sin errores
-- Basado en las pruebas realizadas y los problemas identificados

-- ==============================================================================
-- PREPARACIÓN: Crear base de datos y esquema
-- ==============================================================================
CREATE DATABASE testworkingindices;
CREATE SCHEMA testworkingindices.public;

-- ==============================================================================
-- PRUEBA 1: ÍNDICE B+TREE (Consultas que funcionan)
-- ==============================================================================
CREATE TABLE testworkingindices.public.btree_table (
    id INT PRIMARY KEY,
    score DOUBLE,
    name VARCHAR(50),
    age INT
);

-- Insertar datos de prueba
INSERT INTO testworkingindices.public.btree_table (id, score, name, age) VALUES
(1, 85.5, 'Alice', 25),
(2, 92.0, 'Bob', 30),
(3, 78.3, 'Charlie', 28),
(4, 96.7, 'Diana', 32),
(5, 88.9, 'Eve', 27),
(6, 91.2, 'Frank', 29),
(7, 83.4, 'Grace', 26),
(8, 94.1, 'Henry', 31);

-- Crear índice B+Tree en score (por defecto)
CREATE INDEX idx_btree_score ON testworkingindices.public.btree_table (score);

-- ✅ Consultas B+Tree que funcionan correctamente:
SELECT * FROM testworkingindices.public.btree_table WHERE score = 85.5;      -- ✅ Búsqueda exacta
SELECT * FROM testworkingindices.public.btree_table WHERE score > 90.0;      -- ✅ Rangos (GT) - CORREGIDO

-- ==============================================================================
-- PRUEBA 2: ÍNDICE HASH (Extendible Hashing) - FUNCIONA PERFECTAMENTE
-- ==============================================================================
CREATE TABLE testworkingindices.public.hash_table (
    id INT PRIMARY KEY,
    product_code VARCHAR(20),
    price DOUBLE,
    category VARCHAR(30)
);

-- Insertar datos de prueba
INSERT INTO testworkingindices.public.hash_table (id, product_code, price, category) VALUES
(1, 'A001', 150.0, 'Electronics'),
(2, 'B002', 250.0, 'Clothing'),
(3, 'C003', 75.5, 'Books'),
(4, 'D004', 320.0, 'Electronics'),
(5, 'E005', 45.0, 'Books'),
(6, 'F006', 180.0, 'Clothing'),
(7, 'G007', 95.0, 'Electronics'),
(8, 'H008', 220.0, 'Clothing');

-- Crear índice Hash en product_code (parser mejorado)
CREATE INDEX idx_hash_code ON testworkingindices.public.hash_table (product_code) USING hash(product_code);

-- ✅ Consultas Hash que funcionan correctamente:
SELECT * FROM testworkingindices.public.hash_table WHERE product_code = 'A001';  -- ✅ Hash optimizado
SELECT * FROM testworkingindices.public.hash_table WHERE product_code = 'D004';  -- ✅ Hash optimizado
SELECT * FROM testworkingindices.public.hash_table WHERE product_code = 'H008';  -- ✅ Hash optimizado

-- ==============================================================================
-- PRUEBA 3: ÍNDICE SEQUENTIAL FILE - FUNCIONA PERFECTAMENTE
-- ==============================================================================
CREATE TABLE testworkingindices.public.seq_table (
    id INT PRIMARY KEY,
    employee_id INT,
    salary DOUBLE,
    department VARCHAR(20)
);

-- Insertar datos de prueba (ordenados para Sequential File)
INSERT INTO testworkingindices.public.seq_table (id, employee_id, salary, department) VALUES
(1, 101, 42000.0, 'HR'),
(2, 102, 45000.0, 'HR'),
(3, 103, 48000.0, 'Finance'),
(4, 104, 52000.0, 'IT'),
(5, 105, 55000.0, 'HR'),
(6, 106, 58000.0, 'Finance'),
(7, 107, 65000.0, 'IT'),
(8, 108, 71000.0, 'IT');

-- Crear índice Sequential File en salary (parser mejorado)
CREATE INDEX idx_seq_salary ON testworkingindices.public.seq_table (salary) USING sequential(salary);

-- ✅ Consultas Sequential File que funcionan correctamente:
SELECT * FROM testworkingindices.public.seq_table WHERE salary = 52000.0;           -- ✅ Búsqueda exacta
SELECT * FROM testworkingindices.public.seq_table WHERE salary > 50000.0;          -- ✅ Rangos
SELECT * FROM testworkingindices.public.seq_table WHERE salary < 50000.0;          -- ✅ Rangos
SELECT * FROM testworkingindices.public.seq_table WHERE salary >= 55000.0;         -- ✅ Rangos
SELECT * FROM testworkingindices.public.seq_table WHERE salary <= 50000.0;         -- ✅ Rangos
SELECT * FROM testworkingindices.public.seq_table WHERE salary BETWEEN 45000.0 AND 60000.0; -- ✅ BETWEEN

-- ==============================================================================
-- PRUEBA 4: ÍNDICE R-TREE (Consultas que funcionan)
-- ==============================================================================
CREATE TABLE testworkingindices.public.rtree_table (
    id INT PRIMARY KEY,
    location_name VARCHAR(50),
    x_coord DOUBLE,
    y_coord DOUBLE,
    area DOUBLE
);

-- Insertar datos de prueba (coordenadas espaciales)
INSERT INTO testworkingindices.public.rtree_table (id, location_name, x_coord, y_coord, area) VALUES
(1, 'Park A', 10.5, 20.3, 100.0),
(2, 'Store B', 15.2, 25.7, 50.0),
(3, 'School C', 8.9, 18.1, 200.0),
(4, 'Hospital D', 22.4, 30.6, 150.0),
(5, 'Library E', 12.1, 22.8, 75.0),
(6, 'Mall F', 18.7, 28.9, 300.0),
(7, 'Office G', 14.3, 24.5, 120.0),
(8, 'Bank H', 16.8, 26.2, 80.0);

-- Crear índice R-Tree en x_coord (parser mejorado)
CREATE INDEX idx_rtree_x ON testworkingindices.public.rtree_table (x_coord) USING rtree(x_coord);

-- ✅ Consultas R-Tree que funcionan correctamente:
SELECT * FROM testworkingindices.public.rtree_table WHERE x_coord = 10.5;          -- ✅ Búsqueda exacta - CORREGIDO
SELECT * FROM testworkingindices.public.rtree_table WHERE x_coord > 15.0;          -- ✅ Rangos - CORREGIDO

-- ==============================================================================
-- PRUEBA 5: ÍNDICE ISAM (Funcionando con búsquedas exactas)
-- ==============================================================================
CREATE TABLE testworkingindices.public.isam_table (
    id INT PRIMARY KEY,
    product_id INT,
    price DOUBLE,
    stock INT
);

-- Insertar datos de prueba ANTES de crear el índice
INSERT INTO testworkingindices.public.isam_table (id, product_id, price, stock) VALUES
(1, 1001, 25.99, 100),
(2, 1002, 45.50, 75),
(3, 1003, 12.75, 200),
(4, 1004, 67.20, 50),
(5, 1005, 33.40, 125),
(6, 1006, 89.99, 30),
(7, 1007, 19.95, 150),
(8, 1008, 56.80, 80);

-- Crear índice ISAM en price (parser mejorado + debugging mejorado)
CREATE INDEX idx_isam_price ON testworkingindices.public.isam_table (price) USING isam(price);

-- ✅ Consultas ISAM que funcionan correctamente:
SELECT * FROM testworkingindices.public.isam_table WHERE price = 25.99;            -- ✅ Búsqueda exacta - CORREGIDO
SELECT * FROM testworkingindices.public.isam_table WHERE price = 45.50;            -- ✅ Búsqueda exacta - CORREGIDO
SELECT * FROM testworkingindices.public.isam_table WHERE price = 67.20;            -- ✅ Búsqueda exacta - CORREGIDO

-- ==============================================================================
-- PRUEBA 6: COMPARACIÓN DE RENDIMIENTO (Consultas que funcionan)
-- ==============================================================================
CREATE TABLE testworkingindices.public.performance_test (
    id INT PRIMARY KEY,
    test_value DOUBLE,
    description VARCHAR(100)
);

-- Insertar más datos para prueba de rendimiento
INSERT INTO testworkingindices.public.performance_test (id, test_value, description) VALUES
(1, 100.1, 'Test record 1'),
(2, 200.2, 'Test record 2'),
(3, 300.3, 'Test record 3'),
(4, 400.4, 'Test record 4'),
(5, 500.5, 'Test record 5'),
(6, 600.6, 'Test record 6'),
(7, 700.7, 'Test record 7'),
(8, 800.8, 'Test record 8'),
(9, 900.9, 'Test record 9'),
(10, 1000.0, 'Test record 10');

-- Crear diferentes tipos de índices en la misma columna (todos corregidos)
CREATE INDEX idx_perf_btree ON testworkingindices.public.performance_test (test_value);
CREATE INDEX idx_perf_hash ON testworkingindices.public.performance_test (test_value) USING hash(test_value);
CREATE INDEX idx_perf_seq ON testworkingindices.public.performance_test (test_value) USING sequential(test_value);

-- ✅ Consultas de rendimiento que funcionan correctamente:
SELECT * FROM testworkingindices.public.performance_test WHERE test_value = 500.5;  -- ✅ Usará el índice más apropiado
SELECT * FROM testworkingindices.public.performance_test WHERE test_value > 600.0;  -- ✅ Usará B+Tree para rangos

-- ==============================================================================
-- RESUMEN DE ESTADO ACTUAL
-- ==============================================================================

/*
✅ FUNCIONANDO CORRECTAMENTE:
- Hash Index: Búsquedas exactas (=) ✅
- Sequential File: Búsquedas exactas (=) y rangos (>, <, >=, <=, BETWEEN) ✅
- B+Tree: Búsquedas exactas (=) y rangos GT (>) ✅
- R-Tree: Búsquedas exactas (=) y rangos GT (>) ✅
- ISAM: Búsquedas exactas (=) ✅

⚠️ PROBLEMAS IDENTIFICADOS:
- B+Tree: Rangos LT (<), GTE (>=), LTE (<=), BETWEEN → Error header B+Tree
- R-Tree: Rangos LT (<), GTE (>=), LTE (<=), BETWEEN → Error "Undefined: Semicolon"
- Parser: Problemas con SQLglot en ciertas consultas de rango

🔧 CORRECCIONES IMPLEMENTADAS:
- Parser mejorado con preprocesamiento USING
- Métodos call_btree_range() y call_rtree_range() agregados
- Método call_rtree_point() para búsquedas exactas R-Tree
- Debugging mejorado para ISAM
- Fallback a table scan para casos problemáticos
*/
