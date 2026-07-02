CREATE DATABASE IF NOT EXISTS db;

USE db;

-- El driver JDBC 5.1.45 no soporta caching_sha2_password (plugin de autenticación por defecto en MySQL 8)
ALTER USER 'user'@'%' IDENTIFIED WITH mysql_native_password BY '1234';
GRANT ALL PRIVILEGES ON *.* TO 'user'@'%';
