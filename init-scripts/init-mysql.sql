-- ===========================
CREATE DATABASE IF NOT EXISTS etl_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE etl_db;

-- Tạo bảng để lưu data
CREATE TABLE etl_db.users (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(100),
  email VARCHAR(100),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Chèn dữ liệu mẫu
INSERT INTO etl_db.users (name, email) VALUES
  ('Nguyen Van A', 'a@example.com'),
  ('Tran Thi B', 'b@example.com');


-- ===========================
-- 3. Tạo user Debezium và cấp quyền replication
-- ===========================
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'dbzpwd';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
  ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;