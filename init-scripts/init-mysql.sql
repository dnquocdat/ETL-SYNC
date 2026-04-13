-- ============================================================
-- ETL-SYNC: E-Commerce Order Sync Platform
-- MySQL Schema Initialization
-- ============================================================

CREATE DATABASE IF NOT EXISTS etl_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE etl_db;

-- ============================================================
-- 1. Products table
-- ============================================================
CREATE TABLE products (
  id          INT PRIMARY KEY AUTO_INCREMENT,
  name        VARCHAR(200)   NOT NULL,
  category    VARCHAR(100)   NOT NULL,
  price       DECIMAL(12,2)  NOT NULL,
  description TEXT,
  status      ENUM('active','inactive','discontinued') DEFAULT 'active',
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- ============================================================
-- 2. Inventory table
-- ============================================================
CREATE TABLE inventory (
  id          INT PRIMARY KEY AUTO_INCREMENT,
  product_id  INT NOT NULL,
  warehouse   VARCHAR(50)  NOT NULL DEFAULT 'main',
  quantity    INT NOT NULL DEFAULT 0,
  reserved    INT NOT NULL DEFAULT 0,
  updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (product_id) REFERENCES products(id),
  UNIQUE KEY uk_product_warehouse (product_id, warehouse)
);

-- ============================================================
-- 3. Orders table
-- ============================================================
CREATE TABLE orders (
  id              INT PRIMARY KEY AUTO_INCREMENT,
  customer_name   VARCHAR(200)   NOT NULL,
  customer_email  VARCHAR(200)   NOT NULL,
  status          ENUM('pending','confirmed','processing','shipped','delivered','cancelled') DEFAULT 'pending',
  total_amount    DECIMAL(12,2)  NOT NULL DEFAULT 0,
  shipping_address TEXT,
  created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- ============================================================
-- 4. Order Items table
-- ============================================================
CREATE TABLE order_items (
  id          INT PRIMARY KEY AUTO_INCREMENT,
  order_id    INT NOT NULL,
  product_id  INT NOT NULL,
  quantity    INT NOT NULL,
  unit_price  DECIMAL(12,2) NOT NULL,
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (order_id)   REFERENCES orders(id),
  FOREIGN KEY (product_id) REFERENCES products(id)
);

-- ============================================================
-- 5. Seed Data: Products
-- ============================================================
INSERT INTO products (name, category, price, description) VALUES
  ('iPhone 15 Pro Max',     'Electronics',  1199.99, 'Apple flagship smartphone with A17 Pro chip'),
  ('MacBook Air M3',        'Electronics',  1099.00, '13-inch laptop with M3 chip, 8GB RAM'),
  ('Samsung Galaxy S24',    'Electronics',   799.99, 'Samsung flagship with Snapdragon 8 Gen 3'),
  ('Sony WH-1000XM5',      'Electronics',   348.00, 'Premium noise-cancelling headphones'),
  ('iPad Air M2',           'Electronics',   599.00, '11-inch tablet with M2 chip'),
  ('Nike Air Max 90',       'Fashion',       129.99, 'Classic sneakers with Air Max cushioning'),
  ('Adidas Ultraboost',     'Fashion',       189.99, 'Premium running shoes with Boost midsole'),
  ('Levi 501 Jeans',        'Fashion',        69.99, 'Classic straight-fit denim jeans'),
  ('The North Face Jacket', 'Fashion',       249.99, 'Waterproof outdoor jacket'),
  ('Ray-Ban Aviator',       'Fashion',       161.00, 'Classic aviator sunglasses'),
  ('Dyson V15 Detect',      'Home',          749.99, 'Cordless vacuum with laser dust detection'),
  ('Instant Pot Duo',       'Home',           89.99, '7-in-1 electric pressure cooker'),
  ('Nespresso Vertuo',      'Home',          199.00, 'Coffee machine with centrifusion technology'),
  ('Kindle Paperwhite',     'Books',         139.99, '6.8" display, waterproof e-reader'),
  ('AirPods Pro 2',         'Electronics',   249.00, 'Active noise cancellation, USB-C'),
  ('Samsung 55" OLED TV',   'Electronics',  1299.99, '4K OLED Smart TV with Tizen OS'),
  ('Logitech MX Master 3S', 'Electronics',   99.99,  'Wireless ergonomic mouse'),
  ('Mechanical Keyboard',   'Electronics',   149.99, 'Hot-swappable Cherry MX switches'),
  ('Yoga Mat Premium',      'Sports',         45.99, 'Non-slip 6mm thick exercise mat'),
  ('Protein Powder 2kg',    'Sports',         54.99, 'Whey isolate protein, chocolate flavor');

-- ============================================================
-- 6. Seed Data: Inventory
-- ============================================================
INSERT INTO inventory (product_id, warehouse, quantity, reserved) VALUES
  (1,  'main', 150, 12), (1,  'north', 80,  5),
  (2,  'main', 200, 20), (3,  'main', 180, 15),
  (4,  'main', 300, 25), (5,  'main', 120,  8),
  (6,  'main', 500, 40), (7,  'main', 350, 30),
  (8,  'main', 600, 50), (9,  'main', 200, 10),
  (10, 'main', 250, 18), (11, 'main', 100,  5),
  (12, 'main', 400, 35), (13, 'main', 150, 12),
  (14, 'main', 800, 60), (15, 'main', 450, 40),
  (16, 'main',  80,  3), (17, 'main', 550, 45),
  (18, 'main', 300, 20), (19, 'main', 700, 55),
  (20, 'main', 900, 70);

-- ============================================================
-- 7. Seed Data: Sample Orders
-- ============================================================
INSERT INTO orders (customer_name, customer_email, status, total_amount, shipping_address) VALUES
  ('Nguyen Van A',  'nguyenvana@gmail.com',   'delivered',  1549.98, '123 Le Loi, Q1, HCM'),
  ('Tran Thi B',    'tranthib@gmail.com',     'shipped',     348.00, '45 Nguyen Hue, Q1, HCM'),
  ('Le Van C',      'levanc@gmail.com',       'processing',  989.98, '78 Tran Hung Dao, Q5, HCM'),
  ('Pham Thi D',    'phamthid@yahoo.com',     'confirmed',   319.98, '12 Hai Ba Trung, Q3, HCM'),
  ('Hoang Van E',   'hoangvane@outlook.com',  'pending',    1299.99, '56 Vo Van Tan, Q3, HCM'),
  ('Vo Thi F',      'vothif@gmail.com',       'delivered',   249.99, '90 CMT8, Q10, HCM'),
  ('Dang Van G',    'dangvang@gmail.com',     'shipped',     438.98, '34 Ly Thuong Kiet, Q11, HCM'),
  ('Bui Thi H',     'buithih@gmail.com',      'cancelled',   199.00, '67 Nguyen Trai, Q5, HCM'),
  ('Ngo Van I',     'ngovali@gmail.com',      'processing',  299.98, '23 Cach Mang, Tan Binh, HCM'),
  ('Ly Thi K',      'lythik@gmail.com',       'pending',     599.00, '89 Phan Xich Long, Phu Nhuan, HCM');

-- ============================================================
-- 8. Seed Data: Order Items
-- ============================================================
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
  (1, 1,  1, 1199.99), (1, 4,  1,  348.00),  -- Order 1: iPhone + Sony headphones
  (2, 4,  1,  348.00),                         -- Order 2: Sony headphones
  (3, 6,  2,  129.99), (3, 8,  1,   69.99),   -- Order 3: 2x Nike shoes + Jeans (discount applied at order)
  (4, 7,  1,  189.99), (4, 6,  1,  129.99),   -- Order 4: Ultraboost + Nike shoes
  (5, 16, 1, 1299.99),                         -- Order 5: Samsung TV
  (6, 9,  1,  249.99),                         -- Order 6: North Face jacket
  (7, 15, 1,  249.00), (7, 7,  1,  189.99),   -- Order 7: AirPods + Ultraboost (discount)
  (8, 13, 1,  199.00),                         -- Order 8: Nespresso (cancelled)
  (9, 17, 1,   99.99), (9, 13, 1,  199.00),   -- Order 9: Mouse + Nespresso (discount)
  (10, 5, 1,  599.00);                         -- Order 10: iPad Air

-- ============================================================
-- 9. Create Debezium replication user
-- ============================================================
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'dbzpwd';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
  ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;