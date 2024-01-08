

CREATE DATABASE IF NOT EXISTS myCompany;


USE myCompany;


CREATE TABLE IF NOT EXISTS Users (
  email TEXT(255),
  firstname TEXT(255),
  id INT PRIMARY KEY AUTO_INCREMENT,
  lastname TEXT(255),
  username TEXT(255)
);


CREATE TABLE IF NOT EXISTS Inventory (
  id INT PRIMARY KEY AUTO_INCREMENT,
  quantity INT
);



CREATE TABLE IF NOT EXISTS User_detail (
  address TEXT(255),
  city TEXT(255),
  country TEXT(255),
  id INT PRIMARY KEY AUTO_INCREMENT,
  postcode TEXT(255),
  user_id INT,
  FOREIGN KEY (user_id) REFERENCES Users(id)
);



CREATE TABLE IF NOT EXISTS Products (
  category TEXT(255),
  created_at DATE,
  id INT PRIMARY KEY AUTO_INCREMENT,
  inventory_id INT,
  make TEXT(255),
  model TEXT(255),
  year YEAR,
  FOREIGN KEY (inventory_id) REFERENCES Inventory(id)
);



CREATE TABLE IF NOT EXISTS Orders (
  created_at DATE,
  id INT PRIMARY KEY AUTO_INCREMENT,
  product_id INT NOT NULL,
  quantity INT
);



CREATE TABLE IF NOT EXISTS Order_detail (
  id INT PRIMARY KEY AUTO_INCREMENT,
  order_id INT NOT NULL,
  payment TEXT(255) NOT NULL,
  total INT NOT NULL,
  user_id INT NOT NULL,
  FOREIGN KEY (user_id) REFERENCES Users(id),
  FOREIGN KEY (order_id) REFERENCES Orders(id)
);




