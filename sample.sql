USE myCompany;


INSERT INTO Inventory(quantity) VALUES (221);
INSERT INTO Inventory(quantity) VALUES (348);
INSERT INTO Inventory(quantity) VALUES (64);
INSERT INTO Inventory(quantity) VALUES (175);
INSERT INTO Inventory(quantity) VALUES (375);

INSERT INTO Products(Year,Make,Model,Category,inventory_id,created_at) VALUES (2021,'Plymouth','Prowler','Convertible',2001,'2008-02-28');
INSERT INTO Products(Year,Make,Model,Category,inventory_id,created_at) VALUES (2019,'Oldsmobile','Intrigue','Sedan',2002,'2023-07-26');
INSERT INTO Products(Year,Make,Model,Category,inventory_id,created_at) VALUES (2016,'Hyundai','Sonata Plug-in Hybrid','Sedan',2003,'2022-09-14');
INSERT INTO Products(Year,Make,Model,Category,inventory_id,created_at) VALUES (2017,'Hyundai','Entourage','Van/Minivan',2004,'2001-08-27');
INSERT INTO Products(Year,Make,Model,Category,inventory_id,created_at) VALUES (2020,'Audi','S5','Coupe, Convertible',2005,'2022-07-04');


INSERT INTO Users(username,firstname,lastname,email) VALUES ('ogonzalez','Helen','Black','osbornedebra@example.org');
INSERT INTO Users(username,firstname,lastname,email) VALUES ('vmorgan','Hannah','Peck','eric67@example.net');
INSERT INTO Users(username,firstname,lastname,email) VALUES ('theresaodom','Evan','Wang','sophiataylor@example.com');
INSERT INTO Users(username,firstname,lastname,email) VALUES ('mckenziesteven','Austin','Reed','amy59@example.com');
INSERT INTO Users(username,firstname,lastname,email) VALUES ('martineztim','Brian','Banks','williamdavis@example.org');

INSERT INTO User_detail(user_id,address,city,postcode,country) VALUES (3001,'12749 John Forest
Dawnhaven, MI 94128','West Bradley',17608,'United States Minor Outlying Islands');
INSERT INTO User_detail(user_id,address,city,postcode,country) VALUES (3002,'6385 Chandler Squares Apt. 105
Rebeccafort, MS 04995','North Sarah',58643,'Lithuania');
INSERT INTO User_detail(user_id,address,city,postcode,country) VALUES (3003,'748 Osborne Underpass Apt. 459
Port Janet, NV 57158','Reyesberg',36130,'Palau');
INSERT INTO User_detail(user_id,address,city,postcode,country) VALUES (3004,'69168 Ortega Circles Apt. 398
Charlesville, KS 34750','Lawrenceview',14652,'Wallis and Futuna');
INSERT INTO User_detail(user_id,address,city,postcode,country) VALUES (3005,'7460 Hicks Harbors
Greenton, WY 07163','Cameronville',59824,'South Georgia and the South Sandwich Islands');


INSERT INTO Orders(product_id,quantity,created_at) VALUES (96,2,'2022-02-13');
INSERT INTO Orders(product_id,quantity,created_at) VALUES (96,2,'2012-01-18');
INSERT INTO Orders(product_id,quantity,created_at) VALUES (96,1,'2013-05-13');
INSERT INTO Orders(product_id,quantity,created_at) VALUES (1897,2,'2003-10-23');
INSERT INTO Orders(product_id,quantity,created_at) VALUES (1897,2,'2016-03-29');
INSERT INTO Orders(product_id,quantity,created_at) VALUES (596,1,'2022-02-13');
INSERT INTO Orders(product_id,quantity,created_at) VALUES (596,2,'2012-01-18');
INSERT INTO Orders(product_id,quantity,created_at) VALUES (2001,2,'2013-05-13');
INSERT INTO Orders(product_id,quantity,created_at) VALUES (2003,2,'2003-10-23');
INSERT INTO Orders(product_id,quantity,created_at) VALUES (2003,1,'2012-10-20');


INSERT INTO Order_detail(order_id,user_id,total,payment) VALUES (20001,18,969499,'instalment');
INSERT INTO Order_detail(order_id,user_id,total,payment) VALUES (20002,851,969499,'credit_card');
INSERT INTO Order_detail(order_id,user_id,total,payment) VALUES (20003,224,484749,'credit_card');
INSERT INTO Order_detail(order_id,user_id,total,payment) VALUES (20004,284,302046,'credit_card');
INSERT INTO Order_detail(order_id,user_id,total,payment) VALUES (20005,444,302046,'instalment');
INSERT INTO Order_detail(order_id,user_id,total,payment) VALUES (20006,421,333046,'credit_card');
INSERT INTO Order_detail(order_id,user_id,total,payment) VALUES (20007,28,669499,'credit_card');
INSERT INTO Order_detail(order_id,user_id,total,payment) VALUES (20008,3003,925299,'credit_card');
INSERT INTO Order_detail(order_id,user_id,total,payment) VALUES (20009,3001,815046,'credit_card');
INSERT INTO Order_detail(order_id,user_id,total,payment) VALUES (20010,3002,223604,'credit_card');
