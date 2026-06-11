-- ---------------------------------------------------------------------------
-- Seed data for the test schema. CUSTOMER_ID is identity-generated (1,2,3);
-- ORDER_ID is drawn from ORDER_SEQ (1000, 1001, ...).
-- ---------------------------------------------------------------------------

INSERT INTO ORACLE_USER.CUSTOMERS (NAME, EMAIL, BALANCE, ACTIVE, NOTES)
    VALUES ('Alice Smith', 'alice@example.com', 1500.50, 1, 'VIP customer - priority support');
INSERT INTO ORACLE_USER.CUSTOMERS (NAME, EMAIL, BALANCE, ACTIVE, NOTES)
    VALUES ('Bob Jones',   'bob@example.com',      0.00, 1, NULL);
INSERT INTO ORACLE_USER.CUSTOMERS (NAME, EMAIL, BALANCE, ACTIVE, NOTES)
    VALUES ('Carol White', 'carol@example.com',  -25.00, 0, 'Inactive - closed account');

INSERT INTO ORACLE_USER.ORDERS (ORDER_ID, CUSTOMER_ID, ORDER_DATE, AMOUNT, STATUS)
    VALUES (ORACLE_USER.ORDER_SEQ.NEXTVAL, 1, DATE '2024-01-15', 250.00, 'SHIPPED');
INSERT INTO ORACLE_USER.ORDERS (ORDER_ID, CUSTOMER_ID, ORDER_DATE, AMOUNT, STATUS)
    VALUES (ORACLE_USER.ORDER_SEQ.NEXTVAL, 1, DATE '2024-02-03', 99.99,  'SHIPPED');
INSERT INTO ORACLE_USER.ORDERS (ORDER_ID, CUSTOMER_ID, ORDER_DATE, AMOUNT, STATUS)
    VALUES (ORACLE_USER.ORDER_SEQ.NEXTVAL, 2, DATE '2024-03-21', 12.49,  'NEW');

COMMIT;
