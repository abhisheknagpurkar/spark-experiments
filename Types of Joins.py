# Databricks notebook source
# Sample data for employees
emp_data = [
    (1, "Smith", -1, "2018", "10", "M", 3000),
    (2, "Rose", 1, "2010", "20", "F", 2000),
    (3, "Williams", 1, "2010", "10", "M", 1000),
    (4, "Jones", 2, "2005", "10", "F", 2000),
    (5, "Brown", 2, "2010", "40", "", -1),
    (6, "Brown", 2, "2010", "50", "", -1)
]
emp_schema = ["emp_id", "name", "manager_emp_id", "year_joined", "emp_dept_id", "gender", "salary"]

# Sample data for departments
dept_data = [
    ("Finance", 10),
    ("Marketing", 20),
    ("Sales", 30),
    ("IT", 40)
]
dept_schema = ["dept_name", "dept_id"]

# Create DataFrames
empDF = spark.createDataFrame(emp_data, schema=emp_schema)
deptDF = spark.createDataFrame(dept_data, schema=dept_schema)
display(empDF)
display(deptDF)

# COMMAND ----------

print("Inner Join:")
emp_dept = empDF.join(deptDF, empDF["emp_dept_id"] == deptDF["dept_id"], how="inner")
display(emp_dept)

# COMMAND ----------

print("Outer Join")
emp_dept_outer1 = empDF.join(deptDF, empDF["emp_dept_id"] == deptDF["dept_id"], how="outer")
display(emp_dept_outer1)

print("Full Join")
emp_dept_outer2 = empDF.join(deptDF, empDF["emp_dept_id"] == deptDF["dept_id"], how="full")
display(emp_dept_outer2)

print("Full Outer Join")
emp_dept_outer3 = empDF.join(deptDF, empDF["emp_dept_id"] == deptDF["dept_id"], how="fullouter")
display(emp_dept_outer3)

# COMMAND ----------

print("Right Join")
emp_dept_right_outer1 = empDF.join(deptDF, on=empDF["emp_dept_id"] == deptDF["dept_id"], how="right")
display(emp_dept_right_outer1)

print("Right Outer Join")
emp_dept_right_outer2 = empDF.join(deptDF, on=empDF["emp_dept_id"] == deptDF["dept_id"], how="rightouter")
display(emp_dept_right_outer2)

# COMMAND ----------

print("Left Join")
emp_dept_left_outer1 = empDF.join(deptDF, on=empDF["emp_dept_id"] == deptDF["dept_id"], how="left")
display(emp_dept_left_outer1)

print("Left Outer Join")
emp_dept_left_outer2 = empDF.join(deptDF, on=empDF["emp_dept_id"] == deptDF["dept_id"], how="leftouter")
display(emp_dept_left_outer2)

# COMMAND ----------

print("Left Anti Join")
emp_dept_anti = empDF.join(deptDF, on=empDF["emp_dept_id"] == deptDF["dept_id"], how="leftanti")
display(emp_dept_anti)

print("Left Semi Join")
emp_dept_semi = empDF.join(deptDF, on=empDF["emp_dept_id"] == deptDF["dept_id"], how="leftsemi")
display(emp_dept_semi)

# COMMAND ----------

print("Cross Join")
emp_dept_cross = empDF.join(deptDF, on=empDF["emp_dept_id"] == deptDF["dept_id"], how="cross")
display(emp_dept_cross)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col
from datetime import datetime

# Sample data for Customers
customers_data = [
    (1, "John", "Doe", "john.doe@email.com", "New York", "10001"),
    (2, "Alice", "Smith", "alice.smith@email.com", "San Francisco", "94105"),
    (3, "Bob", "Johnson", "bob.johnson@email.com", "Los Angeles", "90001"),
    (4, "Eva", "Williams", "eva.williams@email.com", "Chicago", "60601"),
    (5, "Charlie", "Brown", "charlie.brown@email.com", "Houston", "77001"),
    (6, "Grace", "Lee", "grace.lee@email.com", "Seattle", "98101"),
    (7, "Daniel", "Miller", "daniel.miller@email.com", "Boston", "02101"),
    (8, "Sophia", "Davis", "sophia.davis@email.com", "Denver", "80202"),
    (9, "Liam", "Moore", "liam.moore@email.com", "Austin", "73301"),
    (10, "Olivia", "Anderson", "olivia.anderson@email.com", "Phoenix", "85001")
]

# Define schema for Customers
customers_schema = ["customer_id", "customer_fname", "customer_lname", 
                     "customer_email", "customer_city", "customer_zipcode"]

# Create Customers DataFrame
customers_df = spark.createDataFrame(customers_data, schema=customers_schema)

# Sample data for Orders
orders_data = [
    (1, datetime(2023, 1, 1), 1, "Shipped"),
    (2, datetime(2023, 1, 2), 2, "Pending"),
    (3, datetime(2023, 1, 3), 4, "Shipped"),
    (4, datetime(2023, 1, 4), 6, "Pending"),
    (5, datetime(2023, 1, 5), 8, "Shipped"),
    (6, datetime(2023, 1, 6), 10, "Shipped"),
    (7, datetime(2023, 1, 7), 3, "Pending"),
    (8, datetime(2023, 1, 8), 5, "Shipped"),
    (9, datetime(2023, 1, 9), 7, "Pending"),
    (10, datetime(2023, 1, 10), 9, "Shipped")
]

# Define schema for Orders
orders_schema = ["order_id", "order_date", "order_customer_id", "order_status"]

# Create Orders DataFrame
orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

# Sample data for Order Items
order_items_data = [
    (1, 1, 101, "Product A", 49.99, 2),
    (2, 1, 102, "Product B", 29.99, 1),
    (3, 2, 103, "Product C", 19.99, 3),
    (4, 3, 104, "Product D", 99.99, 1),
    (5, 4, 105, "Product E", 79.99, 2),
    (6, 5, 106, "Product F", 39.99, 1),
    (7, 5, 107, "Product G", 59.99, 3),
    (8, 6, 108, "Product H", 69.99, 2),
    (9, 8, 109, "Product I", 89.99, 1),
    (10, 10, 110, "Product J", 49.99, 2)
]

# Define schema for Order Items
order_items_schema = ["order_item_id", "order_item_order_id", "order_item_product_id",
                      "order_item_product_name", "order_item_product_price", "order_item_quantity"]

# Create Order Items DataFrame
order_items_df = spark.createDataFrame(order_items_data, schema=order_items_schema)


# COMMAND ----------

display(customers_df)

# COMMAND ----------

display(orders_df)

# COMMAND ----------

display(order_items_df)

# COMMAND ----------


