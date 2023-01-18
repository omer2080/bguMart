import sqlite3
import atexit
import os
from dbtools import Dao

#===========================================================================================================
# Data Transfer Objects:
class Employee:
    def __init__(self, id, name, salary, branche):
        self.id = id
        self.name = name
        self.salary = salary
        self.branche = branche

class Supplier:
        def __init__(self, id, name, contact_information):
            self.id = id
            self.name = name
            self.contact_information = contact_information

class Product:
    def __init__(self, id, description, price, quantity):
        self.id = id
        self.description = description
        self.price = price
        self.quantity = quantity

class Branche:
    def __init__(self, id, location, number_of_employees):
        self.id = id
        self.location = location
        self.number_of_employees = number_of_employees

class Activitie:
    def __init__(self, product_id, quantity, activator_id, date):
        self.product_id = product_id
        self.quantity = quantity
        self.activator_id = activator_id
        self.date = date
 
class Employees_report:
    def __init__ (self, name, salary, working_location, total_sales_income):
        self.name = name
        self.salary = salary
        self.working_location = working_location
        self.total_sales_income = total_sales_income
        
class Activity_report:
    def __init__ (self, date_of_activity, description, quantity, name_of_seller, name_of_supplier):
        self.date_of_activity = date_of_activity
        self.description = description
        self.quantity = quantity
        self.name_of_seller = name_of_seller
        self.name_of_supplier = name_of_supplier
 
 #===========================================================================================================
 # Data Access Objects:
class Employees:
    def __init__(self, conn):
        self._conn = conn
        
    def insert(self, employeeDTO):
            self._conn.execute("""
                    INSERT INTO employees (id, name, salary, branche) VALUES (?,?,?,?)
                """, [employeeDTO.id, employeeDTO.name, employeeDTO.salary, employeeDTO.branche])
             
    def find(self, id):
        c = self._conn.cursor()
        c.execute("""
                SELECT id, name, salary, branche FROM employees WHERE id = ?
            """, [id,])
        return Employee(*c.fetchone())

    def find_all(self):
        c = self._conn.cursor()
        all = c.execute("""SELECT * FROM employees""").fetchall()
        return [Employee(*row) for row in all]


class Suppliers:
    def __init__(self, conn):
        self._conn = conn
        
    def insert(self, suplierDTO):
        self._conn.execute("""
                INSERT INTO suppliers (id, name, contact_information) VALUES (?,?,?)
            """, [suplierDTO.id, suplierDTO.name, suplierDTO.contact_information])
                                                                        
    def find(self, id):
        c = self._conn.cursor()
        c.execute("""
                SELECT id, name, contact_information FROM suppliers WHERE id = ?
            """, [id,])
        return Supplier(*c.fetchone())
    
    def find_all(self):
        c = self._conn.cursor()
        all = c.execute("""SELECT * FROM suppliers""").fetchall()
        return [Supplier(*row) for row in all]
    

class Products:
    def __init__(self, conn):
        self._conn = conn

    def insert(self, productDTO):
        self._conn.execute("""
                INSERT INTO products (id, description, price, quantity) VALUES (?,?,?,?)
            """, [productDTO.id, productDTO.description, productDTO.price, productDTO.quantity])
     
    def find(self, id):
        c = self._conn.cursor()
        c.execute("""
                SELECT id, description, price, quantity FROM products WHERE id = ?
            """, [id,])
        return Product(*c.fetchone())
    
    def find_all(self):
        c = self._conn.cursor()
        all = c.execute("""SELECT * FROM products""").fetchall()
        return [Product(*row) for row in all]   
    
    def update_product_quantity(self, product):
        c = self._conn.cursor()
        c.execute("""
                UPDATE Products SET quantity = ? WHERE id = ?
            """, [product.quantity, product.id])                
 
    
class Branches:
    def __init__(self, conn):
        self._conn = conn
        
    def insert(self, brancheDTO):
        self._conn.execute("""
                INSERT INTO branches (id, location, number_of_employees) VALUES (?,?,?)
            """, [brancheDTO.id, brancheDTO.location, brancheDTO.number_of_employees])

    def find(self, id):
        c = self._conn.cursor()
        c.execute("""
                SELECT id, location, number_of_employees FROM branches WHERE id = ?
            """, [id,])
        return Branche(*c.fetchone())
    
    def find_all(self):
        c = self._conn.cursor()
        all = c.execute("""SELECT * FROM branches""").fetchall()
        return [Branche(*row) for row in all]


class Activities:
    def __init__(self, conn):
        self._conn = conn
    
    def insert(self, activitieDTO):
        self._conn.execute("""
                INSERT INTO activities (product_id, quantity, activator_id, date) VALUES (?,?,?,?)
            """, [activitieDTO.product_id, activitieDTO.quantity, activitieDTO.activator_id, activitieDTO.date])
    
    def find(self, id):
        c = self._conn.cursor()
        c.execute("""
                SELECT product_id, quantity, activator_id, date FROM activities WHERE id = ?
            """, [id,])
        return Activitie(*c.fetchone())
    
    def find_all(self):
        c = self._conn.cursor()
        all = c.execute("""SELECT * FROM activities""").fetchall()
        return [Activitie(*row) for row in all]
 
   
class Employees_report:
    def __init__ (self, name, salary, working_location, total_sales_income):
        self.name = name
        self.salary = salary
        self.working_location = working_location
        self.total_sales_income = total_sales_income
        
        
class Activities_report:
    def __init__ (self, date_of_activity, description, quantity, name_of_seller, name_of_supplier):
        self.date_of_activity = date_of_activity
        self.description = description
        self.quantity = quantity
        self.name_of_seller = name_of_seller
        self.name_of_supplier = name_of_supplier

 
 
#Repository
class Repository(object):
    def __init__(self):
        self._conn = sqlite3.connect('bgumart.db')
        self._conn.text_factory = bytes
        
        self.employees = Employees(self._conn)
        self.suppliers = Suppliers(self._conn)
        self.products = Products(self._conn)
        self.branches = Branches(self._conn)
        self.activities = Activities(self._conn)

 
    def _close(self):
        self._conn.commit()
        self._conn.close()
 
    def create_tables(self):
        self._conn.executescript("""
            CREATE TABLE employees (
                id              INT         PRIMARY KEY,
                name            TEXT        NOT NULL,
                salary          REAL        NOT NULL,
                branche    INT REFERENCES branches(id)
            );
    
            CREATE TABLE suppliers (
                id                   INTEGER    PRIMARY KEY,
                name                 TEXT       NOT NULL,
                contact_information  TEXT
            );

            CREATE TABLE products (
                id          INTEGER PRIMARY KEY,
                description TEXT    NOT NULL,
                price       REAL NOT NULL,
                quantity    INTEGER NOT NULL
            );

            CREATE TABLE branches (
                id                  INTEGER     PRIMARY KEY,
                location            TEXT        NOT NULL,
                number_of_employees INTEGER
            );
    
            CREATE TABLE activities (
                product_id      INTEGER REFERENCES products(id),
                quantity        INTEGER NOT NULL,
                activator_id    INTEGER NOT NULL,
                date            TEXT    NOT NULL
            );
        """)

    
    def get_employees_report(self):
        all = self._conn.execute(""" 
            SELECT employees.name , employees.salary , branches.location , SUM(ABS(COALESCE(activities.quantity * products.price, 0)))
            FROM employees
            LEFT JOIN branches 
                ON employees.branche = branches.id
            LEFT JOIN activities  
                ON activities.activator_id = employees.id 
            LEFT OUTER JOIN products
                ON activities.product_id = products.id
            GROUP BY name ORDER BY name""").fetchall()
        return [Employees_report(*row) for row in all] 

    def get_activities_report(self):
        all = self._conn.execute("""SELECT activities.date , products.description , activities.quantity , employees.name , suppliers.name
            FROM activities 
            LEFT JOIN products
                ON activities.product_id = products.id
            LEFT JOIN employees 
                ON activities.activator_id = employees.id
            LEFT JOIN suppliers 
                ON activities.activator_id = suppliers.id
            order by activities.date""").fetchall()
        return [Activities_report(*row) for row in all] 

    
    def execute_command(self, script: str) -> list:
        return self._conn.cursor().execute(script).fetchall()
 
# singleton
repo = Repository()
atexit.register(repo._close)