from persistence import *


def main():

    print('Employees :')
    table = repo.employees.find_all()
    for row in table:
        x = (row.id, row.name.decode(), row.salary, row.branche)
        print(x)
        
    print('Suppliers :')   
    table = repo.suppliers.find_all()
    for row in table:
        x = (row.id, row.name.decode(), row.contact_information.decode())
        print(x)
    
    print('Products :')
    table = repo.products.find_all()
    for row in table:
        x = (row.id, row.description.decode(), row.price, row.quantity)
        print (x)
        
    print('Branches :')
    table = repo.branches.find_all()
    for row in table:
         x = (row.id, row.location.decode(), row.number_of_employees)
         print(x)

    print('Activities :')
    table = repo.activities.find_all()
    for row in table:
        x = (row.product_id, row.quantity, row.activator_id, row.date.decode())
        print(x)

    #printing reports
    print('employees report :')
    table = repo.get_employees_report()
    for row in table:
        if row.total_sales_income is None:
            row.total_sales_income = 0
        x = (row.name.decode(), row.salary, row.working_location.decode(), row.total_sales_income)
        print(x)
   
    table = repo.get_activities_report()
    length = table.__len__()
    if length > 0:
        print('activity report :')
        for row in table:
            if row.name_of_seller is not None:
                row.name_of_seller = row.name_of_seller.decode()
            if  row.name_of_supplier is not None:
                row.name_of_supplier = row.name_of_supplier.decode()
            x = (row.date_of_activity, row.description.decode(), row.quantity, row.name_of_seller, row.name_of_supplier)
            print(x)

if __name__ == '__main__':
    main()