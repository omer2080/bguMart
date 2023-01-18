from persistence import *

import sys

def main(args : list[str]):
    inputfilename : str = args[1]
    with open(inputfilename) as inputfile:
        # print("printing for test :")
        for line in inputfile:
            splittedline : list[str] = line.strip().split(", ")
            _product_id = int(splittedline[0])
            _quantity = int(splittedline[1])
            _activator = splittedline[2]
            _date = splittedline[3]
            #print ("this is only for tests :")
            # print(_product_id, _quantity, _activator, _date)
            
            product = repo.products.find(_product_id)     
            if product.quantity + _quantity >= 0:
                repo.activities.insert(Activitie(*splittedline))
                product.quantity = product.quantity + _quantity
                repo.products.update_product_quantity(product) 

if __name__ == '__main__':
    main(sys.argv)