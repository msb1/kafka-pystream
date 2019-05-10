'''
Created on Mar 28, 2019

@author: barnwaldo
'''
from core.kbuilder import KStreamBuilder, KStream, KGlobalTable

class Customer_Order(object):

    def __init__(self, order, customer):
        self.order = order
        self.customer = customer
    
class Enriched_Order(object):

    def __init__(self, product, customer, order):
        self.order = order 
        self.product = product
        self.customer = customer
         

def peek(kv):
    print("Peeker -> key: {} -- value: {} {}".format(kv.key, kv.val, kv.val.__dict__))
    return kv

# config info for Kafka broker
config = {'bootstrap.servers': '192.168.21.3:9092', 'group.id': 'barnwaldo', 'session.timeout.ms': 6000}

if __name__ == '__main__':
    
    # create stream of orders
    order_builder = KStreamBuilder('orders', config, keyserde='int', valserde='json')
    order_stream = KStream(order_builder.builder)
    
    # create global table of customers
    cust_builder = KStreamBuilder('customers', config, keyserde='int', valserde='json')
    customers = KGlobalTable(cust_builder.builder, materialized='customer_store')
    
    # create global table of products
    prod_builder = KStreamBuilder('products', config, keyserde='int', valserde='json')
    products = KGlobalTable(prod_builder.builder, materialized='product_store')
    
    
    customer_order_stream = order_stream.join(customers, key_mapper=(lambda kv: kv.val['customerId']), 
                                              value_joiner=(lambda order, customer: Customer_Order(order, customer)))
     
    enriched_order_stream = customer_order_stream.join(products, key_mapper=(lambda kv: kv.val.order['productId']),
                            value_joiner=(lambda customerOrder, product: Enriched_Order(product, customerOrder.customer, customerOrder.order)))
    
    enriched_order_stream.peeker(peek)
    # change table back to stream and then send output to Kafka topic
    enriched_order_stream.to('enriched-orders', config, keyserde='int', valserde='json')





