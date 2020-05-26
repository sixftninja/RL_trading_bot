import random
import pika
import time
import pandas as pd
import numpy as np
import json
import test_pb2
import collections
from collections import deque
import datetime
import sys
import logging
class Communication:
    def __init__(self, market_event_securities, market_event_queue, securities, queue, host,
                 callback_for_levels, callback_for_acks, callback_for_trades):
        logging.basicConfig(level=logging.INFO)
        # securities is included in market_event_securities
        self.num = len(market_event_securities) # number of securities being monitored, e.g. 5
        self.market_event_securities = market_event_securities # strings of securities, e.g. [ZFH0:MBO,ZTH0:MBO,UBH0:MBO,ZNH0:MBO,ZBH0:MBO]
        self.market_event_queue = market_event_queue # strings of names of prices in market_event_securities, e.g. [L1, L2, L3]
        self.securities = securities # strings of securities that can be traded in e.g [ZFH0:MBO,ZTH0:MBO]
        self.queue = queue # strings of names of prices in securities, e.g. [L1,L2]
        self.all_queue = ["L1", "L2", "L3", "L4", "L5"]


        # connect to the rabbitMQ server
        self.host = host # address of the rabbitMQ server
        
        #self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host,credentials=pika.PlainCredentials('test2', 'test2')))
        # self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        print("HOST--->"+self.host)
        # self.credentials = pika.PlainCredentials('test2', 'test2')
        # self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=self.credentials))
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        except:
            self.host = "172.29.208.37"
            self.credentials = pika.PlainCredentials('test2', 'test2')
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=self.credentials))
        
        self.channel = self.connection.channel()
        self._subscribe_to_levels()
        self._subscribe_to_trades()
        self._subscribe_to_acks()
        self._set_up_publish_exchange()
        
        # callback functions
        self.callback_for_levels = callback_for_levels
        self.callback_for_acks = callback_for_acks
        self.callback_for_trades = callback_for_trades
    
    def kickoff(self,):
        self.channel.start_consuming()

    
    def _subscribe_to_levels(self,):
        Ln = "L2"
        for sym in self.market_event_securities:
            self.channel.exchange_declare(exchange = Ln, exchange_type='direct')
            self.channel.queue_declare(queue = sym + "__" + Ln) # keep queue name unique
            self.channel.queue_bind(exchange = Ln, queue = sym + "__" + Ln, routing_key = sym)
            self.channel.basic_consume(queue = sym + "__" + Ln, on_message_callback = self.on_response_levels, auto_ack = True)

    def on_response_levels(self, ch, method, properties, body):
        logging.debug("\n [X] Received Level Data")
        l2Data = test_pb2.L2Data()
        l2Data.ParseFromString(body)
        tob = dict()
        tob["symb"] = str(l2Data.symb)
        dictAskPrice={'L1':l2Data.L1AskPrice,'L2':l2Data.L2AskPrice,'L3':l2Data.L3AskPrice,'L4':l2Data.L4AskPrice,'L5':l2Data.L5AskPrice}
        dictBidPrice={'L1':l2Data.L1BidPrice,'L2':l2Data.L2BidPrice,'L3':l2Data.L3BidPrice,'L4':l2Data.L4BidPrice,'L5':l2Data.L5BidPrice}
        dictAskSize={'L1':l2Data.L1AskSize,'L2':l2Data.L2AskSize,'L3':l2Data.L3AskSize,'L4':l2Data.L4AskSize,'L5':l2Data.L5AskSize}
        dictBidSize={'L1':l2Data.L1BidSize,'L2':l2Data.L2BidSize,'L3':l2Data.L3BidSize,'L4':l2Data.L4BidSize,'L5':l2Data.L5BidSize}
        for lv in self.all_queue:
            tob[lv+"AskPrice"] = dictAskPrice[lv]
            tob[lv+"BidPrice"] = dictBidPrice[lv]
            tob[lv+"AskSize"] = dictAskSize[lv]
            tob[lv+"BidSize"] = dictBidSize[lv]
        logging.debug(" [X] Received level with listening security: %s" % tob["symb"])
        
        # what will the bot do when it receives a market event?
        # --- define the behavior in callback_for_levels
        orders_you_want_to_send = self.callback_for_levels(tob)

    def _subscribe_to_trades(self,):
        for sym in self.market_event_securities:
            self.channel.exchange_declare(exchange = "trade_data", exchange_type='direct')
            self.channel.queue_declare(queue = sym + "__trade") # keep queue name unique
            self.channel.queue_bind(exchange = "trade_data", queue = sym + "__trade", routing_key = sym)
            # currently, only use trade data to update portfolio
            if sym in self.securities:
                self.channel.basic_consume(queue = sym + "__trade", on_message_callback = self.on_response_trades, auto_ack = True)
            else:
                logging.debug("\n [X] undefined callback behavior for trades of %s"%sym)
    
    def on_response_trades(self, ch, method, properties, body):
        logging.debug("\n [X] Received trade")
        tradeobj = test_pb2.TradeOrder()
        tradeobj.ParseFromString(body)
        logging.debug(" [X] Received trade with listening security: %s" % str(tradeobj.symbol))
        
        # what will the bot do when it receives a trade event?
        # --- define the behavior in callback_for_trades (tradeobj are trades related to a symbol
        # no matter whether it contains orders sent by this bot or not)
        self.callback_for_trades(tradeobj)
    
    # when you send orders or cancel orders, you will receive acks for them
    def _subscribe_to_acks(self,):
        # only symbols in self.securities will be traded in
        for sym in self.securities:
            self.channel.exchange_declare(exchange = "ACK", exchange_type='direct')
            self.channel.queue_declare(queue = sym + "__ACK") # keep queue name unique
            self.channel.queue_bind(exchange = "ACK", queue = sym + "__ACK", routing_key = sym)
            self.channel.basic_consume(queue = sym + "__ACK", on_message_callback = self.on_response_acks, auto_ack = True)

    def on_response_acks(self, ch, method, properties, body):
        logging.debug("\n [x] Received acknowledge")
        aMobj = test_pb2.aM()
        aMobj.ParseFromString(body)
        logging.debug(" [x] Received ACK with listening security: %s from strategy %s" % (str(aMobj.symb), aMobj.strategy) )
        
        # what will the bot do when it receives an ack for submitting an order or canceling an order?
        # --- define the behavior in callback_for_acks (aMobj are acks related to a symbol
        # no matter whether it contains orders sent or canceled by this bot or not)
        self.callback_for_acks(aMobj)
    
    def _set_up_publish_exchange(self,):
        self.channel.exchange_declare(exchange="orders_pb", exchange_type="direct")
   
    def _parse_order(self, Order):
        order = test_pb2.OrderBody()
        order.symb = Order['symb']
        order.price = Order['price']
        order.origQty = Order['origQty']
        order.orderNo = Order['orderNo']
        order.status = Order['status']
        order.remainingQty = Order['remainingQty']
        order.action = Order['action']
        order.side = Order['side']
        order.FOK = Order['FOK']
        order.AON = Order['AON']
        order.strategy = Order['strategy']
        return order

    def _send_order(self, Order):
        logging.debug("\n [X] Send a new order for action %s" % Order["side"])
        order = self._parse_order(Order)
        self.channel.basic_publish(exchange='orders_pb', routing_key=order.symb, body=order.SerializeToString())
        logging.debug(" [X] Send order ", order.orderNo)

    def _cancel_order(self, Order):
        Order["action"] = "D"
        logging.debug("\n [X] Cancel an order for action %s" % Order["side"])
        order = self._parse_order(Order)
        self.channel.basic_publish(exchange='orders_pb', routing_key=order.symb, body=order.SerializeToString())
        logging.debug(" [X] Cancel order %s" % str(order.orderNo))