import pandas as pd
import numpy as np

class Account:

    def __init__(self, num, strategy, securities):
        self.num = num # number of securities that the bot listens to
        self.strategy = strategy # strategy name, e.g. double_q_learning
        self.securities = securities # list of security names
        self.sec2idx = dict() # security to index mapping
        for i, sec in enumerate(securities):
            self.sec2idx[sec] = i
        self.buys = np.zeros(num) #vector of buys (positive number), length of num of securities the bot oversees
        self.sells = np.zeros(num) #vector of sells (positive number), length of num of securities the bot oversees
        self.inventory = np.zeros(num) #vector of inventory (negative:short, positive:long), length of num of securities the bot oversees
        self.cash_balance = 0.0
        self.market_value = 0.0
        self.portfolio_value = 0.0 # total value when mark to market
        
    def get_securities(self):
        return self.securities
    
    def get_strategy_name(self):
        return self.strategy
    
    def get_securitiy_index(self):
        return self.sec2idx
    
    def get_inventory(self):
        return self.inventory
    
    def get_balance(self):
        return self.cash_balance

    def append(self, size, price, security): # single security append
        if security in self.sec2idx:
            if size <0:
                self.inventory[self.sec2idx[security]] += size
                self.cash_balance += abs(size) * price
            else:
                self.inventory[self.sec2idx[security]] += size
                self.cash_balance -= size * price 
        
    def append_multiple(self, sizes, prices): # multiple securities append
        sell_sizes = [0 if s > 0 else abs(s) for s in sizes]
        buy_sizes = [s if s > 0 else 0 for s in sizes]
        self._append_sell(self, sell_sizes, prices)
        self._append_buy(self, buy_sizes, prices)
          
    def _append_sell(self, sell_sizes, sell_prices):
        self.inventory -= sell_sizes
        self.cash_balance += np.dot(sell_sizes, sell_prices)
        
    def _append_buy(self, buy_sizes, buy_prices):
        self.inventory += buy_sizes
        self.cash_balance -= np.dot(buy_sizes, buy_prices)
        
    def mark_to_market(self, market_prices):
        self.market_value = np.dot(market_prices, self.inventory)
        self.portfolio_value = self.cash_balance + self.market_value
        return self.market_value, self.portfolio_value
