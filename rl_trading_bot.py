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

import gym
import json
import argparse
import configparser 

import collections
from collections import deque
import sys


from stable_baselines.common.policies   import MlpPolicy
from stable_baselines.common.policies   import MlpLstmPolicy
from stable_baselines.common.policies   import MlpLnLstmPolicy
from stable_baselines.common.policies   import CnnPolicy
from stable_baselines.common.policies   import CnnLstmPolicy
from stable_baselines.common.policies   import CnnLnLstmPolicy
from stable_baselines.common.vec_env    import DummyVecEnv,VecCheckNan
from stable_baselines                   import PPO2
from stable_baselines import SAC
from stable_baselines.gail import generate_expert_traj

# from env.securities_trading_env       import securities_trading_env
from env.gym_trading_env               import securities_trading_env

# Imports for DDPG
from stable_baselines                   import DDPG
from stable_baselines.ddpg.policies     import MlpPolicy    as ddpgMlpPolicy
from stable_baselines.ddpg.noise        import NormalActionNoise, OrnsteinUhlenbeckActionNoise,                                                    AdaptiveParamNoiseSpec

# Imports for TD3
from stable_baselines                   import TD3
from stable_baselines.td3.policies      import MlpPolicy as td3MlpPolicy

from mx_communication import Communication
from agent import Agent

class management:

    def _init_environment(self,datapath,window_size):

        df = pd.read_csv(datapath)
        bid_price_columns = [i for i in range(1,len(df.columns),20)]
        print(bid_price_columns)
        ask_price_columns = [i for i in range(3,len(df.columns),20)]
        bidPrices = df[df.columns[bid_price_columns]]
        askPrices = df[df.columns[bid_price_columns]]
        df_concat = pd.concat([bidPrices, askPrices])
        midPrices = df_concat.groupby(df_concat.index).mean().transpose().values[-len(self.securities):]
        print(midPrices[:,0])

        self.env = DummyVecEnv([lambda: securities_trading_env(np.array(midPrices).T)])
        self.env = VecCheckNan(self.env, raise_exception=True)

        n_actions = self.env.action_space.shape[-1]
        param_noise = None
        action_noise = OrnsteinUhlenbeckActionNoise(mean=np.zeros(n_actions), sigma=float(0.5) * np.ones(n_actions))
        print(n_actions)

        if(self.policy == "DDPG"):
           self.model = DDPG(ddpgMlpPolicy, self.env, verbose=int(self.verbose), param_noise=param_noise, action_noise= action_noise)
        elif(self.policy=="TD3"):
            self.model = TD3(td3MlpPolicy, self.env, verbose=int(self.verbose))
        elif(self.policy=="GAIL"):
            self.model = TD3(td3MlpPolicy, self.env, verbose=int(self.verbose))
        else:
            self.model = PPO2(MlpLnLstmPolicy, self.env, verbose=int(self.verbose))

        if self.load: #load model
            self.model = self.model.load("save/"+modelpath+".h5")

        #init model class
        self.gym_model = Agent(market_event_securities, market_event_queue, securities, queue, host, policy,strategy, cash_balance,self.model,self.env,window_size,self.inventory)
    
    def _init_sec_prices(self, securities):
        sec_state = dict()
        for sec in securities:
            sec_state.setdefault(sec, None)
        return sec_state

    def _init_market_dict(self, market_event_securities, market_event_queue):
        market_dict = dict()
        for sec in market_event_securities:
            sym_dict = dict()
            for e in market_event_queue:
                sym_dict[e] = None
            market_dict[sec] = sym_dict
        return market_dict
    
    # size of each security hold is set to be 0 initially
    def _init_inventory(self, securities):
        inventory = dict()
        for sec in securities:
            inventory[sec] = 0.0
        return inventory

    def __init__(self, market_event_securities, market_event_queue, securities, queue, host, policy,strategy, cash_balance,load,train,train_only,verbose,modelpath,datapath,train_steps,test_steps,window_size,episodes):

        logging.basicConfig(level=logging.INFO)
        
        self.policy = policy
        self.strategy = strategy
        self.verbose = verbose
        self.load = load
        self.train = train
        self.modelpath = modelpath

        self.strategy = strategy # identifier for different clients 
        self.market_event_securities = market_event_securities # strings of securities, e.g. [ZFH0:MBO,ZTH0:MBO,UBH0:MBO,ZNH0:MBO,ZBH0:MBO]
        self.market_event_queue = market_event_queue # strings of names of prices in market_event_securities, e.g. [L1, L2, L3]
        self.securities = securities 

        self.num_of_securities = len(self.securities) # number of securities the bot will trade in
        self.internalID = 0 # internal id for every order the bot wants to send
        self.steps = 0 # number of trades the bot has made
        
        self.cash_balance = cash_balance
        self.inventory = self._init_inventory(self.securities) # size of each security hold
        self.inventoryValue = 0.0
        self.PnL = self.cash_balance + self.inventoryValue

        self.outputfile = "save/"+strategy+"_logs.txt"

        self._init_environment(datapath,window_size)

        if self.train: #Train model if true
            for e in range(episodes):
                logging.info(" Episode : %s" % str(e))
                self.gym_model.train_model(train_steps,test_steps)
            self.model = self.gym_model.model
            model_save = "save/"+self.modelpath+".h5"
            logging.info("Model saved as: "+model_save)
            with open(self.outputfile, "a") as myfile:
                myfile.write("Model saved as: %s \n"%model_save)
            self.model.save(model_save)

        if train_only:
            return

        self.market_dict = self._init_market_dict(self.market_event_securities,  self.market_event_queue) # L1-L5 levels data
        # self.market_dict["ZTH0:MBO"]["L1"] to read l1 data of ZTH0:MBO
        self.ask_trend = self._init_market_dict(self.market_event_securities, self.market_event_queue)
        # if self.market_dict["ZTH0:MBO"]["L1"]["L1AskPrice"] goes up, self.ask_trend["ZTH0:MBO"]["L1"] = 1
        # if self.market_dict["ZTH0:MBO"]["L1"]["L1AskPrice"] goes down, self.ask_trend["ZTH0:MBO"]["L1"] = -1
        # if self.market_dict["ZTH0:MBO"]["L1"]["L1AskPrice"] stays the same, self.ask_trend["ZTH0:MBO"]["L1"] = 0
        self.bid_trend = self._init_market_dict(self.market_event_securities, self.market_event_queue)
        # if self.market_dict["ZTH0:MBO"]["L1"]["L1BidPrice"] goes up, self.bid_trend["ZTH0:MBO"]["L1"] = 1
        # if self.market_dict["ZTH0:MBO"]["L1"]["L1BidPrice"] goes down, self.bid_trend["ZTH0:MBO"]["L1"] = -1
        # if self.market_dict["ZTH0:MBO"]["L1"]["L1BidPrice"] stays the same, self.bid_trend["ZTH0:MBO"]["L1"] = 0
        
        self.mid_market = self._init_sec_prices(securities) # half of the sum of current L1 ask price and L1 bid price
        
        self.exIds_to_inIds = dict() # when your order is acked, the bot will receive an external id for it. map exid to inid here.
        self.inIds_to_orders_sent = dict() # orders sent but not acked
        self.inIds_to_orders_confirmed = dict() # orders confirmed by matching agent


        self.talk = Communication(market_event_securities, market_event_queue, securities, queue, host,
                                  callback_for_levels = self.callback_for_levels,
                                  callback_for_acks = self.callback_for_acks,
                                  callback_for_trades = self.callback_for_trades)
        self.talk.kickoff()
    
    def _save_order_being_sent(self, order):
        self.inIds_to_orders_sent[order["orderNo"]] = order
   
    def cancel_order(self, order):
        self.talk._cancel_order(order)

    def send_order(self, order):
        if order["side"] == 'B' and self.PnL < order["price"] * order["origQty"]:
            logging.warning("portfolio : " + str(self.PnL))
            logging.warning("Not enough portfolio to buy " + str(order["origQty"]) + " " + order["symb"])
            return False
        elif order["side"] == 'S' and self.inventory[order["symb"]] < order["origQty"]:
            logging.warning(order["symb"] + " : " + str(self.inventory[order["symb"]]) )
            logging.warning("Not enough " + order["symb"] + " to sell")
            return False
        else:
            order["orderNo"] = self.internalID
            self._save_order_being_sent(order)
            logging.info("\n Order %s is sent" % str(order["orderNo"]))
            self.internalID += 1
            self.talk._send_order(order)
            return True

    def _update_with_trade(self, tradeobj, side, exId):
        # buy side = 1, sell side = -1
        self._update_inventory(tradeobj.symbol, tradeobj.tradeSize * side)
        self._update_inventory_value()
        self._update_cash(tradeobj.tradeSize, tradeobj.tradePrice * (-side))
        self._update_pnl()
        self._update_order_remain(exId, tradeobj.tradeSize)
        logging.info(" [X] Cash : %s" % str(self.cash_balance))
        logging.info(" [X] Inventory Value : %s" % str(self.inventoryValue))
        logging.info(" [X] Portfolio Value : %s" % str(self.PnL))
        with open(self.outputfile, "a") as myfile:
            myfile.write(" [X] Cash : %s\n" % str(self.cash_balance))
            myfile.write(" [X] Inventory Value : %s\n" % str(self.inventoryValue))
            myfile.write(" [X] Portfolio Value : %s\n" % str(self.PnL))

    def _update_inventory(self, symbol, size):
        self.inventory[symbol] += size
        logging.debug(" [X] inventory:")
        with open(self.outputfile, "a") as myfile:
            for sec in self.securities:
                logging.info("%s : %d"%(sec, self.inventory[sec]))
                myfile.write("%s : %d\n"%(sec, self.inventory[sec]))
    
    def _update_inventory_value(self,):
        inventoryValue = 0.0
        for sec in self.securities:
            if self.mid_market[sec] is not None:
                inventoryValue += self.inventory[sec] * self.mid_market[sec]
        self.inventoryValue = inventoryValue
        logging.debug(" [X] inventory value: %d" % self.inventoryValue)
        for sec in self.securities:
            logging.debug("%s : %d"%(sec, self.inventory[sec]))

    def _update_cash(self, size, price):
        self.cash_balance += size * price
        logging.debug(" [X] cash balance: %d" % self.cash_balance)
    
    def _update_pnl(self,):
        self.PnL = self.cash_balance + self.inventoryValue
        logging.debug(" [X] portfolio value: %d" % self.PnL)

    def _update_order_remain(self, exId, size):
        inId = self.exIds_to_inIds[exId]
        self.inIds_to_orders_confirmed[inId]["remainingQty"] -= size
        if self.inIds_to_orders_confirmed[inId]["remainingQty"] == 0:
            self.inIds_to_orders_confirmed.pop(inId)

    # only accept trade which belongs to this bot
    def _condition_to_accept_trade(self, tradeobj):
        exId = 0
        if tradeobj.buyOrderNo in list(self.exIds_to_inIds.keys()):
            print(self.exIds_to_inIds)
            with open(self.outputfile, "a") as myfile:
                myfile.write("Order %s : Buy Order %d is filled with quantity %d of price %s\n" % (str(tradeobj.buyOrderNo),self.exIds_to_inIds[tradeobj.buyOrderNo], tradeobj.tradeSize, tradeobj.tradePrice))
            logging.info("Order %s : Buy Order %d is filled with quantity %d of price %s\n" % (str(tradeobj.buyOrderNo),self.exIds_to_inIds[tradeobj.buyOrderNo], tradeobj.tradeSize, tradeobj.tradePrice))
            return tradeobj.buyOrderNo, 1
        elif tradeobj.sellOrderNo in list(self.exIds_to_inIds.keys()):
            print(self.exIds_to_inIds)
            logging.info("Order %s : Sell Order %d is filled with quantity %d of price %s\n" % (str(tradeobj.sellOrderNo),self.exIds_to_inIds[tradeobj.sellOrderNo], tradeobj.tradeSize, tradeobj.tradePrice))
            with open(self.outputfile, "a") as myfile:
                myfile.write("Order %s : Sell Order %d is filled with quantity %d of price %s\n" % (str(tradeobj.sellOrderNo),self.exIds_to_inIds[tradeobj.sellOrderNo], tradeobj.tradeSize, tradeobj.tradePrice))
            return tradeobj.sellOrderNo, -1
        else:
            return exId, 0

    def callback_for_trades(self, tradeobj):
        exId, side = self._condition_to_accept_trade(tradeobj)
        if side == -1 or side == 1:
            # uodate inventory, pnl, manage orders, decrease reamaining qty, if reamaining qty is 0, remove it from orders_confirmed
            self._update_with_trade(tradeobj, side, exId)
            self.steps = self.steps+1
            
            self.gym_model.model_reaction_to_trade(tradeobj)


    def _update_with_ack(self, aMobj):
        inId = aMobj.internalOrderNo
        exId = aMobj.orderNo
        if aMobj.action == "A" and (inId in self.inIds_to_orders_sent):
            self.inIds_to_orders_confirmed[inId] = self.inIds_to_orders_sent.pop(inId)
            self.exIds_to_inIds[exId] = inId
            logging.info("ExId: %s -> InId: %s" % (exId, inId))
        elif aMobj.action == "D" and (inId in self.inIds_to_orders_confirmed):
            self.inIds_to_orders_sent[inId] = self.inIds_to_orders_confirmed.pop(inId)
            self.exIds_to_inIds[exId] = inId
            logging.info("ExId: %s -> InId: %s" % (exId, inId))

    #record orders which are not successfully sent or canceled in case you want to send them again and map exid to inid
    def callback_for_acks(self, aMobj):
        if (aMobj.strategy  == self.strategy):
            self._update_with_ack(aMobj)

            self.gym_model.model_reaction_to_ack(aMobj)

    def _update_trend(self, trend, symbol, lv, oldprice, newprice):
        if  newprice > oldprice:
            trend[symbol][lv] = 1
        elif newprice < oldprice:
            trend[symbol][lv] = -1
        else:
            trend[symbol][lv] = 0

    def _update_market_dict(self, tob):
        sym = tob["symb"]
        for lv in self.market_event_queue:
            if tob[lv+"AskPrice"] is not None and tob[lv+"BidPrice"] is not None:
                if self.market_dict[sym][lv] is not None:
                    self._update_trend(self.bid_trend, sym, lv, 
                                       oldprice = self.market_dict[sym][lv][lv+"BidPrice"],
                                       newprice=tob[lv+"BidPrice"])
                    self._update_trend(self.ask_trend, sym, lv,
                                       oldprice = self.market_dict[sym][lv][lv+"AskPrice"],
                                       newprice=tob[lv+"AskPrice"])

                self.market_dict[sym][lv] = {lv+"AskPrice":tob[lv+"AskPrice"],
                                             lv+"BidPrice":tob[lv+"BidPrice"],
                                             lv+"AskSize":tob[lv+"AskSize"],
                                             lv+"BidSize":tob[lv+"BidSize"]}
         
        self.mid_market[sym] = 0.5 * (self.market_dict[sym]["L1"]["L1AskPrice"] + self.market_dict[sym]["L1"]["L1BidPrice"])

    # should be called when new level data arrives
    def callback_for_levels(self, tob):
        self._update_market_dict(tob)
        if  tob["symb"] in self.securities:
            self._update_inventory_value()
            self._update_pnl()
            observation = np.array([v for v in self.mid_market.values()])
            orders = self.gym_model.model_reaction_to_level(observation,self.inventory)
            for order in orders:
                self.send_order(order)



if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config.ini')

    datapath       = config['MAIN']['Data']
    test_steps      = int(config['MAIN']['TestSize']) # train size
    train_steps     = int(config['MAIN']['TrainSize']) # test size
    policy          = str(config['MAIN']['Policy']) # stable baselines policy
    episodes        = int(config['MAIN']['Episodes']) # no of episodes to train env
    modelpath       = str(config['MAIN']['Model']) # path to save/load model
    datapath        = str(config['MAIN']['Data']) # path to load static datafile
    window_size     = int(config['MAIN']['WindowSize']) # window size to listen to before action
    cash_balance    = float(config['MAIN']['StartingMoney']) # starting money for bot 
    # trading strategy name for orders
    strategy        = policy+str(config['MAIN']['Strategy'])+str(config['MAIN']['BotNumber'])
    # List of securities to listen and trade on
    securities      = str(config['MAIN']['Securities']).split(",")
    parser = argparse.ArgumentParser()

    parser.add_argument("--load",dest='load', action="store_true") #load saved model
    parser.add_argument("-v","--verbose",dest='verbose', action="store_true") 
    parser.add_argument('--no-train', dest='train', action='store_false') # train on static dataset
    parser.set_defaults(train=True)
    parser.add_argument("--train-only",dest='train_only', action="store_true") # only train on static dataset without RabbitMQ

    args = parser.parse_args()
    #market_event_securities = ["GEH0:MBO","GEM2:MBO","GEU0:MBO"]
    market_event_securities = securities
    print(len(securities))
    market_event_queue = ["L1","L2","L3"]
    queue = ["L1","L2"]
    # host = "172.29.208.37"
    host = "localhost"

    management(market_event_securities, market_event_queue, securities, queue, host, policy,strategy, cash_balance,args.load,args.train,args.train_only,args.verbose,modelpath,datapath,train_steps,test_steps,window_size,episodes)
    #print('Interface Set Up!')



        



