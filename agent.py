import pandas as pd
import numpy as np
import logging


from stable_baselines.common.vec_env    import DummyVecEnv,VecCheckNan
import random
from env.gym_trading_env               import securities_trading_env

class Agent:
    def __init__(self, market_event_securities, market_event_queue, securities, queue, host, policy,strategy, cash_balance,model,env,window_size,current_inventory):

        self.model = model
        self.env = env
        self.window_size = window_size
        self.observations = []
        self.securities = securities
        self.proposed_inventory = current_inventory
        self.strategy = strategy

        logging.basicConfig(level=logging.INFO)


    def train_model(self,train_steps,test_steps):
        self.model.learn(total_timesteps=train_steps)
        obs = self.env.reset()
        # print(obs)
        for i in range(test_steps):
            print(obs)
            action, _states = self.model.predict(obs)
            obs, rewards, done, info = self.env.step(action)
            if done:
                break
            self.env.render()

    def generate_orders(self,observation,actions,current_inventory):
        # logging.debug("Actions",actions)
        # logging.debug("Current Inventory",current_inventory)
        new_inventory =  np.clip(actions, 0., 1.)
        	
        alpha = np.random.uniform(low=0., high=2., size=new_inventory.shape)
        new_inventory *= alpha
        if (np.sum(np.abs(new_inventory))) != 0.:
            new_inventory /= (np.sum(np.abs(new_inventory)))
            new_inventory *= 100
            new_inventory = np.round(new_inventory,0)

        logging.info("Proposed Inventory : %s"%str(new_inventory))
        orders = []
        for idx, sec in enumerate(self.securities):
            quantity = abs(new_inventory[idx] - self.proposed_inventory[sec])
            action = "A"
            if new_inventory[idx] > self.proposed_inventory[sec]:
                side = "B"
                self.proposed_inventory[sec] = new_inventory[idx]
            elif new_inventory[idx] < self.proposed_inventory[sec]:
                side = "S"
                self.proposed_inventory[sec] = new_inventory[idx]
            else:
                continue

            orders.append(
                {
                    "symb": sec,
                    "price": observation[idx],
                    "origQty": quantity,
                    "status": "A",
                    "remainingQty": quantity,
                    # "orderNo": self.internalID,
                    "action": action,
                    "side": side,
                    "FOK": 0,
                    "AON": 0,
                    "strategy": self.strategy,
                }
            )
        return orders

    def model_reaction_to_level(self, observation,current_inventory):
        logging.debug("[X] Observation count: %d/%d"%(len(self.observations), self.window_size))
        if(self.condition_to_make_prediction(observation) and observation.size == len(self.securities)):
            if(len(self.observations) == self.window_size+1):
                # print("Model reaction")
                self.env = DummyVecEnv([lambda: securities_trading_env(np.array(self.observations).T)])
                self.model.learn(total_timesteps=self.window_size)
                # self.env.reset()
                actions,_states = self.model.predict(observation)
                orders = self.generate_orders(observation,actions,current_inventory)
                self.observations = []
                self.env.render()
                return orders
            else:
                self.observations.append(observation)
                return []
        return []

    def condition_to_make_prediction(self,observation):
        return not (None in observation) 

    def model_reaction_to_trade(self, tradeobj):
    	return 0
    def model_reaction_to_ack(self, aMobj):
    	return 0