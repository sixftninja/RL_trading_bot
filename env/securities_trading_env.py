import random
import gym
import math
import configparser 
import pandas   as pd
import numpy    as np
from   gym      import spaces

config = configparser.ConfigParser()
config.read('config.ini')


MAX_REWARD          = float(config['ENV']['MaximumReward'])
starting_money      = float(config['ENV']['StartingMoney'])
bid_price_columns   = list(map(int,config['ENV']['ColumnsOfBidPrice'].split(',')))
ask_price_columns   = list(map(int,config['ENV']['ColumnsOfAskPrice'].split(',')))
current_money       = float(config['ENV']['StartingMoney'])
actions             = ['buy','sell','hold']
askPriceList        = []
bidPriceList        = []
debug               = 1
obsSpace            = int(config['ENV']['ObservationSpace'])
initial_flag        = True 
old_data            = np.empty((0,2), float)
max_inventory       = float(config['ENV']['MaxInventory'])


print (ask_price_columns)

class securities_trading_env(gym.Env):

    metadata = {'render.modes': ['human']}

    def __init__(self, df):

        global bid_price_columns, ask_price_columns, starting_money

        super(securities_trading_env, self).__init__()

        self.df                 = df
        self.reward_range       = (0, MAX_REWARD)
        self.MAX_STEPS          = len(df["L1bid_price"])  
        self.CURRENT_REWARD     = 0
        self.current_held_sec   = 0
        self.netWorth           = starting_money

        # 25 columns of L1 bid price
        df_bidPrice = df[df.columns[bid_price_columns]]
        # 25 columns of L1 ask price
        df_askPrice = df[df.columns[ask_price_columns]]

        for data in df_askPrice["L1ask_price"]:
            askPriceList.append(data)

        for data in df_bidPrice["L1bid_price"]:
            bidPriceList.append(data)

        print('TOTAL LIST---->'+str(len(bidPriceList)))
        print(bidPriceList[:-50])
        
        #Action Space - made it box so that it is compatable with both tf3 and ppo2 policy
        self.action_space = spaces.Box(low=np.array([0, 0]), high=np.array([2, 1]), dtype=np.float16)
        
        #Observation space - it has stock price upto window of obsSpace and additional data of current_money/MAX_REWARD  and self.current_held_sec/max_inventory 
        self.observation_space = spaces.Box(low=0, high=1, shape=(obsSpace+1, 2), dtype=np.float64)


    def _next_observation(self):

        global askPriceList, bidPriceList, initial_flag, old_data, current_money, max_inventory
        df      = self.df

        if(initial_flag):
            for _ in range(obsSpace-1):
                curr_askPrice  = askPriceList.pop(0)
                curr_bidPrice  = bidPriceList.pop(0)
                old_data = np.append(old_data, np.array([[curr_askPrice,curr_bidPrice]]), axis=0)

            initial_flag = False

        frame = np.empty((0,2), float)

        if (obsSpace != 1):
            frame   = old_data
            old_data = np.delete(old_data, (0), axis=0)


        curr_askPrice  = askPriceList.pop(0)
        curr_bidPrice  = bidPriceList.pop(0)

        self.current_askPrice   = curr_askPrice
        self.current_bidPrice   = curr_bidPrice
        frame = np.append(frame, np.array([[curr_askPrice,curr_bidPrice]]), axis=0)

        if (obsSpace != 1):
            old_data = np.append(old_data, np.array([[curr_askPrice,curr_bidPrice]]), axis=0)

        frame = np.append(frame, np.array([[current_money/starting_money, self.current_held_sec/max_inventory ]]), axis = 0)

        return frame

    def _take_action(self, action):

        global  current_money, actions, starting_money, max_inventory, MAX_REWARD

        action_type = action[0]
        amount = action[1]

        if(debug ==1):
            print("Action is :", action_type)

        if action_type <= 1 and (current_money -self.current_bidPrice)>0 and self.current_held_sec <= max_inventory:
            #that means the action is buy
            current_money -= self.current_bidPrice 
            self.current_held_sec +=1
            # self.CURRENT_REWARD = math.pow(MAX_REWARD, (current_money)/(starting_money))


        elif action_type <= 2 and action_type > 1  and self.current_held_sec>0:
            #that means the action is sell
            current_money += self.current_bidPrice 
            self.current_held_sec -=1
            # self.CURRENT_REWARD = math.pow(MAX_REWARD, (current_money)/(starting_money))


        # elif action_type <=3 and action_type > 2:
            #that means the action is hold
            #current_money      - remains unchanged 
            #current_held_sec   - remains unchanged
            # self.CURRENT_REWARD = math.pow(MAX_REWARD, (current_money)/(starting_money))

        else:
            print('current_money: %.4f, securities: %d, price: %.4f'%(current_money,self.current_held_sec,self.current_bidPrice))
            print("cant buy or sell")
            # self.CURRENT_REWARD = 0

        self.netWorth = current_money + self.current_held_sec * self.current_bidPrice

    def step(self, action):

        global starting_money

        self._take_action(action)

        self.current_step += 1

        if self.current_step > self.MAX_STEPS :
            self.current_step = 0

        weights = np.clip(action, self.action_space.low, self.action_space.high)
        weights /= (np.sum(np.abs(weights)) + 1e-9)
        weights[0] += np.clip(1 - np.sum(np.abs(weights)), 0, 1)


        delay_modifier = (self.current_step / self.MAX_STEPS)
        # rho1 = self.netWorth / starting_money - 1  # rate of returns
        r1 = np.log((self.netWorth + 1e-9) / (starting_money + 1e-9))  # log rate of return
        reward = r1 * delay_modifier
        # self.CURRENT_REWARD*delay_modifier

        done = self.netWorth <= 0

        obs = self._next_observation()

        return obs, reward, done, {}



    def reset(self):
        global askPriceList, bidPriceList, current_money, bid_price_columns, ask_price_columns, initial_flag, old_data

        self.CURRENT_REWARD     = 0
        self.current_step       = 0
        current_money           = starting_money
        df                      = self.df
        self.current_held_sec   = 0
        askPriceList            = []
        bidPriceList            = []
        initial_flag            = True
        old_data                = np.empty((0,2), float)
        self.netWorth           = starting_money


        df_bidPrice = df[df.columns[bid_price_columns]]
        df_askPrice = df[df.columns[ask_price_columns]]


        for data in df_askPrice["L1ask_price"]:
            askPriceList.append(data)

        for data in df_bidPrice["L1bid_price"]:
            bidPriceList.append(data)

        return self._next_observation()

    def render(self, mode='human', close=False):
        global  current_money

        print(f'Step: {self.current_step}')
        print(f'Price: {self.current_bidPrice}')
        print(f'Balance: {current_money}')
        print(f'Securities Held: {self.current_held_sec}')
        print(f'Current Reward: {self.CURRENT_REWARD}')
        print(f'Net Worth: {self.netWorth}')