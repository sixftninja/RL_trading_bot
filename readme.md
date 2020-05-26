
# **Reinforcement Learning based Trading Bot**
1) Create a Portfolio of Stocks using Open AI gym and Stable Baselines.
2) Experiment with different trading strategies.
3) Connect to RabbitMQ to excecute orders and generate PnL


## Dataset Description
Folder: `/data/concat.csv` </br>
Its a static dataset consisting of the bid_price,ask_price,bid_size,ask_size for 25 securities for 1000 timesteps

## Requirements
Check `requirements.txt` </br>
`python -m pip install -r requirements.txt` </br>
Requires Python 3.6/3.7 </br>
Conda Env for the 100 server: </br>
`conda activate /home/citi/anaconda3/envs/cudf-nightly`



## Configuration
File: `config.ini`
Change:
1) Bot Number
2) Model (Path of the saved Model or to save model)

Look out for:
1) **Baseline Algorithm**: (DDPG/TD3/PPO2)
2) **Episodes:** No of epochs to train
3) **Strategy:** Trading Strategy to implement
4) **Train and Test Size**
5) **Window Size for Technical Indicators**
6) **List of Securities to listen and trade on**

The Env and the Bot have can have different strategies and initial capital if required.


## To Run
`python rl_trading_bot.py [--load] [--no-train] [--train-only]` </br>

**Options/Arguments:**
1) load the pretrained model from the modelpath in the config
2) do not train on static data
3) only train and save the model

Save folder for model and logs: `save/`


## Agent
File: `agent.py`</br>
To train, react to levels data, trade data, generate orders based on the actions of the model.
RL Algorithms and Policy:
1) DDPG - DDPGMLP 
2) TD3 - TD3MLP
3) PPO2 - MLPLSTM </br>
These alogrithms are imported from stable baselines [[1]](#1) and trained in a custom Open AI Gym env [[2]](#2).

## Custom Env
Folder: `/env` </br>
**gym_trading_env*8: Custom gym env for custom trading strategy for the selected list of securities

**Observation Space:** Box - average of bid and price stocks for selected securities </br>
Action Space: Box - range [0,1] (Only longs or reallocation, no negative inventory (negative weights))

## Trading Strategies
1) **Momentum:** Ratio of the average bid price in the window with the average price upto current step.
2) **Mean Reversion:** Inverse of momentum. Assumes the security is mean reverting.
3) **Moving Average Convergence Diverngence:** Get discreet signals on a rolling window by combining two moving averages

**Env:**</br>
The current action defines the weights of the portfolio. The sample from the action is clipped between 0,1 and normalized such that the sum of all the weights = 1. This ensures that the portfolio is completely utilized with a distribution of securities. (Only positive inventory values) </br>
The reward for the action is log rate of return with the new weights of the portfolio normalized by the progress for a delayed reward.

**Bot:**</br>
Similarly the current action defines the weights of the portfolio. The sample from the action is clipped between 0,1 and normalized such that the sum of all the weights = 1. Based on the distribution, te portfolio is reallocated and the agent sends buy or sell orders of quantitiy equal to the change of allocation.

## RabbitMQ
**File:** `mx_communication.py` </br>
Create and listen to the channels and communicate with the bots.</br>
**File:** `test_pb2.py` </br>
Protocol buffer generated descriptors

## Next Steps
1) Train the agent on new levels data based on a rolling window rather than a static set of observations
2) Update model and alphas in the bot based on the completed trade (Would require sync with the matching engine)


## References
<a id="1">[1]</a> 
https://stable-baselines.readthedocs.io/en/master/

<a id="2">[2]</a>
https://gym.openai.com/
