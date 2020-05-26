import heapq
from collections import defaultdict
import pika
import sys
import json
import pdb
import test_pb2
from google.protobuf.json_format import MessageToJson
from collections import OrderedDict, Callable


class DictionaryUpdater(object):
    __slots__ = [
        "symb",
        "origQty",
        "remainingQty",
        "price",
        "FOK",
        "AON",
        "action",
        "side",
        "orderNo",
        "strategy",
    ]

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class Order2(DictionaryUpdater):
    def __init__(self, iterable=(), **kwargs):
        super().__init__(**kwargs)

    def setOrderNo(self, orderNo):
        self.orderNo = orderNo

    def __lt__(self, other):  # todo cleaner
        self.price * 1000000 + self.orderNo < other.price * 1000000 + other.orderNo  # fix


class OrderedDefaultDict(OrderedDict):
    def __init__(self, default=None, *a, **kw):
        OrderedDict.__init__(self, *a, **kw)
        self.default = default
        if default is not None and not isinstance(default, Callable):
            raise TypeError("must be Callable")

    def __getitem__(self, key):
        try:
            return OrderedDict.__getitem__(self, key)
        except KeyError:
            raise self.__missing_(key)

    def __missing__(self, key):
        if self.default == None:
            raise KeyError(key)
        else:
            self[key] = value = self.default()
        return value

    def __reduce__(self):  # for pickle
        if self.default is None:
            args = tuple()
        else:
            args = self.default
        return type(self)(self.default, self)


class Order:
    def __init__(
        self,
        symb,
        action,
        status,
        side,
        price,
        origQty,
        remainingQty,
        orderNo,
        strategy="XYZ",
        FOK=0,
        AON=0,
    ):
        self.price = price
        self.origQty = origQty
        self.remainingQty = remainingQty
        self.symb = symb
        self.status = "A"  # gets switched to I upon deletion
        self.side = side
        self.action = action  #'A' addition 'D' deletion
        self.FOK = FOK  # implemented residual stored or killed not tested
        self.AON = AON  # implemented one level not tested
        self.action = action
        self.strategy = strategy

        self.internalOrderNo = orderNo
        self.orderNo = orderNo

    def setOrderNo(self, orderNo):
        self.orderNo = orderNo

    def __lt__(self, other):  # todo cleaner
        self.price * 1000000 + self.orderNo < other.price * 1000000 + other.orderNo  # fix


class Trade:
    def __init__(self, orderNo, tradeNo, qty):
        self.orderNo = orderNo
        self.qty = qty
        self.tradeNo = tradeNo


class Communication:
    def __init__(self, symb, communicationHost):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=communicationHost))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange="market_data_pb", exchange_type="direct")
        self.channel.exchange_declare(exchange="ACK", exchange_type="direct")
        self.channel.exchange_declare(exchange="L1", exchange_type="direct")
        self.channel.exchange_declare(exchange="L2", exchange_type="direct")
        self.channel.exchange_declare(exchange="L3", exchange_type="direct")
        self.channel.exchange_declare(exchange="orders_pb", exchange_type="direct")
        self.channel.exchange_declare(exchange="trade_data", exchange_type="direct")
        self.tradeChannel = self.channel.queue_declare(
            queue=symb + "_trade_pb"
        )  # outgoing trade Queue
        self.orderChannel = self.channel.queue_declare(
            queue=symb + "_orders_pb"
        )  # incoming orderQueue
        self.channel.queue_bind(exchange="orders_pb", queue=symb + "_orders_pb", routing_key=symb)
        # clearingHouse subscribes to ackchannel for all customers


class OrderBook:
    def _consumeOrder(self, ch, method, properties, body):  # consume callback
        # strip orderL1
        print(" [x] Received %r" % body)
        order2 = test_pb2.OrderBody()
        order2.ParseFromString(body)
        print(
            "\nReceived decoded:\n symb:"
            + order2.symb
            + ",price:"
            + str(order2.price)
            + ",origQty"
            + str(order2.origQty)
            + ",orderNo:"
            + str(order2.orderNo)
            + ",status:"
            + order2.status
            + ",remainingQty:"
            + str(order2.remainingQty)
            + ",action:"
            + order2.action
            + ",side:"
            + order2.side
            + ",FOK:"
            + str(order2.FOK)
            + ",AON:"
            + str(order2.AON)
            + ",strategy:"
            + order2.strategy
        )
        order = Order(
            symb=order2.symb,
            action=order2.action,
            status=order2.status,
            side=order2.side,
            price=order2.price,
            origQty=order2.origQty,
            remainingQty=order2.remainingQty,
            strategy=order2.strategy,
            FOK=order2.FOK,
            AON=order2.AON,
            orderNo=order2.orderNo,
        )

        if order.action == "A":
            self.addOrder(order)
        elif order.action == "D":
            self.deleteOrder(order)
        # pdb.set_trace()
        self.match()
        print(order.symb, order.orderNo)

    def _publishOrder(self, Order):
        pass

    def _publishAck(self, Order):
        pass

    def _publishL1(self):
        l1obj = test_pb2.L1Data()
        l1obj.symb = self.symb
        l1obj.L1BidPrice = -1.0 * self.buy[0][0] if len(self.buy) > 0 else 0
        l1obj.L1AskPrice = self.sell[0][0] if len(self.sell) > 0 else 0
        l1obj.L1BidSize = self.buyLevelInfo[-1.0 * self.buy[0][0]] if len(self.buy) > 0 else 0
        l1obj.L1AskSize = self.sellLevelInfo[self.sell[0][0]] if len(self.sell) > 0 else 0
        l1obj.LastPrice = self.lastTrade
        l1obj.LastSize = self.lastSize
        # For Common Channel
        self.communication.channel.basic_publish(
            exchange="market_data_pb", routing_key="l1_pb", body=l1obj.SerializeToString()
        )
        # For Filtered Exchange
        self.communication.channel.basic_publish(
            exchange="L1", routing_key=self.symb, body=l1obj.SerializeToString()
        )

    def _publishL2(self):
        sell_list = heapq.nsmallest(min(5, len(self.sell)), self.sell)
        buy_list = heapq.nsmallest(min(5, len(self.buy)), self.buy)
        sell_prices = [sell_list[x][0] for x in range(len(sell_list))]
        buy_prices = [buy_list[x][0] for x in range(len(buy_list))]
        sell_l2 = list(dict.fromkeys(sell_prices))
        buy_l2 = list(dict.fromkeys(buy_prices))

        l2obj = test_pb2.L2Data()
        l2obj.symb = self.symb
        l2obj.L1BidPrice = (-1 * buy_l2[0]) if len(buy_l2) > 0 else 0
        l2obj.L1BidSize = (self.buyLevelInfo[-1 * buy_l2[0]]) if len(buy_l2) > 0 else 0
        l2obj.L1AskPrice = (sell_l2[0]) if len(sell_l2) > 0 else 0
        l2obj.L1AskSize = (self.sellLevelInfo[sell_l2[0]]) if len(sell_l2) > 0 else 0
        l2obj.L2BidPrice = (-1 * buy_l2[1]) if len(buy_l2) > 1 else 0
        l2obj.L2BidSize = (self.buyLevelInfo[-1 * buy_l2[1]]) if len(buy_l2) > 1 else 0
        l2obj.L2AskPrice = (sell_l2[1]) if len(sell_l2) > 1 else 0
        l2obj.L2AskSize = (self.sellLevelInfo[sell_l2[1]]) if len(sell_l2) > 1 else 0
        l2obj.L3BidPrice = (-1 * buy_l2[2]) if len(buy_l2) > 2 else 0
        l2obj.L3BidSize = (self.buyLevelInfo[-1 * buy_l2[2]]) if len(buy_l2) > 2 else 0
        l2obj.L3AskPrice = (sell_l2[2]) if len(sell_l2) > 2 else 0
        l2obj.L3AskSize = (self.sellLevelInfo[sell_l2[2]]) if len(sell_l2) > 2 else 0
        l2obj.L4BidPrice = (-1 * buy_l2[3]) if len(buy_l2) > 3 else 0
        l2obj.L4BidSize = (self.buyLevelInfo[-1 * buy_l2[3]]) if len(buy_l2) > 3 else 0
        l2obj.L4AskPrice = (sell_l2[3]) if len(sell_l2) > 3 else 0
        l2obj.L4AskSize = (self.sellLevelInfo[sell_l2[3]]) if len(sell_l2) > 3 else 0
        l2obj.L5BidPrice = (-1 * buy_l2[4]) if len(buy_l2) > 4 else 0
        l2obj.L5BidSize = (self.buyLevelInfo[-1 * buy_l2[4]]) if len(buy_l2) > 4 else 0
        l2obj.L5AskPrice = (sell_l2[4]) if len(sell_l2) > 4 else 0
        l2obj.L5AskSize = (self.sellLevelInfo[sell_l2[4]]) if len(sell_l2) > 4 else 0

        print(f"l2 sent to {self.symb}: {repr(l2obj)}")
        # Common Channel
        self.communication.channel.basic_publish(
            exchange="market_data_pb", routing_key="l2_pb", body=l2obj.SerializeToString()
        )
        # Filtered Channel
        self.communication.channel.basic_publish(
            exchange="L2", routing_key=self.symb, body=l2obj.SerializeToString()
        )

    def _publishL3(self):
        pass

    def __init__(self, symb, communicationHost, proRata=False):
        self.symb = symb
        self.proRata = proRata
        self.lastTrade = 0
        self.lastSize = 0
        self.startOrders = 0  # to clean up orders array
        self.orderNo = (
            -1
        )  # this guarantees larger orderNo means later arrival, symb orderNo combo is unqiue
        self.tradeNo = -1
        self.orders = (
            []
        )  # list of all orders key order no : todo drop elements every so often, prealloc for performance
        self.buy = (
            []
        )  # buy heap each element an order price * -1 for sorting in right order #collapse both into 1 structure
        self.sell = []  # sell heap each element an order
        #        self.buyLevelInfo=defaultdict(lambda: 0.0) #keeps track of total size at price level buy side. necessary? optimal? collapse both into array
        #        self.sellLevelInfo=defaultdict(lambda: 0.0) #keeps track of total size at price level sell side. necessary? optimal?

        self.buyLevelInfo = OrderedDefaultDict(
            lambda: 0.0
        )  # keeps track of total size at price level buy side. necessary? optimal? collapse both into array
        self.sellLevelInfo = OrderedDefaultDict(
            lambda: 0.0
        )  # keeps track of total size at price level sell side. necessary? optimal?

        self.communication = Communication(self.symb, communicationHost)  # connections
        self.communication.channel.basic_consume(
            queue=self.symb + "_orders_pb", on_message_callback=self._consumeOrder, auto_ack=False
        )
        self.communication.channel.start_consuming()

    def _sendTOB(self):
        if len(self.buy) > 0 and len(self.sell) > 0:
            print(-1.0 * self.buy[0][0], self.sell[0][0])
            tobobj = test_pb2.TOB()
            tobobj.L1BidPrice = -1.0 * self.buy[0][0]
            tobobj.L1AskPrice = self.sell[0][0]
            tobobj.L1BidSize = self.buyLevelInfo[-1.0 * self.buy[0][0]]
            tobobj.L1AskSize = self.sellLevelInfo[self.sell[0][0]]
            # TODO: this is erroneous, the exchange used never created, ask Peter what should
            # this function do
            self.communication.channel.basic_publish(
                exchange="", routing_key="ibm_L1_pb", body=tobobj.SerializeToString()
            )

    def _sendTrade(self, buyOrderNo, sellOrderNo, tradeNo, tradePrice, tradeSize, symbol):
        if len(self.buy) > 0 and len(self.sell) > 0:
            tradeorderobj = test_pb2.TradeOrder()
            tradeorderobj.buyOrderNo = buyOrderNo
            tradeorderobj.sellOrderNo = sellOrderNo
            tradeorderobj.tradeNo = tradeNo
            tradeorderobj.tradePrice = tradePrice
            tradeorderobj.tradeSize = tradeSize
            tradeorderobj.symbol = symbol

            self.communication.channel.basic_publish(
                exchange="trade_data", routing_key=symbol, body=tradeorderobj.SerializeToString()
            )

            self.communication.channel.basic_publish(
                exchange="trade_data",
                routing_key="trade_pb",
                body=tradeorderobj.SerializeToString(),
            )

    def _cleanupOrders(self, newStart):
        self.startOrders += newStart
        self.order = self.order[newStart:]

    def _removeInactiveOrders(self):

        # remove deleted items or residuals if FOK

        while True:
            if len(self.buy) > 0:
                if self.orders[self.buy[0][1]].status == "I" or self.orders[self.buy[0][1]].FOK:
                    heapq.heappop(self.buy)
                else:
                    break
            else:
                break
        while True:
            if len(self.sell) > 0:
                if self.orders[self.sell[0][1]].status == "I" or self.orders[self.sell[0][1]].FOK:
                    heapq.heappop(self.sell)
                else:
                    break
            else:
                break

    def match(self):
        matched = False
        # only need to do when better than tob order is added
        # can handle a batch of new orders

        #        self._removeInactiveOrders()

        # todo move below lines to better place, most of the time you only need one
        while (
            len(self.buy) > 0
            and len(self.sell) > 0
            and (self.orders[self.buy[0][1]].price) >= self.orders[self.sell[0][1]].price
        ):
            fillQty = min(
                self.orders[self.buy[0][1]].remainingQty, self.orders[self.sell[0][1]].remainingQty
            )

            # todo prorata self.proRata
            # ratio = aggressiveOrder.remainingSize/level[p].size now and then some rounding
            # problem is non filled passive side needs to stay on heap

            # lets just do current ratio versus remainin
            tradePrice = (
                -1.0 * self.buy[0][0] if self.buy[0][0] < self.sell[0][0] else self.sell[0][0]
            )
            self.orders[self.buy[0][1]].remainingQty -= fillQty
            self.buyLevelInfo[self.orders[self.buy[0][1]]] -= fillQty


            self.orders[self.sell[0][1]].remainingQty -= fillQty
            self.sellLevelInfo[self.orders[self.sell[0][1]]] -= fillQty
            # pdb.set_trace()
            if self.orders[self.buy[0][1]].remainingQty <= 0.0:
                print(self.buy[0][1])
                self.orders[self.buy[0][1]].status = "I"

                # add to filled buy orders
                # create trade
                print(
                    "trade (buy): ",
                    self.orders[self.buy[0][1]].price,
                    tradePrice,
                    fillQty,
                )
                self.tradeNo += 1
                self._sendTrade(
                    self.buy[0][1], 0, self.tradeNo, tradePrice, fillQty, self.symb
                )
                heapq.heappop(self.buy)


            if self.orders[self.sell[0][1]].remainingQty <= 0.0:
                self.orders[self.sell[0][1]].status = "I"

                # add to filled sell orders
                # create trade
                print(
                    "trade (sell): ",
                    self.orders[self.sell[0][1]].price,
                    tradePrice,
                    fillQty,
                )
                self.tradeNo += 1
                self._sendTrade(
                    0,self.sell[0][1] , self.tradeNo, tradePrice, fillQty, self.symb
                )
                heapq.heappop(self.sell)

            self.lastTrade = tradePrice
            self.lastSize = fillQty
            self._publishL1()
            self._publishL2()
            matched = True

        self._removeInactiveOrders()  # remove any residual from tob if required
        if not matched:
            self._publishL1()
            self._publishL2()
        return

    def _checkAON(self, order):
        # todo case it is through stack
        if order.side == "B":
            if self.buyLevelInfo[order.price] < order.remainingQty:
                order.status = "I"
        else:
            if self.sellLevelInfo[order.price] < order.remainingQty:
                order.status = "I"

    def addOrder(self, order):
        self.orderNo += 1
        aMobj = test_pb2.aM()
        aMobj.strategy = order.strategy
        aMobj.internalOrderNo = order.orderNo
        aMobj.symb = order.symb
        aMobj.orderNo = self.orderNo
        aMobj.action = "A"
        self.communication.channel.basic_publish(
            exchange="ACK", routing_key=self.symb, body=aMobj.SerializeToString()
        )
        self.communication.channel.basic_publish(
            exchange="ACK", routing_key="ack_pb", body=aMobj.SerializeToString()
        )

        aM2obj = test_pb2.aM2()
        aM2obj.symb = order.symb
        aM2obj.orderNo = self.orderNo
        aM2obj.action = order.action
        aM2obj.price = order.price
        aM2obj.remainingQty = order.remainingQty
        aM2obj.AON = order.AON
        aM2obj.FOK = order.FOK
        aM2obj.Side = order.side
        self.communication.channel.basic_publish(
            exchange="market_data_pb", routing_key="l3_pb", body=aM2obj.SerializeToString()
        )
        self.communication.channel.basic_publish(
            exchange="L3", routing_key=self.symb, body=aM2obj.SerializeToString()
        )
        order.setOrderNo(self.orderNo)

        self._checkAON(order)
        self.orders.append(order)
        if order.side == "B":
            self.buyLevelInfo[order.price] += order.remainingQty
            heapq.heappush(self.buy, (-order.price, order.orderNo))
        else:
            self.sellLevelInfo[order.price] += order.remainingQty
            heapq.heappush(self.sell, (order.price, order.orderNo))

    def deleteOrder(self, order):
        self.orders[self.orderNo].status = "I"
        aMobj = test_pb2.aM()
        aMobj.strategy = order.strategy
        aMobj.internalOrderNo = order.internalOrderNo
        aMobj.symb = order.symb
        aMobj.orderNo = self.orderNo
        aMobj.action = "D"
        print(aMobj)
        self.communication.channel.basic_publish(
            exchange="ACK", routing_key=self.symb, body=aMobj.SerializeToString()
        )
        self.communication.channel.basic_publish(
            exchange="ACK", routing_key="ack_pb", body=aMobj.SerializeToString()
        )

        if order.side == "B":
            self.buyLevelInfo[order.price] -= order.remainingQty
        else:
            self.sellLevelInfo[order.price] -= order.remainingQty
        # to do send ack channel message if you can't delete order because already filled


if __name__ == "__main__":

    #    o1=Order('ibm','B',100.,10)
    #    o2 = Order('ibm', 'B', 101., 20)
    #    o3 = Order('ibm', 'B', 102., 10)
    #    o4 = Order('ibm', 'S', 100., 40)
    # d1={'symb':'ibm','price':100,'origQty':200.,'orderNo':0,'status':'A','remainingQty':0.0}
    # o1=Order2(**d1)
    # ob= OrderBook('ZTH0:MBO','localhost')
    symbol = str(sys.argv[1])
    ip = str(sys.argv[2])
    ob = OrderBook(symbol, ip)

