import time
from tcoreapi_mq import * 
import tcoreapi_mq
import threading
import json

def main():

    global g_QuoteZMQ
    global g_QuoteSession

    #登入(與 TOUCHANCE zmq 連線用，不可改)
    g_QuoteZMQ = QuoteAPI("ZMQ","8076c9867a372d2a9a814ae710c256e2")
    q_data = g_QuoteZMQ.Connect("51237")
    print(q_data)

    if q_data["Success"] != "OK":
        print("[quote]connection failed")
        return

    g_QuoteSession = q_data["SessionKey"]


    #查詢指定合约訊息
    # quoteSymbol = "TC.F.TWF.FITX.HOT"
    #print("查詢指定合約：",g_QuoteZMQ.QueryInstrumentInfo(g_QuoteSession, quoteSymbol))
    #查詢指定類型合約列表
    #期貨：Fut
    #期權：Opt
    #證券：Fut2
    print("查詢合約：",g_QuoteZMQ.QueryAllInstrumentInfo(g_QuoteSession,"Fut2"))
    with open('test.txt','w',encoding='utf-8') as file:
        file.write(json.dumps(g_QuoteZMQ.QueryAllInstrumentInfo(g_QuoteSession,"Fut2")))


if __name__ == '__main__':
    main()