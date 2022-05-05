import baostock as bs
import pandas as pd

#### 登陆系统 ####
lg = bs.login()
# 显示登陆返回信息
# print('login respond error_code:'+lg.error_code)
# print('login respond  error_msg:'+lg.error_msg)

#### 获取历史K线数据 ####
def getKline(stockCodeList,start_date,end_date,frequency):
    a=[]
    if type(stockCodeList) in [str]: stockCodeList=stockCodeList.split(',')
    for stockCode in stockCodeList:
        stockCode=stockCode.replace('sz.','').replace('sh.','')
        if str(stockCode)[:1]=='6': stockCode='sh.'+stockCode
        else:stockCode='sz.'+stockCode
        bs1=bs.query_stock_basic(code=stockCode)
        stockName=bs1.get_row_data()[1]
        kline=[];upNumber=0;downNumber=0;allRaise=0;upData=0;downData=0
        column="date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM"
        column="date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg"
        rs = bs.query_history_k_data_plus(stockCode,column,start_date=start_date, end_date=end_date,frequency=frequency, adjustflag="3") #frequency="d"取日k线，adjustflag="3"默认不复权
        while True & rs.next():
            k=rs.get_row_data()
            if float(k[10])>0:upNumber=upNumber+1;upData=upData+float(k[10])
            if float(k[10]) < 0: downNumber = downNumber + 1;downData=downData+float(k[10])
            allRaise=allRaise+float(k[10])
            # print(k)#打印每根K线
        a.append([stockCode,stockName,upNumber,downNumber,round(upData,2),round(downData,2),round(allRaise,2)])
        # print(stockCode,stockName,upNumber,downNumber,round(upData,2),round(downData,2),round(allRaise,2))
    a.sort(key=lambda x: x[6], reverse=True)#按涨跌幅降序
    for i in a:print(i[1],i[2],i[3],i[4],i[5],i[6])
        #### 打印结果集 ####
        # data_list = []
        # while (rs.error_code == '0') & rs.next():
        #     # 获取一条记录，将记录合并在一起
        #     data_list.append(rs.get_row_data())
            # print(rs.get_row_data())
        # result = pd.DataFrame(data_list, columns=rs.fields)
        # #### 结果集输出到csv文件 ####
        # result.to_csv("D:/history_k_data.csv", encoding="gbk", index=False)
        # print(result)
profit_list=[];a={}

shiborFlag=0
def profit():
    for i in range(4):
        rs_profit = bs.query_profit_data(code="sz.300595", year=2020, quarter=i+1)
        while True & rs_profit.next():#### 登出系统 ####
            profit=rs_profit.get_row_data()
            a[i+1]=[round(float(profit[6])*0.00000001,2)]
        rs_profit = bs.query_profit_data(code="sz.300595", year=2021, quarter=i + 1)
        while True & rs_profit.next():  #### 登出系统 ####
            profit = rs_profit.get_row_data()
            a[i + 1] = a[i + 1]+[round(float(profit[6]) * 0.00000001, 2)]
        rs_profit = bs.query_profit_data(code="sz.300595", year=2022, quarter=i + 1)
        while True & rs_profit.next():  #### 登出系统 ####
            profit = rs_profit.get_row_data()
            a[i + 1] = a[i + 1] + [round(float(profit[6]) * 0.00000001, 2)]
    print(a)
        # profit_list.append(rs_profit.get_row_data())
    if shiborFlag:
        shibor=bs.query_shibor_data(start_date="2022-01-01", end_date="2022-1-31")
        while True & shibor.next():#### 登出系统 #
            pass;#print(shibor.get_row_data())
# profit()
#医药相关
# stockList='300595','603127','300347','300759','603882','300357','300725','002821','600763','300760','300015','600436'
#新能源
stockList='002460,002812,300014,300750,603799,002709,603659,601012,002466,300450,300274,600885'
getKline(stockList,start_date='2021-11-05', end_date='2022-04-29',frequency='d')
# getKline(stockList,start_date='2021-09-24', end_date='2022-02-04',frequency='w')
# getKline(stockList,start_date='2022-02-11', end_date='2022-04-29',frequency='w')
bs.logout()