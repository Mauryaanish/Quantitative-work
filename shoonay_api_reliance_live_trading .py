import pandas as pd
import numpy as np
from datetime import datetime ,time as dt_time
from dateutil.relativedelta import *
import time
import websocket
import requests
from glob import glob
from NorenRestApiPy.NorenApi import  NorenApi

class ShoonyaApiPy(NorenApi):
    
    def __init__(self):
        NorenApi.__init__(self, host='https://api.shoonya.com/NorenWClientTP/', websocket='wss://api.shoonya.com/NorenWSTP/')        
        global api
        api = self

api = ShoonyaApiPy()
user    = '#####'
pwd     = '######'
factor2 = str(input('Entry authorize pin :'))
vc      = 'FA99130_U'
app_key = '30ab24f8ff81d3314929abee0fcb8862'
imei    = 'abc1234'

ret = api.login(userid=user, password=pwd, twoFA=factor2, vendor_code=vc, api_secret=app_key, imei=imei)

if ret is None :
    print(f'Login Failed check your authorize pin : {factor2}')

else:
    print('Login successful :')
    data_path = glob(r'D:\api/*')
    option_path = data_path[0]
    cash_path = data_path[2]
    cash_data = pd.read_csv(cash_path)
    cash_token = cash_data[cash_data['Symbol'] == 'RELIANCE'].reset_index(drop = 'First')['Token'].iloc[0]
    cash_token = str(cash_token)
    option_data = pd.read_csv(option_path)
    stock_nfo = option_data[option_data['Symbol'] == 'RELIANCE'].reset_index(drop = 'Frist')
    stock_nfo['expiry_month'] = stock_nfo['TradingSymbol'].apply(lambda x: x[10:13])
    stock_nfo['strike_price'] = stock_nfo['TradingSymbol'].apply(lambda x: x[16:])
    quantity = int(input('Entry Quantity : '))
    expiry = str(input('Entry Expiry : ')).upper()
    position_flag = 0
    
    while True :
        
        if datetime.now().time() >= dt_time(9,25) and (position_flag == 0) :
           print('Algo Start : ')
           exch = 'NSE'
           ltp = float(api.get_quotes(exchange=exch, token=cash_token)['lp'])
           base_price = 20
           atm = base_price*round(ltp/base_price)
           atm_ce = stock_nfo[(stock_nfo['expiry_month'] == expiry) & (stock_nfo['strike_price'] == str(atm)) & (stock_nfo['OptionType'] == 'CE')].reset_index(drop = 'Frist')['Token'].iloc[0]
           atm_pe = stock_nfo[(stock_nfo['expiry_month'] == expiry) & (stock_nfo['strike_price'] == str(atm)) & (stock_nfo['OptionType'] == 'PE')].reset_index(drop = 'Frist')['Token'].iloc[0]
           atm_ce = str(atm_ce)
           atm_pe = str(atm_pe)
           ce_atm_ltp = float(api.get_quotes(exchange='NFO', token=atm_ce)['lp'])
           pe_atm_ltp = float(api.get_quotes(exchange='NFO', token=atm_pe)['lp'])
           ce_pe_atm_ltp = ce_atm_ltp + pe_atm_ltp
           ce_pe_atm_ltp = ce_atm_ltp + pe_atm_ltp
           otm_ce = atm + ce_pe_atm_ltp
           otm_ce = str(base_price*round(otm_ce/base_price))
           otm_pe = atm - ce_pe_atm_ltp
           otm_pe = str(base_price*round(otm_pe/base_price))
           ce_trading_symbol = stock_nfo[(stock_nfo['expiry_month'] == expiry) & (stock_nfo['strike_price'] == otm_ce) & (stock_nfo['OptionType'] == 'CE')].reset_index(drop = 'Frist').iloc[0]['TradingSymbol']
           pe_trading_symbol = stock_nfo[(stock_nfo['expiry_month'] == expiry) & (stock_nfo['strike_price'] == otm_pe) & (stock_nfo['OptionType'] == 'PE')].reset_index(drop = 'Frist').iloc[0]['TradingSymbol']
           ce_trading_token =stock_nfo[(stock_nfo['expiry_month'] == expiry) & (stock_nfo['strike_price'] == otm_ce) & (stock_nfo['OptionType'] == 'CE')].reset_index(drop = 'Frist').iloc[0]['Token']
           pe_trading_token = stock_nfo[(stock_nfo['expiry_month'] == expiry) & (stock_nfo['strike_price'] == otm_pe) & (stock_nfo['OptionType'] == 'PE')].reset_index(drop = 'Frist').iloc[0]['Token']
    
            
           ce_sell_info = api.place_order(buy_or_sell='S', product_type='M',
                            exchange='NFO', tradingsymbol=ce_trading_symbol, 
                            quantity=quantity, discloseqty=0, price_type='LMT',  price=0,  trigger_price=0,
                             retention='DAY',  remarks='my_order_001')
    
           pe_sell_info = api.place_order(buy_or_sell='S', product_type='M',
                            exchange='NFO', tradingsymbol=pe_trading_symbol, 
                            quantity=quantity, discloseqty=0, price_type='LMT',  price=0,  trigger_price=0,
                             retention='DAY',  remarks='my_order_001')

           print(f'Call Order Details : {ce_sell_info}')
           print(f'Put Order Details : {pe_sell_info}')
            
           ce_otm_price = float(api.get_quotes(exchange='NFO', token=str(ce_trading_token))['lp'])
           pe_otm_price = float(api.get_quotes(exchange='NFO', token=str(pe_trading_token))['lp'])
    
           entry_price = ce_otm_price + pe_otm_price
           target = entry_price - 3
           stoploss = entry_price + 2.95
    
           print(f'Target :- {target} | Stoploss :- {stoploss}')
           print(f'Entry_price:- {entry_price}')
    
           while True :
               try:
                   
                   ce_option_live_price = float(api.get_quotes(exchange='NFO', token=str(ce_trading_token))['lp'])
                   pe_option_live_price = float(api.get_quotes(exchange='NFO', token=str(pe_trading_token))['lp'])
    
                   
                   spread = ce_option_live_price + pe_option_live_price
                   mtm = (entry_price - spread)*int(quantity)
    
                   print('\rLive MTM:' , round(mtm,2) , end ='' , flush=True)
    
    
                   if (spread <= target) or (spread >= stoploss):
    
                       ce_buy_info = api.place_order(buy_or_sell='B', product_type='M',
                                        exchange='NFO', tradingsymbol=ce_trading_symbol, 
                                        quantity=quantity, discloseqty=0, price_type='LMT',  price=0,  trigger_price=0,
                                         retention='DAY',  remarks='my_order_001')
    
                       pe_buy_info = api.place_order(buy_or_sell='B', product_type='M',
                                        exchange='NFO', tradingsymbol=pe_trading_symbol, 
                                        quantity=quantity, discloseqty=0, price_type='LMT',  price=0,  trigger_price=0,
                                         retention='DAY',  remarks='my_order_001')
                       position_flag =1
                       print(f'Call Buy info:- {ce_buy_info}')
                       print(f'Put Buy info:- {pe_buy_info}')
                       print('Target or Sl Hit ')
                       
                       break
    
                   elif (datetime.now().time() >= dt_time(15,15)) and (position_flag == 0):
    
                       ce_buy_info = api.place_order(buy_or_sell='B', product_type='M',
                                        exchange='NFO', tradingsymbol=ce_trading_symbol, 
                                        quantity=quantity, discloseqty=0, price_type='LMT',  price=0,  trigger_price=0,
                                         retention='DAY',  remarks='my_order_001')
    
                       pe_buy_info = api.place_order(buy_or_sell='B', product_type='M',
                                        exchange='NFO', tradingsymbol=pe_trading_symbol, 
                                        quantity=quantity, discloseqty=0, price_type='LMT',  price=0,  trigger_price=0,
                                         retention='DAY',  remarks='my_order_001')
                       position_flag =1
                       print(f'Call Buy info:- {ce_buy_info}')
                       print(f'Put Buy info:- {pe_buy_info}')
                       print('Not hit target and stoploss')
                       break
    
                   else:
                       time.sleep(0.5)
    
               except Exception as e:
                   
                   print(e)
                 
    
        elif position_flag != 0 :
            
            print('All are Position Clear')
            log = api.logout()
            print(log)
            break
    
        else:
            time.sleep(1)
            
            

