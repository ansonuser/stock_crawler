from numpy import result_type
import requests
import ast
import pandas as pd
class SP:
    def __init__(self):
        self.price_url = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={}&stockNo={}&_=1619614931299'
        self.vol_url = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={}&stockNo={}'# &_=1625492379206'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language':'en-us,en;q=0.5',
            'Accept-Charset':'ISO-8859-1,utf-8;q=0.7,*;q=0.7'
        }
 
    
    def get_price(self, date, stock):
        result = requests.get(self.price_url.format(date, stock), headers=self.headers)
        if result.status_code == 200:
            try:
                return ast.literal_eval(result.text)
            except:
                return {}
        else:
            return {}

    def get_volumn(self, date, stock):
        rs = requests.session()
        result = rs.get(self.vol_url.format(date, stock), verify=False, headers=self.headers)
        if result.status_code == 200:
            try:
                return ast.literal_eval(result.text)
            except:
                return {}
        else:
            return {}

if __name__ == '__main__':
    sp = SP()
    # result = sp.get_price('20210427', '2330')
    result = sp.get_volumn('20210427', '2330')
    data = pd.DataFrame(result['data'], columns = result['fields'])
    print(data)
    print("end")

    