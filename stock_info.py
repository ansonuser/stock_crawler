import requests
from collections import defaultdict
from urllib import parse
from html.parser import HTMLParser
from bs4 import BeautifulSoup
import bs4
import sys
sys.path.append('.')
from db_utils.agent import MSAgent
import pandas as pd
import re
import logging
import datetime
import asyncio
import traceback

requests.adapters.DEFAULT_RETRIES = 10

class Spider:
    def __init__(self, url_dict):
        """
        IFRs 2011 採用
        """
        self.parent = 'https://mops.twse.com.tw/mops/web/'
        self.url_dict = url_dict
        self.headers = {'Content-Type':'application/x-www-form-urlencoded'}
        self.logger = logging.getLogger()
        path = '/Users/naiminwang/Projects/VscodeProjects/Finance/logs'  + str(datetime.datetime.today().date()) + ".log"
        handler = logging.FileHandler(path, 'a', 'utf-8')
        handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(handler)
        self.feed_input =  {
            'encodeURIComponent': '1',
            'step': '1',
            'firstin': '1',
            'off': '1',
            'keyword4':'', 
            'code1':'', 
            'TYPEK2':'', 
            'checkbtn':'',
            'queryName': 'co_id',
            'inpuType': 'co_id',
            'TYPEK': 'all',
            'isnew': 'false',
            'co_id': '',
            'year': '',
            'season': ''
        }
    @staticmethod
    def encode(respond):
        true_encodeing = requests.utils.get_encodings_from_content(respond.text)
        true_text = respond.content.decode(true_encodeing[0], 'replace')
        return  true_text
    @staticmethod
    def process_bifrs(r):
        # content = str(r.content, "utf8", errors="ignore")
        soup = BeautifulSoup(r.text, features="lxml")
        tables=soup.find_all('table')
        row_list = []
        if len(tables) > 0:
            lines = tables[-1].text.splitlines()
            for i in range(len(lines)):
                v_list = []
                init = ''
                for v in lines[i]:
                    v = v.replace('\u3000','').replace('\xa0','').replace(':','')
                    if v != ' ':
                        init += v
                    else:
                        if init != '' and len(init) > 1:
                            v_list.append(init)
                            init = ''
                if len(v_list) > 0:
                    # v_list[0] = v_list[0]
                    if len(v_list) > 1 :
                        for j in range(len(v_list)):
                            if '$' in v_list[j]:
                                v_list[j] = v_list[j].replace('$','').replace(',','')
                            
                    row_list.append(v_list)
        return row_list
    @staticmethod
    def process_aifrs(r):
        # content = str(r.content, "utf8", errors="ignore")
        soup = BeautifulSoup(r.text, features="lxml")
        tables = soup.find_all('table')
        row_list = []
        if len(tables) > 0:
            rows = tables[-1].find_all('tr')
            for r in rows:
                v_list = []
                vs = r.find_all('td')
                for v in vs:
                    v = v.text.replace('\u3000','').replace('\xa0','').replace(':','')
                    v_list.append(v)
                row_list.append(v_list)
        return row_list
    async def get_company_info(self):
        params = {}
        data_mapping = defaultdict(dict)
        data_mapping['市場別']['TYPEK'] = {
            'sii':'上市',
            'otc':'上櫃',
            'rotc':'興櫃'
        }

        data_mapping['產業別']['code'] = {
            '02':'食品工業',
            '03':'塑膠工業',
            '04':'紡織纖維',
            '05':'電機機械',
            '06':'電器電纜',
            '07':'化學生技醫療',
            '08':'玻璃陶瓷',
            '10':'鋼鐵工業',
            '11':'橡膠工業',
            '13':'電子工業',
            '14':'建材營造',
            '15':'航運業',
            '16':'觀光事業',
            '17':'金融業',
            '18':'貿易百貨',
            '20':'其他',
            '21':'化學工廠',
            '22':'生技醫療業',
            '23':'油電燃氣業',
            '24':'半導體業',
            '25':'電腦及週邊設備',
            '26':'光電業',
            '27':'通信網路業',
            '28':'電子零組件業',
            '29':'電子通路業',
            '30':'資訊服務業',
            '31':'其他電子業',
            '32':'文化創意業',
            '33':'農業科技',
            '34':'電子商務',
            '80':'管理股票',
            '91':'存託憑證'
        }

        params['TYPEK'] = list(data_mapping['市場別']['TYPEK'].keys())
        params['code'] = list(data_mapping['產業別']['code'].keys())
        
        init_dict = {
            'encodeURIComponent':'1',
            'firstin':'1',
            'step':'1',
            'funcName':'t51sb01',
            'inpuType':'keyword'
        }
        result = None
        records = {}
        url = self.parent + self.url_dict['company_list']
        for t_,k in data_mapping['市場別']['TYPEK'].items():
            init_dict.update({
                'TYPEK':t_,
                'code':'',
            })
            
            r = requests.post(url, data=init_dict, headers=self.headers)
            if r.status_code == 200:
                content_ = r.text
                regex = re.compile(r"[\r\n\t]")
                content = regex.sub(" ", content_)
                soup = BeautifulSoup(content, 'html.parser')
                tables = soup.find_all('table')
            else:
                self.logger.error("Error code:{} at get_company_info and parameter is {}".format(r.status_code,
                str(init_dict)))
            records[k] = tables[1]
        if len(records) > 0:
            result = records
        return result
    async def get_revenue(self, params):#ok
        """
        {
            'co_id': '2330',
            'year': '110',
            'month': '01'
        }
        單位（千）
        """
        feed_input = {
            'encodeURIComponent': 1,
            'step': 1,
            'firstin': 1,
            'year':'',
            'off': 1,
            'keyword4':'', 
            'code1':'', 
            'TYPEK2':'', 
            'checkbtn':'', 
            'queryName': 'co_id',
            'inpuType': 'co_id',
            'isnew':'false',
            'TYPEK': 'all',
            'co_id': '',
            'month': '',
        }
        feed_input.update(params)
        if 'season' in feed_input:
            del feed_input['season']
        if int(feed_input['year']) >= 101:
            url = self.parent + self.url_dict['revenue_ifrs']
        else:
            url = self.parent + self.url_dict['revenue']
            feed_input.update({'step':0})
        r = requests.post(url, data=feed_input, headers=self.headers)
        result = None
        record = {}
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            tables = soup.find_all('table')
            if len(tables) > 0:
                record['curr_revenue'] = float(soup.find_all('table')[3].find_all('tr')[2].find_all('td')[0].text.replace(',','').split()[0])
            else:
                self.logger.info("No data")
        else:
            self.logger.error("Error code:{} at get_revenue and parameter is {}".format(r.status_code,
                str(feed_input)))
        if len(record) > 0:
            result = record
        return result
    async def get_income(self, params): #ok
        """
        params = {
            'co_id':'2330',
            'year':'108',
            'season':'01',
        }
        """
        feed_input = self.feed_input.copy()
        feed_input.update(params)
        b_ifrs = False
        result = None
        try:
            if int(feed_input['year']) < 101:
                url = self.parent + 'ajax_t05st34'
                b_ifrs = True
            else:
                url = self.parent + self.url_dict['income_statement']
            r = requests.post(url, data=feed_input, headers=self.headers)
            if r.status_code == 200:
                if b_ifrs:
                    result =  Spider.process_bifrs(r)
                else:
                    result = Spider.process_aifrs(r)
            else:
                self.logger.error("Error code:{} at get_income and parameter is {}".format(r.status_code,
                    str(feed_input)))
        except Exception as e:
            self.logger.error("[get_income] Connect failed at {}".format(feed_input))
            self.logger.error(self.error_message(e))
            
        return result
    async def get_balance(self, params): #ok
        feed_input = self.feed_input.copy()
        feed_input.update(params)
        b_ifrs = False
        if int(feed_input['year']) < 101:
            url = self.parent + 'ajax_t05st33'
            b_ifrs = True
        else:
            url = self.parent + self.url_dict['balance_sheet']
        result = None
        try:
            r = requests.post(url, data=feed_input, headers=self.headers)
            if r.status_code == 200:
                if b_ifrs:
                    result = Spider.process_bifrs(r)
                else:
                    result = Spider.process_aifrs(r)
            else:
                self.logger.error("Error code:{} at get_balance and parameter is {}".format(r.status_code,
                    str(feed_input)))
        except Exception as e:
            self.logger.error("[get_balance] Connect failed at {}".format(feed_input))
            self.logger.error(self.error_message(e))
        return result
    async def get_cash_flow(self, params):#ok
        feed_input = self.feed_input.copy()
        feed_input.update(params)
        b_ifrs = False
        if int(feed_input['year']) < 101:
            url = self.parent + 'ajax_t05st39'
            b_ifrs = True
        else:
            url = self.parent + self.url_dict['cash_flow']
        
        result = None
        try:
            r = requests.post(url, data=feed_input, headers=self.headers)
            if r.status_code == 200:
                if b_ifrs:
                    result = Spider.process_bifrs(r)
                else:
                    result = Spider.process_aifrs(r)
            else:
                self.logger.error("Error code:{} at get_cash_flow and parameter is {}".format(r.status_code,
                    str(feed_input)))
        except Exception as e:
            self.logger.error("[get_cash_flow] Connect failed at {}".format(feed_input))
            self.logger.error(self.error_message(e))
        return result
    @staticmethod
    async def parsing_company_info_table(records):
        result = None
        table_dict = {}
        # rows = []
        headers = []
        run_flag = True
        for k,table in records.items():
            tr_list = table.find_all('tr')
            row = []
            for idx,tr in enumerate(tr_list):
                
                if len(headers) != 0:
                    run_flag = False
                if not run_flag:
                    if 'class' not in tr.attrs:
                        continue
                tmp = []
                for content in tr.contents:
                    if isinstance(content, bs4.element.Tag):
                        tmp.append(content.text)
                if run_flag:
                    headers.append(tmp)
                else:
                    row.append(tmp)
                    row_len = len(tmp)
            table_dict[k] = row
        
        if len(table_dict) > 0:
            result = table_dict
        return result, headers

    @staticmethod
    def error_message(e):
        error_class = e.__class__.__name__ #取得錯誤類型
        detail = e.args[0] #取得詳細內容
        cl, exc, tb = sys.exc_info() #取得Call Stack
        lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
        fileName = lastCallStack[0] #取得發生的檔案名稱
        lineNum = lastCallStack[1] #取得發生的行號
        funcName = lastCallStack[2] #取得發生的函數名稱
        errMsg = "File \"{}\", line {}, in {}: [{}] {}".format(fileName, lineNum, funcName, error_class, detail)
        return errMsg

if __name__ == '__main__':
    url_dict = {}
    url_dict['company_list'] = 'ajax_t51sb01'
    url_dict['balance_sheet'] = 'ajax_t164sb03'
    url_dict['revenue_ifrs'] = 'ajax_t05st10_ifrs'
    url_dict['revenue'] = 'ajax_t05st10'
    url_dict['income_statement'] = 'ajax_t164sb04'
    url_dict['cash_flow'] = 'ajax_t164sb05'
    spider = Spider(url_dict)
    company_info_respone = spider.get_company_info()
    result, headers = spider.parsing_company_info_table(company_info_respone)
    print("End")


