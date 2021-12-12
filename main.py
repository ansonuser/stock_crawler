import sys
sys.path.append('..')
sys.path.append('.')
from db_utils.agent import MSAgent
from crawler.stock_info import Spider
import asyncio
import json
from fuzzywuzzy import fuzz

class Method_Implement:
    def __init__(self):
        url_dict = {}
        url_dict['company_list'] = 'ajax_t51sb01'
        url_dict['balance_sheet'] = 'ajax_t164sb03'
        url_dict['revenue_ifrs'] = 'ajax_t05st10_ifrs'
        url_dict['revenue'] = 'ajax_t05st10'
        url_dict['income_statement'] = 'ajax_t164sb04'
        url_dict['cash_flow'] = 'ajax_t164sb05'
        self.spider = Spider(url_dict)
        self.database = 'stock_db'
        self.msa = MSAgent()
        self.msa.set_db(self.database)
        self.msa.login('root', '1994anson0618')
        self.msa.set_col_dict()
        self.stock_ids = self.msa.query( self.database, "SELECT id,stock_id FROM table_basic_info", fetch=True)
        self.get_transform()
        self.collect_columnames()
       
    def collect_columnames(self):
        self.colname_mapping = {}
        tables  = ['table_cash_flow', 'table_income', 'table_balance']
        command = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}'"
        for t in tables:
            columns = self.msa.query(self.database, command.format(t), fetch=True)
            columns = [i[0] for i in columns]
            self.colname_mapping[t.replace('table','get')] = columns
        
    def collect_basic(self):
        loop = asyncio.get_event_loop()
        company_info_respone = loop.run_until_complete(self.spider.get_company_info())
        result, _ = loop.run_until_complete(self.spider.parsing_company_info_table(company_info_respone))
        rows_list = []
        if result is not None:
            for k,v in result.items():
                for row in v:
                    row = tuple(row + [k])
                    row = tuple([r.replace('\xa0','') for r in row])
                    rows_list.append(row)
            self.msa.insert(self.database, 'table_basic_info', rows_list)

    def collect_revenue(self, params):
        """
        單位千
        """
        commad = "SELECT id from table_revenue WHERE year={} and month={} and stock_id={}"\
            .format(params['year'], params['month'], params['stock_id'])
        if_exist = self.msa.query('stock_db', commad, fetch=True)
        feed_params = params.copy()
        feed_params['co_id'] = params['stock_id']
        if len(if_exist) > 0:
            result = self.spider.get_revenue(feed_params)
            if result is not None:
                self.msa.insert('stock_db', 'table_revene', 
                (self.msa.select_id(params['stock_id']), params['year'], params['month'],
                result['curr_revenue'])
                )
    def get_transform(self):
        commad = "SELECT name FROM table_season_cols"
        mapping = self.msa.query(self.database, commad, fetch=True)[-1][0]
        mapping = json.loads(mapping)
        self.mapping = {}
        for k,v in mapping.items():
            self.mapping[v] = k

        self.stock_ids = {
            str(i[0]):str(i[1]) for i in self.stock_ids
        }
        
    async def collect_season_data(self, func, params):
        feed_params = params.copy()
        feed_params['co_id'] = params['stock_id']
        del feed_params['stock_id']
        table_name = "table" + func[3:]
        command = "SELECT id from {} WHERE stock_id=%s and year=%s and season=%s".format(table_name)
        q_result = self.msa.query('stock_db', command, (params['stock_id'], params['year'], params['season']), fetch=True)
        if len(q_result) < 1:
            task = asyncio.ensure_future(getattr(self.spider, func)(feed_params))
            await task
           
            colnames = ['stock_id','year','season']
            values = []
            data = task.result()
            if data is not None:
                if len(data) > 1:
                    for i in data:
                        if len(i) >= 2:
                            i[0] = i[0].replace('\u3000','').replace(' ','').replace('\xa0','').replace(':','').replace('\t','')
                            flag = False
                            for k in self.mapping.keys():
                                if i[0] in self.mapping and self.mapping[k] in self.colname_mapping[func]:
                                    if k == i[0] :
                                        i[0] = k
                                        flag = True
                                        break                    
                            if flag:
                                if self.mapping[i[0]] not in colnames:
                                    colnames.append(self.mapping[i[0]])
                                    values.append(i[1])

                    
                        self.msa.insert('stock_db', table_name, 
                            (self.msa.select_id(params['stock_id']), params['year'], params['season'], *values), column_names=colnames)
      
    def run(self):
        self.collect_basic()

    async def update_season(self, years, seasons):
        func_list = ['get_cash_flow','get_balance','get_income']
        for stock_id in self.stock_ids.values():
            # stock_id = stock_id[0]
            for year in years:
                for season in seasons:
                    for func in func_list:
                        params = {
                            'stock_id':str(stock_id),
                            'year':year,
                            'season':season
                        }
                        task = asyncio.create_task(self.collect_season_data(func, params))
                        await task
    async def update_month(self, years, months):
        for stock_id in self.stock_ids.values():
            for year in years:
                for month in months:
                    params = {
                        'stock_id':str(stock_id),
                        'year':str(year),
                        'month':str(month)
                    }
                    task = asyncio.create_task(self.collect_revenue(params))
                    await task
                    
    async def get_older_revenue(self):
        years = [str(y-1911) for y in range(2008,2021,1)]
        months = ['%02d'%(s+1) for s in range(12)]
        await self.update_month(years, months)
    async def get_older_info(self):
        years = [str(y-1911) for y in range(2008,2021,1)]
        seasons = ['%02d'%s for s in range(1,5,1)]
        await self.update_season(years, seasons)

if __name__ == "__main__":
    MI = Method_Implement()
     # MI.collect_basic()
    asyncio.run(MI.get_older_info())
   
    
