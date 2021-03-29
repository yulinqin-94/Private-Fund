# -*- coding: gbk -*-
# python3
import sys, os, datetime, logging, cx_Oracle, pdb
import pandas as pd
import traceback

log = logging.getLogger('root.Reporter')


class Reporter(object):
    
    def __init__(self,begDate,endDate,dte):
        self.dte  = int(dte)
        self.begDate = int(begDate)
        self.endDate  = int(endDate)
        self.conn = cx_Oracle.connect('','','')
        
        # 大产品列表
        sql = "select '' from '' where accountid not like '%SUB%' and dte>={0} and dte<={1} group by ''".format(self.begDate,self.endDate)
        PRODIDS = pd.read_sql(sql,self.conn)[''].tolist()
        prodids = []
        for prodid in PRODIDS:
            prodid = prodid.split('_')[0]
            prodids.append(prodid)
        self.PRODIDS = list(set(prodids))
        
        # 子产品列表
        sql = "select '' from '' where accountid like '%SUB%' and dte>={0} and dte<={1} group by ''".format(self.begDate,self.endDate)
        self.SUB_PRODIDS = pd.read_sql(sql,self.conn)[''].tolist()
        
        # 各项先行报表中每个产品在excel中对应的名字
        self.algo_prods = []
        self.alpha_prods = []
        self.t0_prods = []
        self.cta_prods = []
        self.rq_prods = []  
        
        # 所有先行报表中各子产品对应的sheet名无重复汇总
        self.ACTIDS = set(self.algo_prods + self.alpha_prods + self.t0_prods + self.cta_prods + self.rq_prods)

        # Alpha各子策略下产品
        self.S_188 = []
        self.S_101 = []
        self.S_186 = []
        self.S_166 = []
        
        # 先行算好的各报表路径
        self.path_algo = ''
        self.path_alpha = ''
        self.path_t0 = ''
        self.path_cta = ''
        self.path_rq = ''
        
        #self.conn.close()
    
    def _get_Algo_All(self): # 算法
        try:
            ndf = pd.DataFrame(columns=[u'产品',u'日期',u'算法初步考核收益(万)'])
            for month in range(10,13):
                filename = os.path.join(self.path_algo,'from_2020{0}01_to_2020{0}31'.format(month))  
                result=[]
                for root, dirs, files in os.walk(filename): 
                    for f in files:
                        tmp = f.split('.')
                        if tmp[-1]!='xlsx':
                            continue
                        if len(tmp[0].replace('details-','').replace('sumary-','').split('-'))!=6:
                            continue
                        if '~$' in tmp[0]:
                            continue
                        #print(f)
                        df = pd.read_excel(os.path.join(filename,f),sheet_name='profitdetail') 
                        result.append(df)                      
                df = pd.concat(result)                                             
                df = df.drop_duplicates(subset=[u'产品',u'日期'],keep='first',inplace=False)
                df = df[[u'产品',u'日期',u'考核收益(万)']]
                df.rename(columns={u'考核收益(万)': u'算法初步考核收益(万)'}, inplace=True)   
                ndf = pd.concat([ndf,df]) 
				
            return ndf
        
        except Exception as e:
            log.error('_get_Algo_All error: {}'.format(e))
            #print(traceback.format_exc())
            pdb.set_trace()
            raise
            
    def _get_Algo_All_Summary(self): # 算法
        try:
            ndf = pd.DataFrame(columns=[u'产品',u'算法考核收益(万)'])
            ndf.set_index(u'产品', inplace=True)
            
            for month in range(10,13):
                fname = 'from_2020{0}01_to_2020{0}31\productprofit2020{0}.xlsx'.format(month)
                filename = os.path.join(self.path_algo,fname)
                #print(fname)
                df = pd.read_excel(filename)
                df = df[[u'产品',u'考核收益(万)']]
                df.rename(columns={u'考核收益(万)': u'算法考核收益(万)'}, inplace=True)   
                df[u'产品'] = df[u'产品'].str.split('_',expand=True)[0]
                df = df.set_index(u'产品')
                df = df.groupby(lambda x:x, axis=0).sum()
                #pdb.set_trace()
                ndf = pd.concat([ndf,df],join='outer')
                ndf = ndf.groupby(lambda x:x, axis=0).sum()
            ndf = ndf.reset_index()
            ndf.rename(columns={'index': u'产品'}, inplace=True) 
                
            return ndf
        
        except Exception as e:
            log.error('_get_Algo_All_Summary error: {}'.format(e))
            #print(traceback.format_exc())
            pdb.set_trace()
            raise
    
        
    def _get_Algo(self): # 算法
        try:
            df = self.df_Algo_all.loc[self.df_Algo_all[u'产品']==self.actid] 
            df = df[[u'日期',u'算法初步考核收益(万)']]
            print('Algo Done')
            
            return df
        
        except Exception as e:
            log.error('_get_Algo error: {}'.format(e))
            print(traceback.format_exc())
            raise
    
    
    def _get_Algo_Summary(self): # 算法
        try:
            #pdb.set_trace()
            df = self.df_Algo_all_summary.loc[self.df_Algo_all_summary[u'产品']==self.prod]
            df = df[[u'算法考核收益(万)']]
            
            return df
        
        except Exception as e:
            log.error('_get_Algo_Summary error: {}'.format(e))
            print(traceback.format_exc())
            raise
    
        
    def _get_Alpha(self): # Alpha
        try:
            ndf1 = pd.DataFrame(columns = [u'日期',u'Alpha188总收益(万)'])
            ndf2 = pd.DataFrame(columns = [u'日期',u'Alpha101总收益(万)',u'Alpha101择时收益(万)'])
            ndf3 = pd.DataFrame(columns = [u'日期',u'Alpha186总收益(万)'])
            ndf4 = pd.DataFrame(columns = [u'日期',u'Alpha166总收益(万)'])
            
            #pdb.set_trace()
            
            if self.actid in self.S_188:
                filename = os.path.join(self.path_alpha,'188子策略成交金额2020Q4_sub.xlsx')  
                df_188 = pd.read_excel(filename,sheet_name='{}'.format(self.actid))
                df_188 = df_188[[u'日期',u'总收益']]
                ndf1[u'日期'] = df_188[u'日期']
                ndf1[u'Alpha188总收益(万)'] = df_188[u'总收益']
            
            if self.actid in self.S_186:
                #pdb.set_trace()
                filename = os.path.join(self.path_alpha,'186子策略成交金额2020Q4_sub.xlsx')  
                df_186 = pd.read_excel(filename,sheet_name='{}'.format(self.actid))
                df_186 = df_186[[u'日期',u'总收益']]
                ndf3[u'日期'] = df_186[u'日期']
                ndf3[u'Alpha186总收益(万)'] = df_186[u'总收益']
                
            if self.actid in self.S_166:
                filename = os.path.join(self.path_alpha,'166子策略成交金额2020Q4_sub.xlsx')  
                df_166 = pd.read_excel(filename,sheet_name='{}'.format(self.actid))
                df_166 = df_166[[u'日期',u'总收益']]
                ndf4[u'日期'] = df_166[u'日期']
                ndf4[u'Alpha166总收益(万)'] = df_166[u'总收益']
            
            if self.actid in self.S_101:  
                filename = os.path.join(self.path_alpha,'101子策略成交金额2020Q4_sub.xlsx')  
                df_101 = pd.read_excel(filename,sheet_name='{}'.format(self.actid))
                df_101 = df_101[[u'日期',u'总收益',u'择时收益']]
                ndf2[u'日期'] = df_101[u'日期']
                ndf2[u'Alpha101总收益(万)'] = df_101[u'总收益']
                ndf2[u'Alpha101择时收益(万)'] = df_101[u'择时收益']
                
            ndf1.set_index(u'日期', inplace=True)
            ndf2.set_index(u'日期', inplace=True)
            ndf3.set_index(u'日期', inplace=True)
            ndf4.set_index(u'日期', inplace=True)    
            ndf = pd.concat([ndf1,ndf2,ndf3,ndf4], axis=1, join='outer')
            ndf = ndf.fillna(0)
            ndf = ndf.reset_index()
            print('Alpha Done')
        
            return ndf
        
        except Exception as e:
            log.error('_get_Alpha error: {}'.format(e))
            pdb.set_trace()
            #print(traceback.format_exc())
            raise
        
        
    def _get_Alpha_TVal(self): # 
        try:
            ndf = pd.DataFrame(columns = [u'产品名称',u'188成交金额(万)',u'101成交金额(万)',u'186成交金额(万)',u'166成交金额(万)'])
            ndf1 = pd.DataFrame(columns = [u'产品名称',u'188成交金额(万)'])
            ndf2 = pd.DataFrame(columns = [u'产品名称',u'101成交金额(万)'])
            ndf3 = pd.DataFrame(columns = [u'产品名称',u'186成交金额(万)'])
            ndf4 = pd.DataFrame(columns = [u'产品名称',u'166成交金额(万)'])
            
            #pdb.set_trace()
            
            if self.prod in self.S_188:
                filename = os.path.join(self.path_alpha,'188子策略成交金额2020Q4_sub.xlsx')  
                df = pd.read_excel(filename,sheet_name=u'汇总')
                df_188 = df.loc[df[u'产品名称'] == self.prod]
                df_188 = df_188[[u'产品名称',u'188成交金额(万)']]
                ndf1[u'产品名称'] = df_188[u'产品名称']
                ndf1[u'188成交金额(万)'] = df_188[u'188成交金额(万)']
            
            if self.prod in self.S_186:
                filename = os.path.join(self.path_alpha,'186子策略成交金额2020Q4_sub.xlsx')  
                df = pd.read_excel(filename,sheet_name=u'汇总')
                df_186 = df.loc[df[u'产品名称'] == self.prod]
                df_186 = df_186[[u'产品名称',u'186成交金额(万)']]
                ndf3[u'产品名称'] = df_186[u'产品名称']
                ndf3[u'186成交金额(万)'] = df_186[u'186成交金额(万)']
                
            if self.prod in self.S_166:
                filename = os.path.join(self.path_alpha,'166子策略成交金额2020Q4_sub.xlsx')  
                df = pd.read_excel(filename,sheet_name=u'汇总')
                df_166 = df.loc[df[u'产品名称'] == self.prod]
                df_166 = df_166[[u'产品名称',u'166成交金额(万)']]
                ndf4[u'产品名称'] = df_166[u'产品名称']
                ndf4[u'166成交金额(万)'] = df_166[u'166成交金额(万)']
            
            if self.prod in self.S_101:  
                filename = os.path.join(self.path_alpha,'101子策略成交金额2020Q4_sub.xlsx')  
                df = pd.read_excel(filename,sheet_name=u'汇总')
                df_101 = df.loc[df[u'产品名称'] == self.prod]
                df_101 = df_101[[u'产品名称',u'101成交金额(万)']]
                ndf2[u'产品名称'] = df_101[u'产品名称']
                ndf2[u'101成交金额(万)'] = df_101[u'101成交金额(万)']
                
            ndf1.set_index(u'产品名称', inplace=True)
            ndf2.set_index(u'产品名称', inplace=True)
            ndf3.set_index(u'产品名称', inplace=True)
            ndf4.set_index(u'产品名称', inplace=True)    
            ndf = pd.concat([ndf1,ndf2,ndf3,ndf4], axis=1, join='outer')
            ndf = ndf.fillna(0)
            ndf = ndf.reset_index()
            ndf = ndf[[u'188成交金额(万)',u'101成交金额(万)',u'186成交金额(万)',u'166成交金额(万)']]
            #pdb.set_trace()
        
            return ndf
        
        except Exception as e:
            log.error('_get_Alpha_TVal error: {}'.format(e))
            pdb.set_trace()
            #print(traceback.format_exc())
            raise
        
        
    def _get_T0(self): # 手工T0, 机器T0
        try:
            sheetdic = {}
             
            ndf = pd.DataFrame(columns = [u'日期',u'手工T0交易收益',u'机器T0交易收益'])
            # 手工T0
            if self.actid in ():
                fname = 'T0产品交易Q4.xlsx'
                filename = os.path.join(self.path_t0,fname)
                sheetname = sheetdic[self.actid]
                df = pd.read_excel(filename, sheet_name=sheetname)
                df = df[[u'日期', u'交易收益']]
                ndf[u'日期'] = df[u'日期']
                ndf[u'手工T0交易收益'] = df[u'交易收益']
                ndf = ndf.fillna(0)
            # 机器T0
            if self.actid in ():
                fname = '机器T0产品交易Q4.xlsx'
                filename = os.path.join(self.path_t0,fname)  
                sheetname = sheetdic[self.actid]
                df = pd.read_excel(filename, sheet_name=sheetname)
                df = df[[u'日期', u'交易收益']]
                ndf[u'日期'] = df[u'日期']
                ndf[u'机器T0交易收益'] = df[u'交易收益']
                ndf = ndf.fillna(0)
    
            print('T0 Done')
            return ndf

        except Exception as e:
            log.error('_get_T0 error: {}'.format(e))
            pdb.set_trace()
            #print(traceback.format_exc())
            raise
        
        
    def _get_CTA(self): # CTA
        try:
            filename = os.path.join(self.path_cta,'CTA策略业绩归因报告2020Q4.xlsx')  
            df = pd.read_excel(filename,sheet_name='{}'.format(self.actid))
            df = df[[u'日期',u'总收益']]
            df.rename(columns={u'总收益': u'CTA收益(万)'}, inplace=True)
            print('CTA Done')
            return df
            
        except Exception as e:
            log.error('_get_CTA error: {}'.format(e))
            pdb.set_trace()
            #print(traceback.format_exc())
            raise

    def _get_RQ(self): # 融券
        try:
            #pdb.set_trace()
            filename = os.path.join(self.path_rq,'168子策略成交金额2020Q4_sub.xlsx')  
            df = pd.read_excel(filename,sheet_name='{}'.format(self.actid))
            df = df[[u'日期',u'总收益']]
            df.rename(columns={u'总收益': u'融券168总收益(万)'}, inplace=True)
            print('RQ Done')
            return df
            
        except Exception as e:
            log.error('_get_RQ error: {}'.format(e))
            pdb.set_trace()
            #print(traceback.format_exc())
            raise
    
    
    def _get_SUB_Prod_summary(self): 
        try:
            if self.actid in self.algo_prods: 
                print('Doing Algo')
                df_algo = self._get_Algo()
            else:
                df_algo = pd.DataFrame(columns = [u'日期',u'算法初步考核收益(万)'])
            df_algo.set_index(u'日期', inplace=True)
                
            if self.actid in self.alpha_prods:
                print('Doing Alpha')
                df_alpha = self._get_Alpha()
            else:
                df_alpha = pd.DataFrame(columns = [u'日期',u'Alpha188总收益(万)',u'Alpha101总收益(万)',u'Alpha101择时收益(万)',
                                                   u'Alpha186总收益(万)',u'Alpha166总收益(万)'])
            df_alpha.set_index(u'日期', inplace=True)
                
            if self.actid in self.t0_prods:
                print('Doing T0')
                df_t0 = self._get_T0()
            else:
                df_t0 = pd.DataFrame(columns = [u'日期',u'手工T0交易收益',u'机器T0交易收益'])
            df_t0.set_index(u'日期', inplace=True)
                
            if self.actid in self.cta_prods:
                print('Doing CTA')
                df_cta = self._get_CTA()
            else:
                df_cta = pd.DataFrame(columns = [u'日期',u'CTA收益(万)'])
            df_cta.set_index(u'日期', inplace=True)
            
            if self.actid in self.rq_prods:
                print('Doing RQ')
                df_rq =self._get_RQ()
            else:
                df_rq = pd.DataFrame(columns = [u'日期',u'融券168总收益(万)'])
            df_rq.set_index(u'日期', inplace=True)
            
            ndf = pd.concat([df_algo, df_alpha, df_t0, df_cta, df_rq], axis=1,join='outer')
            ndf = ndf.reset_index()
            return ndf
        
        except Exception as e:
            log.error('_get_SUB_Prod_summary error: {}'.format(e))
            pdb.set_trace()
            #print(traceback.format_exc())
            raise
    
    
    def _get_Prod_summary(self, Do_Summary): #某一大产品汇总
        try:
            df = pd.DataFrame(columns = [u'日期',u'算法初步考核收益(万)',u'Alpha188总收益(万)',u'Alpha101总收益(万)',u'Alpha101择时收益(万)',
                                          u'Alpha186总收益(万)',u'Alpha166总收益(万)',
                                      u'手工T0交易收益',u'机器T0交易收益',u'CTA收益(万)', u'融券168总收益(万)'])
            df.set_index(u'日期', inplace=True)
            acts = []
            
            for prodid in self.ACTIDS:  
                if self.prod in prodid:
                    acts.append(prodid)
            count = 1
            total = len(acts)
            for actid in acts:
                self.actid = actid
                print(count,'/',total,' Sub: ',self.actid)
                df_sub = self._get_SUB_Prod_summary()    #获取某子产品多行全日期报表
                df_sub = df_sub.fillna(0)
                df_sub.set_index(u'日期', inplace=True)
                df = pd.concat([df,df_sub],axis=1,join='outer').groupby(lambda x:x, axis=1).sum()
                count += 1
            df = df.reset_index()
            df.rename(columns={'index': u'日期'}, inplace=True)     #所有子产品多行全日期按日期加和后的报表
            
            if df.empty:
                return df
            
            if not Do_Summary:  # 后面的产品sheet
                df_sum = df[[u'日期',u'算法初步考核收益(万)',u'Alpha188总收益(万)',u'Alpha101总收益(万)',u'Alpha101择时收益(万)',
                             u'Alpha186总收益(万)',u'Alpha166总收益(万)',
                             u'CTA收益(万)',u'手工T0交易收益',u'机器T0交易收益',u'融券168总收益(万)']]
            
            else:   # 第一页汇总：每个子产品按日期全部加和成一条报表
                df_sum = pd.DataFrame(columns = [u'产品名称',u'日期',u'算法考核收益(万)',u'Alpha总账户成交金额(万)',
                        u'Alpha188总收益(万)',u'188成交金额(万)',u'188考核交易成本(万)',
                        u'Alpha101总收益(万)',u'Alpha101择时收益(万)',u'101成交金额(万)',u'101考核交易成本(万)',
                        u'Alpha186总收益(万)',u'186成交金额(万)',u'186考核交易成本(万)',
                        u'Alpha166总收益(万)',u'166成交金额(万)',u'166考核交易成本(万)',
                        u'CTA收益(万)',u'手工T0交易收益',u'机器T0交易收益',u'融券168总收益(万)'],index=[0])
                df_sum[u'产品名称'] = self.prod
                startdate = min(df[u'日期'])
                enddate = max(df[u'日期']) 
                ndf_period = str(startdate) + '-' + str(enddate)
                df_sum[u'日期']      = ndf_period
                if self.prod in []:
                    df_sum[u'算法考核收益(万)'] = df[u'算法初步考核收益(万)'].sum()
                else:
                    df_sum[u'算法考核收益(万)'] = self._get_Algo_Summary().values[0][0].round(2)
                sql = "select '' from '' where prodid like 'SUB%{0}%' and dte>={1} and dte<={2} and type=0".format(self.prod,self.begDate,self.endDate)
                df_sTval  = pd.read_sql(sql,self.conn)
                if df_sTval.values == None:
                    df_sum[u'Alpha总账户成交金额(万)'] = 0
                else:
                    df_sum[u'Alpha总账户成交金额(万)'] = (df_sTval.values[0][0]/10000).round(2)
                df_sum[u'Alpha188总收益(万)'] = df[u'Alpha188总收益(万)'].sum()
                df_sum[u'Alpha101总收益(万)'] = df[u'Alpha101总收益(万)'].sum()
                df_sum[u'Alpha101择时收益(万)'] = df[u'Alpha101择时收益(万)'].sum()
                df_sum[u'Alpha186总收益(万)'] = df[u'Alpha186总收益(万)'].sum()
                df_sum[u'Alpha166总收益(万)'] = df[u'Alpha166总收益(万)'].sum()
                df_sum[u'CTA收益(万)'] = df[u'CTA收益(万)'].sum()
                df_sum[u'手工T0交易收益'] = df[u'手工T0交易收益'].sum()
                df_sum[u'机器T0交易收益'] = df[u'机器T0交易收益'].sum()
                df_sum[u'融券168总收益(万)'] = df[u'融券168总收益(万)'].sum()
                
                if self.prod in self.alpha_prods:
                    df_TVal = self._get_Alpha_TVal()
                    df_sum[u'188成交金额(万)']     = df_TVal[u'188成交金额(万)'].values[0]
                    df_sum[u'101成交金额(万)']     = df_TVal[u'101成交金额(万)'].values[0]
                    df_sum[u'186成交金额(万)']     = df_TVal[u'186成交金额(万)'].values[0]
                    df_sum[u'166成交金额(万)']     = df_TVal[u'166成交金额(万)'].values[0]
                    df_sum[u'188考核交易成本(万)'] = (df_sum[u'Alpha188总收益(万)'].values[0] + min(-df_sum[u'188成交金额(万)'].values[0]*0.0001,
                        df_sum[u'188成交金额(万)'].values[0]/df_sum[u'Alpha总账户成交金额(万)'].values[0]*df_sum[u'算法考核收益(万)'].values[0])).round(2)
                    df_sum[u'101考核交易成本(万)'] = (df_sum[u'Alpha101总收益(万)'].values[0] + min(-df_sum[u'101成交金额(万)'].values[0]*0.0001,
                        df_sum[u'101成交金额(万)'].values[0]/df_sum[u'Alpha总账户成交金额(万)'].values[0]*df_sum[u'算法考核收益(万)'].values[0])).round(2)
                    df_sum[u'186考核交易成本(万)'] = (df_sum[u'Alpha186总收益(万)'].values[0] + min(-df_sum[u'186成交金额(万)'].values[0]*0.0001,
                        df_sum[u'186成交金额(万)'].values[0]/df_sum[u'Alpha总账户成交金额(万)'].values[0]*df_sum[u'算法考核收益(万)'].values[0])).round(2)
                    df_sum[u'166考核交易成本(万)'] = (df_sum[u'Alpha166总收益(万)'].values[0] + min(-df_sum[u'166成交金额(万)'].values[0]*0.0001,
                        df_sum[u'166成交金额(万)'].values[0]/df_sum[u'Alpha总账户成交金额(万)'].values[0]*df_sum[u'算法考核收益(万)'].values[0])).round(2)
                    
                else:
                    df_sum[u'188成交金额(万)']     = 0
                    df_sum[u'188考核交易成本(万)'] = 0
                    df_sum[u'101成交金额(万)']     = 0
                    df_sum[u'101考核交易成本(万)'] = 0
                    df_sum[u'186成交金额(万)']     = 0
                    df_sum[u'186考核交易成本(万)'] = 0
                    df_sum[u'166成交金额(万)']     = 0
                    df_sum[u'166考核交易成本(万)'] = 0
                    
                #pdb.set_trace()
                
            return df_sum
    
        except Exception as e:
            log.error('_get_Prod_summary error: {}'.format(e))
            pdb.set_trace()
            #print(traceback.format_exc())
            raise
    
    
    def _gen_report(self):
        try:
            dir0  = os.path.join('.','2020Q4Report')
            if not os.path.exists(dir0):
                os.makedirs(dir0)
            writer_All   = pd.ExcelWriter(u'%s/2020第四季度各产品业绩报告.xlsx'%(dir0))
            
            # 算法所有子产品所有日期罗列
            print('Grapping _Algo_All ')
            self.df_Algo_all = self._get_Algo_All() 
            self.df_Algo_all_summary = self._get_Algo_All_Summary()
            
            # 第一页季度汇总
            ndf = pd.DataFrame(columns = [u'产品名称',u'日期',u'算法考核收益(万)',u'Alpha总账户成交金额(万)',
                        u'Alpha188总收益(万)',u'188成交金额(万)',u'188考核交易成本(万)',
                        u'Alpha101总收益(万)',u'Alpha101择时收益(万)',u'101成交金额(万)',u'101考核交易成本(万)',
                        u'Alpha186总收益(万)',u'186成交金额(万)',u'186考核交易成本(万)',
                        u'Alpha166总收益(万)',u'166成交金额(万)',u'166考核交易成本(万)',
                        u'CTA收益(万)',u'手工T0交易收益',u'机器T0交易收益',u'融券168总收益(万)'])
            count = 1
            total = len(self.PRODIDS)
            for prod in self.PRODIDS:
                self.prod = prod
                print(count,'/',total,' Generating Summary: ',prod)
                Do_Summary = True
                df = self._get_Prod_summary(Do_Summary)
                #print(df)
                if not df.empty:
                    ndf = pd.concat([ndf,df])
                else:
                    print('No %s'%self.prod)
                count += 1
            ndf.to_excel(writer_All,sheet_name=u'产品业绩汇总',index=False)
            
            #分产品全季度每日展示
            count = 1
            total = len(self.PRODIDS)
            for prod in self.PRODIDS:
                self.prod = prod
                print(count,'/',total,' Generating Product: ',prod)
                Do_Summary = False
                df = self._get_Prod_summary(Do_Summary)
                if not df.empty:
                    df.to_excel(writer_All,sheet_name='%s'%self.prod,index=False)
                else:
                    print('No %s'%self.prod)
                count += 1
            #pdb.set_trace()
            writer_All.save()
            print('All Done !!!')
            
        except Exception as e:
            log.error('_gen_report error: {}'.format(e))
            pdb.set_trace()
            #print(traceback.format_exc())
            raise

    def doReport(self):
        try:
            self._gen_report()
            return 0
        except Exception as e:
            log.error('doReport error: {}'.format(e))
            return -1
        finally:
            self.conn.close()


if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)

    begDate = 20201001  #int(sys.argv[1])
    endDate = 20201231  #int(sys.argv[2])
    dte = int(datetime.datetime.today().strftime('%Y%m%d'))
    
    
    Reporter(begDate,endDate,dte).doReport()