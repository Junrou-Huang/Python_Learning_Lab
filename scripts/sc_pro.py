# -*- coding: utf-8 -*-
"""
Created on Wed Jun  7 16:34:30 2023

@author: admin
"""
#%%
import numpy as np
import os
import sys
import pandas as pd

#%%
class div_protein():
    def __init__(self,file_url:str):
        self.file_url = file_url
        self.orin,self.to_num = self.split_spname_seq(self.file_url)
        self.div2 = self.get_div2(self.to_num)
        self.div = self.get_div(self.to_num)
    
    def split_spname_seq(self,file_url):
        orin = dict()#key:species name, value: remove empty seq(single  single)
        to_num = dict()
        with open(file_url,"r",encoding="utf-8") as f:
            flag = False
            for m in f:
                if flag:
                    for i in m:
                        if i == ' ':#remove empty value
                            continue
                        else:
                            orin[c_name].append(i)#save oringe data
                            to_num[c_name].append(ord(i))#transform to number
                    flag=False
                    to_num[c_name] = np.array(to_num[c_name],dtype=np.float16)#transform to numpy array
                    #replace '-' to 'np.nan'
                    j=0
                    for i in orin[c_name]:#find the positon of '-' and transform it to np.nan
                        if i == '-':
                            #print(to_num[c_name][j],type(to_num[c_name][j]))
                            to_num[c_name][j]=np.nan
                        j+=1
                #get name and create a corresponse dict
                if ">" in m:
                    c_name=m[1:-1]
                    flag = True
                    orin[c_name]=[]#create a list for dic
                    to_num[c_name]=[]
        return orin, to_num
    def get_div(self,to_num):
        div = dict()
        to_num_cp = to_num
        while len(to_num_cp) >1:
            name_list = list(to_num_cp.keys())
            name_one = name_list[0]
            #print(name_one)
            name_list.remove(name_one)
            for i in name_list:
                div[name_one+' --- '+i] = to_num[name_one]-to_num[i]
                #% save to file
                #if os.path.exists('./p_information'):
                    #pass
                #else:
                    #os.mkdir('./p_information')
                #with open('./p_information/'+self.file_url+'.csv','a') as f:
                    #f.writelines(name_one+' --- '+i)
                    #f.write('\n')
                    #for j in range(len(div[name_one+' --- '+i])):
                        #f.write(str(div[name_one+' --- '+i][j]))
                        #if j < len(div[name_one+' --- '+i])-1:
                            #f.write(',')
                    #f.write('\n')
            to_num_cp.pop(name_one)
        return div
    def get_div2(self,to_num):
        '''本函数主要用来做放回差值位点信息'''
        for i in range(len(to_num.keys())):
            sub_operation=[]
            to_num_cp = to_num
            name_list = list(to_num_cp.keys())
            name_one = name_list[i]
            name_list.remove(name_one)
            for j in name_list:#sub operation
                sub_operation.append(np.abs(to_num_cp[name_one]-to_num_cp[j]))
            total = np.array(sub_operation)#transform to numpy array and then caculate total value
            total = np.sum(total,0)#get sum
            if os.path.exists('./p_information'):
                pass
            else:
                os.mkdir('./p_information')
            with open('./p_information/'+self.file_url+'.csv','a') as f:
                f.writelines('#reference:'+name_one+':')
                f.writelines('#follow ordered seq:'+str(name_list)+':')
                f.write('\n')
                for k in range(len(self.orin[name_one])):
                    f.write(self.orin[name_one][k])
                    if k < (len(self.orin[name_one])-1):
                        f.write('\t')
                #f.write('\n')
               
                for m in sub_operation:
                    for n in range(m.shape[0]-1):
                        if np.isnan(m[n]):
                            f.write(str(m[n]))
                        else:
                            f.write(str(int(m[n])))
                        if n < (m.shape[0]-1):
                            f.write('\t')
                    f.write('\n')
                f.writelines('total sum'+'\n')
                for n in range(m.shape[0]-1):
                    if np.isnan(total[n]):
                        f.write(str(total[n]))
                    else:
                        f.write(str(int(total[n])))
                    if n < (m.shape[0]-1):
                        f.write('\t')
                f.write('\n')
        return sub_operation
                
    #def savetofile(self,div):
#%%
#a = div_protein('./OG0005105.aligned-gb.sorted.w0')
def ddd(url:list):
    for i in url:
        a = div_protein(i)
#%%
import multiprocessing

#%%
from multiprocessing import Process
if __name__ == "__main__":
    file_url = sys.argv[1]#get file path
    cpus = int(sys.argv[2])#get cpu numbers
    
    #get file url
    print('first-step split file')
    file_list = os.listdir(file_url)
    nfile = [] #need to handle files
    for k in file_list:
        if os.path.splitext(k)[1]=='.w0':
            nfile.append(k)
    #split works to every kenel
    print('tow step split works')
    process = []
    n_work_ke=int(len(nfile)/cpus)
    for k in range(cpus):
        if len(nfile) % 2 ==0: # ou shu
            process.append(Process(target=ddd,kwargs={"url":nfile[k*n_work_ke:(k+1)*n_work_ke]}))
        else: # ji shu
            if k== cpus-1:
                process.append(Process(target=ddd,kwargs={"url":nfile[k*n_work_ke:]}))
            else:
                process.append(Process(target=ddd,kwargs={"url":nfile[k*n_work_ke:(k+1)*n_work_ke]}))
    ## hung works
    for m in process:
        m.start()
        
    #print(sys.argv[:])
    print ("Sub-process(es) done.")
