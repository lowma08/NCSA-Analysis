#!/usr/bin/env python
# coding: utf-8

# In[20]:


import pandas as pd
import numpy as nm
import glob2
import tkinter as tk
import os
import PIL.Image
import PIL.ImageTk
from fuzzywuzzy import process
import datetime as dt

class initiate():
    def __init__(self,window):
        
        
        self.L1 = tk.Label(window, text="Path")
        self.L1.grid(row=0,column=1)
        self.logo_path = r"C:\Users\michael.lowery\Pictures\Hobsons.PNG"
        self.im = PIL.Image.open(self.logo_path)
        self.image = PIL.ImageTk.PhotoImage(self.im)
        self.pic = tk.Label(window, image = self.image).grid(row=10,column=10)
#         self.pic.image = self.image

#         L2 = tk.Label(window, text="Path2")
#         L2.grid(row=0,column=4)



        self.path_entry = tk.StringVar()
        self.E1 = tk.Entry(window,textvariable=self.path_entry,width=50)
        self.E1.grid(row=1,column=1)

#         author_input = tk.StringVar()
#         E2 = tk.Entry(window,textvariable=author_input,width=40)
#         E2.grid(row=1,column=4)


        # file_path = r"C:\Users\michael.lowery\Hobsons\Ashley Bernett - Mike\NCSA Files\Test"
        self.List1 = tk.Listbox(window,height=6,width=50,selectmode='single')
        self.List1.grid(row=2,column=1,rowspan=6)

#         List2 = tk.Listbox(window,height=6,width=80)
#         List2.grid(row=2,column=4,rowspan=6)

        self.Sd(rB1 = tk.Scrollbar(window)
        self.SB1.griow=2,column=0,rowspan=6)

        self.List1.configure(yscrollcommand=self.SB1.set)
        self.SB1.configure(command=self.List1.yview)

#         SB2 = tk.Scrollbar(window)
#         SB2.grid(row=2,column=3,rowspan=6)

#         List2.configure(yscrollcommand=SB1.set)
#         SB2.configure(command=List2.yview)

        self.B1 = tk.Button(window,text="Retrieve Files",width=20,command=self.path_file)
        self.B1.grid(row=2,column=10)
        self.B2 = tk.Button(window,text="Select",width=20,command=self.call_back)
        self.B2.grid(row=3,column=10)
        
        self.B3 = tk.Button(window,text="Accept & Close",width=20,command=self.close)
        self.B3.grid(row=4,column=10)        




    def path_file(self):
        self.path = self.path_entry.get()
        self.files = os.listdir(self.path)
        self.List1.insert(tk.END,*self.files)

    def call_back(self):

        self.idx = self.List1.curselection()
        self.selection = self.List1.get(*self.idx)

    def close(self):
        window.destroy()

#     print(List1.get(idx))
#     ret = List1.curselection()
#     value = List1[ret]
#     print(ret)
    

# r"C:\Users\michael.lowery\Hobsons\Ashley Bernett - Mike\NCSA Files\Test"




class source_analysis():

    def __init__(self,raw_data):
        raw_data = pd.read_excel(raw_data,sheet_name='Values')

        data = raw_data.copy()


        source = data['Sources']
        source_list = list(source)
        break_list = []

        def uniq_source(source_list=None):
            break_list = []
            for s in source_list:
                seg = str(s)
                seg = seg.replace('; ',';')
                spl = seg.split(';')
                break_list.append(spl)
            break_list
            final_list = []
            for t in break_list:
                string = t
                for c in string:
                    final_list.append(c) 
            
            results = nm.unique(final_list,return_counts=True)
            return results

        results_cnt = uniq_source(source_list)[1].astype(int)
        results = uniq_source(source_list)[0]
        threshold = results_cnt.sum()*.02
        table = nm.column_stack((results,results_cnt))
        results_table = pd.DataFrame(table,columns=['Source','Count'])
        results_table['Count'] = results_table['Count'].astype(int)

        filtered_results = results_table[results_table['Count'] >= threshold]
        filtered_sources = filtered_results['Source']
    #         results_table

        set_sources = ['SAT','ACT','Cappex','Carnegie','Chegg','Christian Connector','College Board','College Bound Selection','CollegeXpresss',
                       'Common App','Hobsons','NRCCUA','PC&U','RaiseMe','Royall','YouVisit','Unigo','Coalition Application','My College Guide',
                       'You Visit','Slate','TOEFL']

        test = data.groupby(by=['Naviance?']).sum()
        headers = data.columns

        fields = []
        for h in headers:
            if '20' in h and h in test.columns:
                fields.append(h)



        source = data['Sources'].dropna()
        source_fnd = []


        binary = 0
        for s in source:
            sources_split = s.split(';')
            for r in set_sources:
                if any(r in ss for ss in sources_split):
                    binary = 1
                else:
                    binary = 0 
                source_fnd.append(binary)



        test = nm.array(source_fnd)

        results = nm.hsplit(test,len(set_sources))
        shape = nm.shape(results)


        results_reshaped = nm.reshape(results,(shape[1],shape[0]))
        nm.shape(results_reshaped)
        test = pd.DataFrame(results_reshaped,columns=set_sources)





        results_df = pd.DataFrame(results_reshaped,columns=set_sources)

        # with pd.ExcelWriter("C:\\Users\\michael.lowery\\Hobsons\\Ashley Bernett - Mike\\NCSA Files\\Test\\SourceBinary.xlsx",mode='a') as file:
        #     results_df_new.to_excel(file,sheet_name='Source Binary',index=None)
        # short_df.to_csv(r'C:\Users\michael.lowery\Hobsons\Ashley Bernett - Mike\Source Analysis\Source Analysis University of North Texas_Sources.csv')

        results_df.reset_index(inplace=True,drop=True)
        funnel_data = data.dropna(subset=['Sources'])[fields]
        funnel_data.reset_index(inplace=True,drop=True)

        results_df1 = pd.concat([results_df,funnel_data],axis=1)

        funnel = []
        hobson_count = []
        no_hobson_count = []
        source_name = []

        for f in fields:
            for s in set_sources:
                H_yes_sum = results_df1[(results_df1[f]==1) & (results_df1['Hobsons']==1)][s].sum()
                H_no_sum = results_df1[(results_df1[f]==1) & (results_df1['Hobsons']==0)][s].sum()
                funnel.append(f)
                source_name.append(s)
                hobson_count.append(H_yes_sum)
                no_hobson_count.append(H_no_sum)

        final_data = nm.column_stack((funnel,source_name,hobson_count,no_hobson_count))
        final_data = pd.DataFrame(final_data,columns=['Funnel','Source','Hobsons Count','No Hobsons Count'])
        final_data
        Hob_pivot = pd.pivot_table(final_data,index=['Source'],values=['Hobsons Count'],aggfunc=nm.sum,columns=['Funnel'])
        no_Hob_pivot = pd.pivot_table(final_data,index=['Source'],values=['No Hobsons Count'],aggfunc=nm.sum,columns=['Funnel'])
        # isin Naviance, Hobson,Active
        fields = sorted(fields)
        flat_columns = ['Source'] + fields
        Hob_flattened = pd.DataFrame(Hob_pivot.to_records(),dtype=int)
        Hob_flattened.columns = flat_columns
        Hob_flattened.insert(1,column='Hobsons?',value='Yes')
        no_Hob_flattened = pd.DataFrame(no_Hob_pivot.to_records(),dtype=int)
        no_Hob_flattened.columns = flat_columns
        no_Hob_flattened.insert(1,column='Hobsons?',value='No')
        self.all_flattened = Hob_flattened.append(no_Hob_flattened)
        self.all_flattened.iloc[:,2:] = self.all_flattened.apply(pd.to_numeric, errors='coerce', axis=1)
        
        self.source_info = results_table.sort_values(by=['Count'],ascending=False)

class NCSA_Extract():
    
    def __init__(self,path=app.path):
        path = app.path
        file_path = '%s\*.%s' % (path,'xlsx')
        files = glob2.glob(file_path)

        self.school_names = []

        for file in files:

            temp = file[len(file_path)+12:-5]
            string = []
            remove_chars = ['-','*','_','.','!','@','#','$']

            for c in remove_chars:
                temp = temp.replace(c," ")

            for t in temp.split():
        #         if not t in ('-','*'):
                try:
                    int(t)
                except:
                    string.append(t)
            real = None
            for each in string:
                if real is None:
                    real = each
                else:
                    real = real + ' ' + each
            self.school_names.append(real)


        def fuzzymatch(wrongnames,correctnames,return_cnt=1,match_pcnt=80):


            result_list = []
            result_perc = []
            for n in wrongnames:
                try:
                    matches = process.extractBests(n,correctnames,score_cutoff=match_pcnt,limit=return_cnt)[0]

                except:
                    matches = []

                if len(matches)>0:
                    result_list.append(matches[0])
                    result_perc.append(matches[1])
                else:
                    result_list.append(str('No Matches Found'))
                    result_perc.append(0)

            final_results = nm.column_stack((wrongnames,result_list,result_perc))
            final_results = pd.DataFrame(final_results,columns=['Orig_Name','Match','Match %'])
            return final_results

        # This will go into UI where user pastes file path which houses list of correct client names
        school_list = pd.read_csv(r"C:\Users\michael.lowery\Hobsons\Ashley Bernett - Mike\NCSA Files\client_names.csv",encoding='iso-8859-1')
        correctnames = school_list['Account Name']


        # This will go into UI where can select cutoff %
        fuzzyresults = fuzzymatch(wrongnames=self.school_names,correctnames=correctnames)



        final_table = pd.DataFrame()
        for file, school, match in zip(files,self.school_names,fuzzyresults['Match']):
            raw_data = pd.read_excel(file,sheet_name='Values')
            data = raw_data.copy()
            headers = data.columns    

            states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
                      "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
                      "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
                      "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
                      "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

            data = data[data['State'].isin((states))]

            test = data.groupby(by=['Naviance?']).sum()
            fields = []
            for h in headers:
                if '20' in h and h in test.columns:
                    fields.append(h)


            results = test[fields]
            results.reset_index(inplace=True)
            results.insert(loc=0,column='School Name', value=school)
            results.insert(loc=0,column='Match Name', value=match)

            self.final_table = final_table.append(results,sort=False)
    
# results.to_excel(r"C:\\Users\\michael.lowery\\Hobsons\\Ashley Bernett - Mike\\NCSA Files\\Test\\Results.xlsx",index=False)


# Will add loop here to go through fields and produce pivot table for each
# pivot = pd.pivot_table(data,index=['State'],values=['2017 Enrolled'],columns=['Naviance?'],aggfunc=nm.sum,margins=True,fill_value=0)
# pivot



# Enrolled_Pivot = pivot.sort_values(by=[('2017 Enrolled','All')],ascending=False)

# flattened = pd.DataFrame(Enrolled_Pivot.to_records())

# final_table = flattened.loc[flattened['State'].isin((states))]
# final_table = final_table.append(final_table.sum(numeric_only=True).rename('Total'))
# final_table['State'].fillna('Total',inplace=True)
# final_table['% from Naviance'] = final_table["('2017 Enrolled', 'Yes')"]/final_table["('2017 Enrolled', 'All')"]

# # final_table.loc['Grand Total'] = final_table.sum(axis=0,numeric_only=True)
# final_table.to_excel(r"C:\\Users\\michael.lowery\\Hobsons\\Ashley Bernett - Mike\\NCSA Files\\Test\\Reverse Match 273 Anna Maria College - 2018_Test.xlsx",index=False)

# lr = len(final_table.loc['Total'])
# final_table.loc['Total'][lr-1]

      
        
        
        
        
window = tk.Tk()
window.title('Directory Entry')
window.geometry("500x200")
app = initiate(window)
window.mainloop()
selection = app.selection

file_path = app.path + "\\" + selection

    
# Idea - GUI selection on file we want to run source analysis on. 
SA = source_analysis(file_path)


NCSA = NCSA_Extract(app.path)

  
# p.all_flattened
# Observe differences between two lists


# source_py = data['Sources']
# source_cy = data['Sources.1']
# source_list_py = list(source_py)
# source_list_cy = list(source_cy)

# py = uniq_source(source_list_py)
# cy = uniq_source(source_list_cy)

# new_sources = nm.setdiff1d(cy,py)

# new_sources = pd.DataFrame(new_sources,columns=["New Sources"])

# save_path = '%s\%s' % (r"C:\Users\michael.lowery\Hobsons\Ashley Bernett - Mike\WIP", selection[:-5] + ' Results.xlsx')
now = dt.datetime.now()
datestamp = now.strftime('%m/%d/%y, %H:%M')
exec_date = 'This program was executed: ' + datestamp
exec_date = pd.DataFrame([exec_date],columns=['Time Stamp'])

save_path = selection[:-5] + ' Results.xlsx'
with pd.ExcelWriter(save_path, mode='w') as writer:
    SA.all_flattened.to_excel(writer, sheet_name='Source Analysis',index=None)
    SA.source_info.to_excel(writer, sheet_name='Source Info',index=None)
    NCSA.final_table.to_excel(writer, sheet_name='NCSA',index=None)
    exec_date.to_excel(writer, sheet_name='Version Info',index=None)
# If in GUI user selects both functions or NCSA, then line here will also dump those results to sep tab




# In[ ]:




