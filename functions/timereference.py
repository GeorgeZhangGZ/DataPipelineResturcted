from datetime import datetime
import numpy as np
import math


class TimeFrame():
    def __init__(self, target_date, columns):
        self.targetdate = target_date
        self.columns = columns
        
        
    def column_index(self,col_name):
        """
        Get the index of the specified column
        """
        cols = self.columns
        sidx = np.argsort(cols)
        self.col_ind = sidx[np.searchsorted(cols,col_name,sorter=sidx)]
        return self.col_ind
  

    def date_col(self, string):
        col_pattern = [col for col in self.columns if string in col]
        self.firstcol = self.column_index(col_name = min(col_pattern))
        self.lastcol = self.column_index(col_name = max(col_pattern))
        return self.firstcol, self.lastcol
  

    def split_datestr(self,string, separator):
        self.date_string = string.split(separator)[1]
        return self.date_string
    
    
    def str2date(self, s_format):
        self.date = datetime.strptime(self.date_string, s_format)
        return self.date
    
    
    def get_first_appearance(self, *row):#, columns = []):
        first_date = self.targetdate
        for i in range(self.firstcol,self.lastcol):
            if row[i] >= 3:
                first_date_str = self.split_datestr(self.columns[i],'_')
                first_date = self.str2date('%Y-%M') 
        first_interval = self.targetdate - first_date
        return first_interval.days
              
        
    def get_target_days(self):
        earliest_datestr = self.split_datestr(self.columns[self.firstcol],'_')
        earliest_date = self.str2date('%Y-%M') 
        max_time = (self.targetdate - earliest_date)
        return max_time.days