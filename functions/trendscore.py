import pandas as pd
import numpy as np
import math



class CTM():

    def column_index(self,df,start_col,end_col):
        """
        Get the index of the specified column
        """
        cols = df.columns
        sidx = np.argsort(cols)
        self.start_ind = sidx[np.searchsorted(cols,start_col,sorter=sidx)]
        self.end_ind = sidx[np.searchsorted(cols,end_col,sorter=sidx)]
        return self.start_ind, self.end_ind

    # def get_ctm(self,*row):
    #     up_movement = []
    #     down_movement = []
    #     try:
    #         for i in range(self.start_ind,self.end_ind):
    #             if row[i] <= row[i + 1]:
    #                 diff = row[i + 1] - row[i]
    #                 up_movement.append(diff)
    #             else:
    #                 diff = row[i] - row[i + 1]
    #                 down_movement.append(diff)
    #         ctm = (sum(up_movement) - sum(down_movement)) / (sum(up_movement) + sum(down_movement))
    #     except:
    #         ctm = 0.0
    #     return ctm
    def get_ctm(self,*row):
        up_movement = []
        down_movement = []
        for i in range(self.start_ind,self.end_ind):
            if row[i] <= row[i + 1]:
                diff = row[i + 1] - row[i]
                up_movement.append(diff)
            else:
                diff = row[i] - row[i + 1]
                down_movement.append(diff)
        try:
            ctm = (sum(up_movement) - sum(down_movement)) / (sum(up_movement) + sum(down_movement))
        except ZeroDivisionError:
            ctm = 0.0
        return ctm

