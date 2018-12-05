from csv import Sniffer
import pandas as pd
import datetime
import numpy as np


class TableInformation:
    '''
    this is used to 
    '''

    @staticmethod
    def validate(date_text):
        try:

            datetime.datetime.strptime(date_text, "%Y-%m-%d")
            return True
        except:
            pass

    def __init__(self, filepath):
        self.filepath = filepath

    def show_line(self):
        try:
            with open(self.filepath, "r") as myfile:
                head = [next(myfile) for x in range(5)]
            self.line = head[1]
            return self.line
        except FileNotFoundError:
            print("OOPS! Did you type in the correct path?")

    def show_delimeter(self):
        sniffer = Sniffer()
        sniffer.preferred = [',', '|', ';', ':', '::', '||']
        dialect = sniffer.sniff(self.show_line())
        self.deli = dialect.delimiter
        return self.deli

    def show_table_info(self):
        try:
            self.length = len(self.show_line().split(self.show_delimeter()))
            print("Line : {} \nDelimeter : {} \nLength : {}\nDate : {}".format(
                self.line, self.deli, self.length, self.is_date()))
            return self.line, self.deli, self.length, self.is_date()
        except AttributeError:
            print("OOPS! ")

    def is_date(self):
        c = 0
        dte = []
        for i in self.line.split(','):
            if (self.validate(i[1:11])):
                dte.append(c)

            c += 1
        return dte

    def which_table(self, table_meta, field_meta):
        self.table_meta = table_meta
        self.field_meda = field_meta

        def dateparse(x): return pd.datetime.strptime(x, '%Y-%m-%d')
        date = self.is_date()
        data = pd.read_csv(self.filepath, nrows=5, delimiter=self.show_delimeter(
        ), date_parser=dateparse, parse_dates=date)
        probable = []
        for index, row in table_meta.iterrows():
            if (row[1] == self.length) & (row[2] == self.deli):
                probable.append(row[0].lower())
        prob = field_meta.loc[field_meta['TableName'].isin(probable)].copy()
        data_list = data.columns.tolist()
        data_list.sort()
        req = []
        for i in probable:
            temp_list = []
            tempTable = prob.loc[prob['TableName'] == str(i)].copy()
            temp_list = tempTable['fields'].tolist()
            temp_list.sort()
            if np.array_equal(temp_list, data_list) == True:
                req.append(i)
        print("The required tables are {}".format(req))
        return req
