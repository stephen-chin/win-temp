# -*- coding: utf-8 -*- 한글 인코딩 에러를 방지하기 위한 부분
import pandas
import numpy
import scipy.stats
import datetime

from PyQt5 import QtWidgets, QtCore, QtGui
from pandas.tseries.offsets import BDay

from database import *
from dates import *
from multipledispatch import dispatch


class BridgeInterface:
    def __init__(self):
        super(BridgeInterface, self).__init__()

        self.sector_portfolio_fromdate = ''
        self.sector_portfolio_todate = ''
        self.sector_portfolio_pillar = ''

        self.sector_summary_fromdate = ''
        self.sector_summary_todate = ''
        self.sector_summary_pillar = ''
        self.combobox_text = ''

        self.security_portfolio_fromdate = ''
        self.security_portfolio_todate = ''
        self.security_portfolio_pillar = ''

        self.security_summary_fromdate = ''
        self.security_summary_todate = ''
        self.security_summary_code = ''

    def get_sector_portfolio_fromdate(self, fromdate):
        self.sector_portfolio_fromdate = fromdate

    def get_sector_portfolio_todate(self, todate):
        self.sector_portfolio_todate = todate

    def get_sector_portfolio_pillar(self, pillar):
        self.sector_portfolio_pillar = pillar

    def set_sector_portfolio_fromdate(self):
        return self.sector_portfolio_fromdate

    def set_sector_portfolio_todate(self):
        return self.sector_portfolio_todate

    def set_sector_portfolio_pillar(self):
        return self.sector_portfolio_pillar

    def get_sector_summary_fromdate(self, fromdate):
        self.sector_summary_fromdate = fromdate

    def get_sector_summary_todate(self, todate):
        self.sector_summary_todate = todate

    def get_sector_summary_pillar(self, pillar):
        self.sector_summary_pillar = pillar

    def get_sector_summary_sector(self, sector):
        self.combobox_text = sector

    def set_sector_summary_fromdate(self):
        return self.sector_summary_fromdate

    def set_sector_summary_todate(self):
        return self.sector_summary_todate

    def set_sector_summary_pillar(self):
        return self.sector_summary_pillar

    def get_security_portfolio_fromdate(self, fromdate):
        self.security_portfolio_fromdate = fromdate

    def get_security_portfolio_todate(self, todate):
        self.security_portfolio_todate = todate

    def get_security_portfolio_pillar(self, pillar):
        self.security_portfolio_pillar = pillar

    def set_security_portfolio_fromdate(self):
        return self.security_portfolio_fromdate

    def set_security_portfolio_todate(self):
        return self.security_portfolio_todate

    def set_security_portfolio_pillar(self):
        return self.security_portfolio_pillar

    def get_security_summary_fromdate(self, fromdate):
        self.security_summary_fromdate = fromdate

    def get_security_summary_todate(self, todate):
        self.security_summary_todate = todate

    def get_security_summary_code(self, code):
        self.security_summary_code = code

    def set_security_summary_fromdate(self):
        return self.security_summary_fromdate

    def set_security_summary_todate(self):
        return self.security_summary_todate

    def set_security_summary_code(self):
        return self.security_summary_code

    def import_interface_field_code(self, fromdate, todate):
        self.field_fromdate_code = fromdate
        self.field_todate_code = todate

    def import_interface_parameter_code(self, code):
        self.code = code

    def import_interface_worksheet_code(self, day, week, month, quarter, year):
        # self.table1_worksheet = value  # code by value
        # self.table2_worksheet = volume  # code by volume
        self.day_worksheet = day
        self.week_worksheet = week
        self.month_worksheet = month
        self.quarter_worksheet = quarter
        self.year_worksheet = year

    def import_interface_chart_code(self, fromdate, todate, pillar, investor, plot1, plot2, plot111, plot112):
        self.field_fromdate_chart = fromdate
        self.field_todate_chart = todate
        self.field_pillar_chart = pillar
        self.investor_chart = investor
        self.plot_code_default_close = plot111
        self.plot_code_default_holding = plot112
        self.plot = plot1
        self.plot_twinx = plot2

    def import_interface_chart2(self, chart1, chart2, chart3):
        self.chart2_1 = chart1
        self.chart2_2 = chart2
        self.chart2_3 = chart3


class BridgeDates(BridgeInterface):
    def __init__(self):
        super(BridgeDates, self).__init__()
        cob = datetime.datetime.now() - pandas.tseries.offsets.BDay(1)
        self.cob = cob.strftime('%Y%m%d')
        # self.fromdate_code = ''
        # self.todate_code = ''

    # def CalcDatesCode(self):
    #     if self.security_summary_todate == '':
    #         self.todate_code = str(self.cob)
    #     else:
    #         self.todate_code = self.field_todate_code.text()
    #
    #     if self.field_fromdate_code.text() == "":
    #         pillar = '10Y'
    #         temp = self.date.convert_to_day(pillar)
    #         self.fromdate_code = (datetime.datetime.strptime(self.todate_code, '%Y%m%d').date() - pandas.tseries.offsets.BDay(
    #             temp)).strftime('%Y%m%d')
    #     else:
    #         self.fromdate_code = self.field_fromdate_code.text()
    #
    #     self.UpdateDateField()

    def CalcDatesChart(self):
        pass

    # def UpdateDateField(self):
    #     self.field_fromdate_code.setText(self.fromdate_code)
    #     self.field_todate_code.setText(self.todate_code)


class BridgeDatabase(BridgeDates):
    def __init__(self):
        super(BridgeDatabase, self).__init__()
        self.masterstockinfo = TableMasterStockInfo()
        self.opt10051_1 = TableOpt10051_1()
        self.opt10051_3 = TableOpt10051_3()
        self.opt10060_1 = TableOpt10060_1()
        self.opt10060_2 = TableOpt10060_2()
        self.opt10081 = TableOpt10081()
        self.logic_code = LogicCode()

    @dispatch(str, str)
    def query_table_opt10051_1(self, fromdate, todate):
        self.opt10051_1_table = self.opt10051_1.get_table(fromdate, todate)

    @dispatch(str, str, str)
    def query_table_opt10051_1(self, fromdate, todate, sectorname):
        self.opt10051_1_table = self.opt10051_1.get_table(fromdate, todate, sectorname)

    def query_table_opt10051_3(self, fromdate, todate):
        self.opt10051_3_table = self.opt10051_3.get_table(fromdate, todate)

    def query_table_opt10060_1(self):  # replace get_opt10060_1_table
        # self.CalcDatesCode()
        self.opt10060_1_table = self.opt10060_1.get_table(self.security_summary_code,
                                                          self.security_summary_fromdate,
                                                          self.security_summary_todate)

    def query_table_opt10060_2(self):
        # self.CalcDatesCode()
        self.opt10060_2_table = self.opt10060_2.get_table(self.security_summary_code,
                                                          self.security_summary_fromdate,
                                                          self.security_summary_todate)

    def query_table_opt10081(self):
        # self.CalcDatesCode()
        self.opt10081_table = self.opt10081.get_table(self.security_summary_code,
                                                      self.security_summary_fromdate,
                                                      self.security_summary_todate)

    def get_codename(self):
        return self.masterstockinfo.get_name_given_code(self.code)


class Dataframe(BridgeDatabase):
    def __init__(self):
        super(Dataframe, self).__init__()

    def preprocess_opt10051_1(self):
        self.data_end = max(self.opt10051_1_table['individual'].loc[self.opt10051_1_table['individual'] != 0].index)

    def preprocess_opt10051_3(self):
        self.data_end = max(self.opt10051_1_table['individual'].loc[self.opt10051_3_table['individual'] != 0].index)

    def preprocess_opt10060_1(self):
        self.data_end = max(self.opt10060_1_table['individual'].loc[self.opt10060_1_table['individual'] != 0].index)
        # TODO: Improve logic - Below can cause index error issue as the fromdate either from input or default (2000101) may not match one of date in database
        # self.data_end = self.opt10060_1_table['individual'].loc[self.opt10060_1_table['date'] == self.field_todate_code.text()].index.tolist()[0]

    def preprocess_opt10060_2(self):
        # TODO: is this required?
        pass

    def create_sector_kospi_dataframe_by_value(self):
        self.individuals_sector_kospi = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['individual']})
        self.foreigner_sector_kospi = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['foreigner']})
        self.broker_sector_kospi = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['broker']})
        self.insurance_sector_kospi = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['insurance']})
        self.fund_sector_by_kospi = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['fund']})
        self.misc_sector_kospi = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['misc']})
        self.bank_sector_kospi = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['bank']})
        self.pension_sector_kospi = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['pension']})
        self.privateequity_sector_kospi = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['privateequity']})
        self.sovereign_sector_kospi = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['sovereign']})
        self.corporate_sector_kospi = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['corporate']})
        self.expatriate_sector_kospi = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['expatriate']})
        self.institution_sector_kospi = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['institution']})

        self.investor_list_kospi_value = (self.individuals_sector_kospi, self.foreigner_sector_kospi, self.broker_sector_kospi, self.insurance_sector_kospi, self.fund_sector_by_kospi,
                               self.misc_sector_kospi, self.bank_sector_kospi, self.pension_sector_kospi, self.privateequity_sector_kospi, self.sovereign_sector_kospi,
                               self.corporate_sector_kospi, self.expatriate_sector_kospi, self.institution_sector_kospi)

    def create_sector_kosdaq_dataframe_by_value(self):
        self.individuals_sector_kosdaq = pandas.DataFrame({'Date': self.opt10051_3_table['date'], 'Close': self.opt10051_3_table['close'], 'Pos': self.opt10051_3_table['individual']})
        self.foreigner_sector_kosdaq = pandas.DataFrame({'Date': self.opt10051_3_table['date'], 'Close': self.opt10051_3_table['close'], 'Pos': self.opt10051_3_table['foreigner']})
        self.broker_sector_kosdaq = pandas.DataFrame({'Date': self.opt10051_3_table['date'], 'Close': self.opt10051_3_table['close'], 'Pos': self.opt10051_3_table['broker']})
        self.insurance_sector_kosdaq = pandas.DataFrame({'Date': self.opt10051_3_table['date'], 'Close': self.opt10051_3_table['close'], 'Pos': self.opt10051_3_table['insurance']})
        self.fund_sector_by_kosdaq = pandas.DataFrame({'Date': self.opt10051_3_table['date'], 'Close': self.opt10051_3_table['close'], 'Pos': self.opt10051_3_table['fund']})
        self.misc_sector_kosdaq = pandas.DataFrame({'Date': self.opt10051_3_table['date'], 'Close': self.opt10051_3_table['close'], 'Pos': self.opt10051_3_table['misc']})
        self.bank_sector_kosdaq = pandas.DataFrame({'Date': self.opt10051_3_table['date'], 'Close': self.opt10051_3_table['close'], 'Pos': self.opt10051_3_table['bank']})
        self.pension_sector_kosdaq = pandas.DataFrame({'Date': self.opt10051_3_table['date'], 'Close': self.opt10051_3_table['close'], 'Pos': self.opt10051_3_table['pension']})
        self.privateequity_sector_kosdaq = pandas.DataFrame({'Date': self.opt10051_3_table['date'], 'Close': self.opt10051_3_table['close'], 'Pos': self.opt10051_3_table['privateequity']})
        self.sovereign_sector_kosdaq = pandas.DataFrame({'Date': self.opt10051_3_table['date'], 'Close': self.opt10051_3_table['close'], 'Pos': self.opt10051_3_table['sovereign']})
        self.corporate_sector_kosdaq = pandas.DataFrame({'Date': self.opt10051_3_table['date'], 'Close': self.opt10051_3_table['close'], 'Pos': self.opt10051_3_table['corporate']})
        self.expatriate_sector_kosdaq = pandas.DataFrame({'Date': self.opt10051_3_table['date'], 'Close': self.opt10051_3_table['close'], 'Pos': self.opt10051_3_table['expatriate']})
        self.institution_sector_kosdaq = pandas.DataFrame({'Date': self.opt10051_3_table['date'], 'Close': self.opt10051_3_table['close'], 'Pos': self.opt10051_3_table['institution']})

        self.investor_list_kosdaq_value = (self.individuals_sector_kosdaq, self.foreigner_sector_kosdaq, self.broker_sector_kosdaq, self.insurance_sector_kosdaq, self.fund_sector_by_kosdaq,
                               self.misc_sector_kosdaq, self.bank_sector_kosdaq, self.pension_sector_kosdaq, self.privateequity_sector_kosdaq, self.sovereign_sector_kosdaq,
                               self.corporate_sector_kosdaq, self.expatriate_sector_kosdaq, self.institution_sector_kosdaq)

    def create_dataframe_price(self):
        self.ohlc = pandas.DataFrame({'Date': self.opt10081_table['date'], 'Volume': self.opt10081_table['volume'],
                                      'Value': self.opt10081_table['value'], 'Open': self.opt10081_table['open'],
                                      'High': self.opt10081_table['high'], 'Low': self.opt10081_table['low'],
                                      'Close': self.opt10081_table['price']})


class StrategyQuery(BridgeDatabase):
    DEFAULT_PILLAR = '3M'

    def __init__(self):
        super(StrategyQuery, self).__init__()
        self.date = Dates()

    def calculate_security_portfolio(self, worksheet1, worksheet2):
        print('<<quantitative/StrategyQuery/calculate_security_portfolio<<')
        self.preprocess_security_portfolio()
        self.publish_security_portfolio_kospi(worksheet1)
        self.publish_security_portfolio_kosdaq(worksheet2)
        print('>>quantitative/StrategyQuery/calculate_security_portfolio>>')

    def preprocess_security_portfolio(self):
        if not self.security_portfolio_todate:
            self.security_portfolio_todate = self.cob

        if self.security_portfolio_pillar:
            self.security_portfolio_fromdate = (datetime.datetime.strptime(self.security_portfolio_todate, '%Y%m%d').date()
                                            - pandas.tseries.offsets.BDay(
                        self.date.convert_to_day(self.security_portfolio_pillar))).strftime('%Y%m%d')
        else:
            if self.security_portfolio_fromdate:
                self.security_portfolio_pillar = ''
            else:
                self.security_portfolio_fromdate = (datetime.datetime.strptime(self.security_portfolio_todate, '%Y%m%d').date()
                                            - pandas.tseries.offsets.BDay(
                        self.date.convert_to_day(self.DEFAULT_PILLAR))).strftime('%Y%m%d')

    def publish_security_portfolio_kospi(self, worksheet):
        data = self.logic_code.code_pick_1(self.security_portfolio_fromdate, self.security_portfolio_todate)
        nrow = len(data.index)
        ncol = len(data.columns)

        worksheet.setRowCount(nrow)
        worksheet.setColumnCount(ncol)
        
        self.fill_worksheet_security_portfolio(data, worksheet, 2)
        worksheet.setHorizontalHeaderLabels(list(data.columns.values))
        worksheet.setAlternatingRowColors(True)
        worksheet.setStyleSheet("background-color:rgb(255, 255, 204); alternate-background-color:rgb(229, 255, 204);")
        worksheet.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        worksheet.resizeColumnsToContents()
        worksheet.resizeRowsToContents()

    def publish_security_portfolio_kosdaq(self, worksheet):
        data = self.logic_code.code_pick_2(self.security_portfolio_fromdate, self.security_portfolio_todate)
        nrow = len(data.index)
        ncol = len(data.columns)

        worksheet.setRowCount(nrow)
        worksheet.setColumnCount(ncol)
        
        self.fill_worksheet_security_portfolio(data, worksheet, 2)
        worksheet.setHorizontalHeaderLabels(list(data.columns.values))
        worksheet.setAlternatingRowColors(True)
        worksheet.setStyleSheet("background-color:rgb(255, 255, 204); alternate-background-color:rgb(229, 255, 204);")
        worksheet.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        worksheet.resizeColumnsToContents()
        worksheet.resizeRowsToContents()

    def fill_worksheet_security_portfolio(self, data, worksheet, colindex):
        for i in range(len(data.index)):
            for j in range(len(data.columns)):
                item = QtWidgets.QTableWidgetItem()
                item.setTextAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignRight)
                cell = data.iat[i, j]

                if j > colindex and cell >= 0:
                    # cell = format(cell, ',')
                    item.setForeground(QtGui.QColor(0, 0, 255))
                    item.setData(QtCore.Qt.DisplayRole, int(cell))
                elif j > colindex and cell < 0:
                    # cell = format(cell, ',')
                    item.setForeground(QtGui.QColor(255, 0, 0))
                    item.setData(QtCore.Qt.DisplayRole, int(cell))
                else:
                    item.setForeground(QtGui.QColor(64, 64, 64))
                    item.setData(QtCore.Qt.DisplayRole, str(cell))

                worksheet.setItem(i, j, item)


class Strategy(Dataframe):
    PILLAR_DEFAULT = '10Y'
    SECURITY_PILLAR_DEFAULT = '10Y'

    def __init__(self):
        print('\tquantitative/Strategy/init')
        super(Strategy, self).__init__()

        self.date = Dates()

        self.investor_label_kr = ('개인', '외국인', '금융투자', '보험', '투신', '기타금융', '은행', '연기금', '사모펀드', '국가', '기타법인', '내외국인', '기관계')
        self.investor_label_en = ('individual', 'foreigner', 'institution', 'broker', 'insurance', 'fund', 'misc', 'bank',
                                  'pension', 'privateequity', 'sovereign', 'corporate', 'expatriate', 'institution')
        self.sector_kospi = ('종합(KOSPI)', '대형주', '중형주', '소형주', '음식료업', '섬유의복', '종이목재', '화학', '의약품', '비금속광물',
                             '철강금속', '기계', '전기전자', '의료정밀', '운수장비', '유통업', '전기가스업', '건설업', '운수창고', '통신업',
                             '금융업', '은행', '증권', '보험', '서비스업', '제조업')
        self.day_pillar = ['1D', '2D', '3D', '4D', '5D', '6D', '7D', '8D', '9D', '10D', '11D', '12D', '13D', '14D',
                           '15D', '16D', '17D', '18D', '19D', '20D']
        self.week_pillar = ['1W', '2W', '3W', '4W', '5W', '6W', '7W', '8W', '9W', '10W', '11W', '12W', '13W', '14W',
                            '15W', '16W', '17W', '18W', '19W', '20W', '21W', '22W', '23W', '24W', '25W', '26W', '27W',
                            '28W', '29W', '30W', '31W', '32W']
        self.month_pillar = ['1M', '2M', '3M', '4M', '5M', '6M', '7M', '8M', '9M', '10M', '11M', '12M',
                             '13M', '14M', '15M', '16M', '17M', '18M', '19M', '20M', '21M', '22M', '23M', '24M', '25M',
                             '26M', '27M', '28M', '29M', '30M', '31M', '32M', '33M', '34M', '35M', '36M', '37M', '38M',
                             '39M', '40M', '41M', '42M', '43M', '44M', '45M', '46M', '47M', '48M', '49M', '50M', '51M',
                             '52M', '53M', '54M', '55M', '56M', '57M', '58M', '59M', '60M']
        self.quarter_pillar = ['1Q', '2Q', '3Q', '4Q', '5Q', '6Q', '7Q', '8Q', '9Q', '10Q', '11Q', '12Q']
        self.year_pillar = ['1Y', '2Y', '3Y', '4Y', '5Y', '6Y', '7Y', '8Y', '9Y', '10Y']

    def get_db_path(self):

        return self.path

    def calculate_sector_portfolio(self, worksheet1, worksheet2):
        print('<<quantitative/Strategy/calculate_sector_portfolio<<')
        self.preprocess_sector_portfolio()
        self.query_table_opt10051_1(self.sector_portfolio_fromdate, self.sector_portfolio_todate)
        self.query_table_opt10051_3(self.sector_portfolio_fromdate, self.sector_portfolio_todate)
        self.publish_tab_sector_portfolio_kospi(worksheet1)
        self.publish_tab_sector_portfolio_kosdaq(worksheet2)
        print('>>quantitative/Strategy/calculate_sector_portfolio>>')

    def preprocess_sector_portfolio(self):
        if not self.sector_portfolio_todate:
            self.sector_portfolio_todate = self.cob

        if self.sector_portfolio_pillar:
            self.sector_portfolio_fromdate = (datetime.datetime.strptime(self.sector_portfolio_todate, '%Y%m%d').date()
                                            - pandas.tseries.offsets.BDay(
                        self.date.convert_to_day(self.sector_portfolio_pillar))).strftime('%Y%m%d')
        else:
            if self.sector_portfolio_fromdate:
                self.sector_portfolio_pillar = ''
            else:
                self.sector_portfolio_fromdate = (datetime.datetime.strptime(self.sector_portfolio_todate, '%Y%m%d').date()
                                            - pandas.tseries.offsets.BDay(
                        self.date.convert_to_day(self.PILLAR_DEFAULT))).strftime('%Y%m%d')

    def publish_tab_sector_portfolio_kospi(self, worksheet):
        print('\tquantitative/Strategy/publish_tab_sector_portfolio_kospi')
        data = self.opt10051_1.get_summary(self.sector_portfolio_fromdate, self.sector_portfolio_todate)
        nrow = len(data.index)
        ncol = len(data.columns)

        worksheet.setRowCount(nrow)
        worksheet.setColumnCount(ncol)
        self.fill_worksheet_sector_portfolio(data, worksheet)
        worksheet.setHorizontalHeaderLabels(list(data.columns.values))
        worksheet.setAlternatingRowColors(True)
        worksheet.setStyleSheet("background-color:rgb(255, 255, 204); alternate-background-color:rgb(229, 255, 204);")
        worksheet.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        worksheet.resizeColumnsToContents()
        worksheet.resizeRowsToContents()

    def publish_tab_sector_portfolio_kosdaq(self, worksheet):
        print('\tquantitative/Strategy/publish_tab_sector_portfolio_kosdaq')
        data = self.opt10051_3.get_summary(self.sector_portfolio_fromdate, self.sector_portfolio_todate)
        nrow = len(data.index)
        ncol = len(data.columns)

        worksheet.setRowCount(nrow)
        worksheet.setColumnCount(ncol)
        self.fill_worksheet_sector_portfolio(data, worksheet)
        worksheet.setHorizontalHeaderLabels(list(data.columns.values))
        worksheet.setAlternatingRowColors(True)
        worksheet.setStyleSheet("background-color:rgb(255, 255, 204); alternate-background-color:rgb(229, 255, 204);")
        worksheet.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        worksheet.resizeColumnsToContents()
        worksheet.resizeRowsToContents()

    def fill_worksheet_sector_portfolio(self, data, worksheet):
        for i in range(len(data.index)):
            for j in range(len(data.columns)):
                item = QtWidgets.QTableWidgetItem()
                item.setTextAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignRight)
                cell = data.iat[i, j]  # cell = dataframe.iat(i, j)

                if j > 1 and cell >= 0:
                    # cell = format(cell, ',')
                    item.setForeground(QtGui.QColor(0, 0, 255))
                    item.setData(QtCore.Qt.DisplayRole, int(cell))
                elif j > 1 and cell < 0:
                    # cell = format(cell, ',')
                    item.setForeground(QtGui.QColor(255, 0, 0))
                    item.setData(QtCore.Qt.DisplayRole, int(cell))
                else:
                    item.setForeground(QtGui.QColor(64, 64, 64))
                    item.setData(QtCore.Qt.DisplayRole, str(cell))

                worksheet.setItem(i, j, item)

    def calculate_sector_summary(self, worksheet):
        print('<<quantitative/Strategy/calculate_sector_summary<<')
        self.preprocess_sector_summary()
        self.query_table_opt10051_1(self.sector_summary_fromdate, self.sector_summary_todate, self.combobox_text)
        self.preprocess_opt10051_1()
        self.create_sector_summary_dataframe()
        self.calculate_sector_summary_bucket()
        self.publish_tab_sector_summary(worksheet)
        print('>>quantitative/Strategy/calculate_sector_summary>>')

    def preprocess_sector_summary(self):
        if not self.sector_summary_todate:
            self.sector_summary_todate = self.cob

        if self.sector_summary_pillar:
            self.sector_summary_fromdate = (datetime.datetime.strptime(self.sector_summary_todate, '%Y%m%d').date()
                                            - pandas.tseries.offsets.BDay(
                        self.date.convert_to_day(self.sector_summary_pillar))).strftime('%Y%m%d')
        else:
            if self.sector_summary_fromdate:
                self.sector_summary_pillar = ''
            else:
                self.sector_summary_fromdate = (datetime.datetime.strptime(self.sector_summary_todate, '%Y%m%d').date()
                                            - pandas.tseries.offsets.BDay(
                        self.date.convert_to_day(self.PILLAR_DEFAULT))).strftime('%Y%m%d')

    def create_sector_summary_dataframe(self):
        self.individuals_sector = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['individual']})
        self.foreigner_sector = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['foreigner']})
        self.broker_sector = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['broker']})
        self.insurance_sector = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['insurance']})
        self.fund_sector = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['fund']})
        self.misc_sector = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['misc']})
        self.bank_sector = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['bank']})
        self.pension_sector = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['pension']})
        self.privateequity_sector = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['privateequity']})
        self.sovereign_sector = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['sovereign']})
        self.corporate_sector = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['corporate']})
        # self.expatriate_sector = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['expatriate']})
        self.institution_sector = pandas.DataFrame({'Date': self.opt10051_1_table['date'], 'Close': self.opt10051_1_table['close'], 'Pos': self.opt10051_1_table['institution']})

        self.investor_list_sector_value = (self.individuals_sector, self.foreigner_sector, self.broker_sector, self.insurance_sector, self.fund_sector,
                               self.misc_sector, self.bank_sector, self.pension_sector, self.privateequity_sector, self.sovereign_sector,
                               self.corporate_sector, self.institution_sector)

    def calculate_sector_summary_bucket(self):
        self.pillar = []
        self.day_sector = self.create_forward_table(self.opt10051_1_table, self.day_pillar)
        self.pillar = []
        self.week_sector = self.create_forward_table(self.opt10051_1_table, self.week_pillar)
        self.pillar = []
        self.month_sector = self.create_forward_table(self.opt10051_1_table, self.month_pillar)
        self.pillar = []
        self.quarter_sector = self.create_forward_table(self.opt10051_1_table, self.quarter_pillar)
        self.pillar = []
        self.year_sector = self.create_forward_table(self.opt10051_1_table, self.year_pillar)

    def publish_tab_sector_summary(self, worksheet):
        frames = [self.day_sector[0:5], self.week_sector[0:4], self.month_sector[0:3], self.quarter_sector[0:4],
                  self.year_sector[0:]]
        dataframe = pandas.concat(frames)
        df_nrow = len(dataframe.index)
        df_ncol = len(dataframe.columns) - 2  # TODO: temp fix to exclude expatriate and institution
        extra_row = 0
        worksheet.setRowCount(df_nrow + extra_row)
        worksheet.setColumnCount(df_ncol)

        for i in range(df_nrow):
            for j in range(df_ncol):
                cell = dataframe.iat[i, j]
                item = QtWidgets.QTableWidgetItem()
                item.setTextAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignRight)

                if j > 2 and cell >= 0:
                    item.setForeground(QtGui.QColor(255, 0, 0))
                elif j > 2 and cell < 0:
                    item.setForeground(QtGui.QColor(0, 0, 255))
                else:
                    item.setForeground(QtGui.QColor(64, 64, 64))

                if i < 5:
                    item.setBackground(QtGui.QColor(242, 242, 242))
                elif 5 <= i < 9:
                    item.setBackground(QtGui.QColor(255, 255, 204))
                elif 9 <= i < 12:
                    item.setBackground(QtGui.QColor(204, 255, 204))
                elif 12 <= i < 16:
                    item.setBackground(QtGui.QColor(204, 255, 255))
                elif 16 <= i < 26:
                    item.setBackground(QtGui.QColor(230, 230, 230))
                else:
                    item.setBackground(QtGui.QColor(255, 255, 255))

                if j > 0:
                    cell = format(cell, ',')

                item.setData(QtCore.Qt.DisplayRole, str(cell))
                worksheet.setItem(i, j, item)

                worksheet.setHorizontalHeaderLabels(list(dataframe.columns.values))
                worksheet.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
                worksheet.setVisible(False)
                worksheet.resizeColumnsToContents()
                worksheet.resizeRowsToContents()
                worksheet.setVisible(True)

    def preprocess_security_summary(self):
        if not self.security_summary_todate:
            self.security_summary_todate = self.cob

        if not self.security_summary_fromdate:
            self.security_summary_fromdate = (datetime.datetime.strptime(self.security_summary_todate, '%Y%m%d').date()
                                              - pandas.tseries.offsets.BDay(self.date.convert_to_day(self.SECURITY_PILLAR_DEFAULT))).strftime('%Y%m%d')

    def calculate_security_summary_amount(self, ws_amt, ws_qty):
        print('<<quantitative/Strategy/calculate_summary_by_code<<')
        # self.get_opt10060_1_table()
        self.preprocess_security_summary()
        self.query_table_opt10060_1()
        self.preprocess_opt10060_1()
        self.create_investor_dataframe_by_value()
        self.calculate_quant1()
        self.calculate_pillar_by_value()
        self.publish_tab_code_by_value(ws_amt)

        # self.get_opt10060_2_table()
        self.query_table_opt10060_2()
        self.create_investor_dataframe_by_quantity()
        self.calculate_quant2()
        self.calculate_pillar_by_quantity()
        self.publish_tab_code_by_quantity(ws_qty)

        self.query_table_opt10081()
        self.create_dataframe_price()
        # self.assign_pillar_tab()
        # self.publish_tab_day()
        # self.publish_tab_week()
        # self.publish_tab_month()
        # self.publish_tab_quarter()
        # self.publish_tab_year()
        print('>>quantitative/Strategy/calculate_summary_by_code>>')

    def calculate_sector_table_by_pillar(self):
        # Not being used since no tab setup for individual pillars
        self.pillar = []
        self.day_sector_table = self.create_forward_table(self.opt10051_1_table, self.day_pillar)
        self.pillar = []
        self.week_sector_table = self.create_forward_table(self.opt10051_1_table, self.week_pillar)
        self.pillar = []
        self.month_sector_table = self.create_forward_table(self.opt10051_1_table, self.month_pillar)
        self.pillar = []
        self.quarter_sector_table = self.create_forward_table(self.opt10051_1_table, self.quarter_pillar)
        self.pillar = []
        self.year_sector_table = self.create_forward_table(self.opt10051_1_table, self.year_pillar)

    def calculate_sector_table_by_pillar_(self, table):
        # Not being used since no tab setup for individual pillars
        self.pillar = []
        self.day_sector_table = self.create_forward_table(table, self.day_pillar)
        self.pillar = []
        self.week_sector_table = self.create_forward_table(table, self.week_pillar)
        self.pillar = []
        self.month_sector_table = self.create_forward_table(table, self.month_pillar)
        self.pillar = []
        self.quarter_sector_table = self.create_forward_table(table, self.quarter_pillar)
        self.pillar = []
        self.year_sector_table = self.create_forward_table(table, self.year_pillar)

    def create_investor_dataframe_by_value(self):
        self.individual_1 = pandas.DataFrame({'Date': self.opt10060_1_table['date'], 'Close': self.opt10060_1_table['close'], 'Pos': self.opt10060_1_table['individual']})
        self.foreigner_1 = pandas.DataFrame({'Date': self.opt10060_1_table['date'], 'Close': self.opt10060_1_table['close'], 'Pos': self.opt10060_1_table['foreigner']})
        self.broker_1 = pandas.DataFrame({'Date': self.opt10060_1_table['date'], 'Close': self.opt10060_1_table['close'], 'Pos': self.opt10060_1_table['broker']})
        self.insurance_1 = pandas.DataFrame({'Date': self.opt10060_1_table['date'], 'Close': self.opt10060_1_table['close'], 'Pos': self.opt10060_1_table['insurance']})
        self.fund_1 = pandas.DataFrame({'Date': self.opt10060_1_table['date'], 'Close': self.opt10060_1_table['close'], 'Pos': self.opt10060_1_table['fund']})
        self.misc_1 = pandas.DataFrame({'Date': self.opt10060_1_table['date'], 'Close': self.opt10060_1_table['close'], 'Pos': self.opt10060_1_table['misc']})
        self.bank_1 = pandas.DataFrame({'Date': self.opt10060_1_table['date'], 'Close': self.opt10060_1_table['close'], 'Pos': self.opt10060_1_table['bank']})
        self.pension_1 = pandas.DataFrame({'Date': self.opt10060_1_table['date'], 'Close': self.opt10060_1_table['close'], 'Pos': self.opt10060_1_table['pension']})
        self.privateequity_1 = pandas.DataFrame({'Date': self.opt10060_1_table['date'], 'Close': self.opt10060_1_table['close'], 'Pos': self.opt10060_1_table['privateequity']})
        self.sovereign_1 = pandas.DataFrame({'Date': self.opt10060_1_table['date'], 'Close': self.opt10060_1_table['close'], 'Pos': self.opt10060_1_table['sovereign']})
        self.corporate_1 = pandas.DataFrame({'Date': self.opt10060_1_table['date'], 'Close': self.opt10060_1_table['close'], 'Pos': self.opt10060_1_table['corporate']})
        self.expatriate_1 = pandas.DataFrame({'Date': self.opt10060_1_table['date'], 'Close': self.opt10060_1_table['close'], 'Pos': self.opt10060_1_table['expatriate']})
        self.institution_1 = pandas.DataFrame({'Date': self.opt10060_1_table['date'], 'Close': self.opt10060_1_table['close'], 'Pos': self.opt10060_1_table['institution']})

        self.investor_list1 = (self.individual_1, self.foreigner_1, self.broker_1, self.insurance_1, self.fund_1,
                              self.misc_1, self.bank_1, self.pension_1, self.privateequity_1, self.sovereign_1,
                              self.corporate_1, self.expatriate_1, self.institution_1)

    def create_investor_dataframe_by_quantity(self):
        self.individual_2 = pandas.DataFrame({'Date': self.opt10060_2_table['date'], 'Close': self.opt10060_2_table['close'], 'Pos': self.opt10060_2_table['individual']})
        self.foreigner_2 = pandas.DataFrame({'Date': self.opt10060_2_table['date'], 'Close': self.opt10060_2_table['close'], 'Pos': self.opt10060_2_table['foreigner']})
        self.broker_2 = pandas.DataFrame({'Date': self.opt10060_2_table['date'], 'Close': self.opt10060_2_table['close'], 'Pos': self.opt10060_2_table['broker']})
        self.insurance_2 = pandas.DataFrame({'Date': self.opt10060_2_table['date'], 'Close': self.opt10060_2_table['close'], 'Pos': self.opt10060_2_table['insurance']})
        self.fund_2 = pandas.DataFrame({'Date': self.opt10060_2_table['date'], 'Close': self.opt10060_2_table['close'], 'Pos': self.opt10060_2_table['fund']})
        self.misc_2 = pandas.DataFrame({'Date': self.opt10060_2_table['date'], 'Close': self.opt10060_2_table['close'], 'Pos': self.opt10060_2_table['misc']})
        self.bank_2 = pandas.DataFrame({'Date': self.opt10060_2_table['date'], 'Close': self.opt10060_2_table['close'], 'Pos': self.opt10060_2_table['bank']})
        self.pension_2 = pandas.DataFrame({'Date': self.opt10060_2_table['date'], 'Close': self.opt10060_2_table['close'], 'Pos': self.opt10060_2_table['pension']})
        self.privateequity_2 = pandas.DataFrame({'Date': self.opt10060_2_table['date'], 'Close': self.opt10060_2_table['close'], 'Pos': self.opt10060_2_table['privateequity']})
        self.sovereign_2 = pandas.DataFrame({'Date': self.opt10060_2_table['date'], 'Close': self.opt10060_2_table['close'], 'Pos': self.opt10060_2_table['sovereign']})
        self.corporate_2 = pandas.DataFrame({'Date': self.opt10060_2_table['date'], 'Close': self.opt10060_2_table['close'], 'Pos': self.opt10060_2_table['corporate']})
        self.expatriate_2 = pandas.DataFrame({'Date': self.opt10060_2_table['date'], 'Close': self.opt10060_2_table['close'], 'Pos': self.opt10060_2_table['expatriate']})
        self.institution_2 = pandas.DataFrame({'Date': self.opt10060_2_table['date'], 'Close': self.opt10060_2_table['close'], 'Pos': self.opt10060_2_table['institution']})

        self.investor_list2 = (self.individual_2, self.foreigner_2, self.broker_2, self.insurance_2, self.fund_2, self.misc_2,
                              self.bank_2, self.pension_2, self.privateequity_2, self.sovereign_2, self.corporate_2, self.expatriate_2,
                              self.institution_2)

    def publish_tab_code_by_value(self, worksheet):
        frames = [self.day_table1[0:5], self.week_table1[0:4], self.month_table1[0:3], self.quarter_table1[0:4], self.year_table1[0:]]
        data = pandas.concat(frames)
        nrow = len(data.index)
        ncol = len(data.columns)
        extra_row = 0
        worksheet.setRowCount(nrow + extra_row)
        worksheet.setColumnCount(ncol)

        for i in range(nrow):
            for j in range(ncol):
                cell = data.iat[i, j]
                item = QtWidgets.QTableWidgetItem()
                item.setTextAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignRight)

                if j > 2 and cell >= 0:
                    item.setForeground(QtGui.QColor(255, 0, 0))
                elif j > 2 and cell < 0:
                    item.setForeground(QtGui.QColor(0, 0, 255))
                else:
                    item.setForeground(QtGui.QColor(64, 64, 64))

                if i < 5:
                    item.setBackground(QtGui.QColor(242, 242, 242))
                elif 5 <= i < 9:
                    item.setBackground(QtGui.QColor(255, 255, 204))
                elif 9 <= i < 12:
                    item.setBackground(QtGui.QColor(204, 255, 204))
                elif 12 <= i < 16:
                    item.setBackground(QtGui.QColor(204, 255, 255))
                elif 16 <= i < 26:
                    item.setBackground(QtGui.QColor(230, 230, 230))
                else:
                    item.setBackground(QtGui.QColor(255, 255, 255))

                if j > 0:
                    cell = format(cell, ',')

                item.setData(QtCore.Qt.DisplayRole, str(cell))
                worksheet.setItem(i, j, item)

        worksheet.setHorizontalHeaderLabels(list(data.columns.values))
        worksheet.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        worksheet.setVisible(False)
        worksheet.resizeColumnsToContents()
        worksheet.resizeRowsToContents()
        worksheet.setVisible(True)

    def publish_tab_code_by_quantity(self, worksheet):
        frames = [self.day_table2[0:5], self.week_table2[0:4], self.month_table2[0:3], self.quarter_table2[0:4], self.year_table2[0:]]
        data = pandas.concat(frames)
        nrow = len(data.index)
        ncol = len(data.columns)
        extra_row = 13
        worksheet.setRowCount(nrow + extra_row)
        worksheet.setColumnCount(ncol)

        for i in range(nrow):
            for j in range(ncol):
                cell = data.iat[i, j]
                item = QtWidgets.QTableWidgetItem()
                item.setTextAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignRight)

                if j > 2 and cell >= 0:
                    item.setForeground(QtGui.QColor(255, 0, 0))
                elif j > 2 and cell < 0:
                    item.setForeground(QtGui.QColor(0, 0, 255))
                else:
                    item.setForeground(QtGui.QColor(64, 64, 64))

                if i < 5:
                    item.setBackground(QtGui.QColor(242, 242, 242))
                elif 5 <= i < 9:
                    item.setBackground(QtGui.QColor(255, 255, 204))
                elif 9 <= i < 12:
                    item.setBackground(QtGui.QColor(204, 255, 204))
                elif 12 <= i < 16:
                    item.setBackground(QtGui.QColor(204, 255, 255))
                elif 16 <= i < 26:
                    item.setBackground(QtGui.QColor(230, 230, 230))
                else:
                    item.setBackground(QtGui.QColor(255, 255, 255))

                if j > 0:
                    cell = format(cell, ',')

                item.setData(QtCore.Qt.DisplayRole, str(cell))
                worksheet.setItem(i, j, item)

        worksheet.setItem(nrow + 1, 0, QtWidgets.QTableWidgetItem('평균단가'))
        worksheet.setItem(nrow + 2, 0, QtWidgets.QTableWidgetItem('매집현재'))
        worksheet.setItem(nrow + 3, 0, QtWidgets.QTableWidgetItem('매집랭킹'))
        worksheet.setItem(nrow + 4, 0, QtWidgets.QTableWidgetItem('매집비중'))
        worksheet.setItem(nrow + 5, 0, QtWidgets.QTableWidgetItem('매집상대'))
        worksheet.setItem(nrow + 6, 0, QtWidgets.QTableWidgetItem('분산비율'))
        worksheet.setItem(nrow + 7, 0, QtWidgets.QTableWidgetItem('매집고점'))
        worksheet.setItem(nrow + 8, 0, QtWidgets.QTableWidgetItem('선도랭킹'))
        worksheet.setItem(nrow + 9, 0, QtWidgets.QTableWidgetItem('선도비중'))
        worksheet.setItem(nrow + 10, 0, QtWidgets.QTableWidgetItem('선도상대'))

        investor_exc_institution = self.investor_list2[:len(self.investor_list2) - 1]
        avgbuyprice = [round(df['AvgBuyPrice'][0], 0) for df in investor_exc_institution]
        holdnow = [df['Hold'][0] for df in investor_exc_institution]
        holdnow_sum = sum(holdnow)
        holdnow_max = max(holdnow)
        holdmax = [df['HoldMax'][0] for df in investor_exc_institution]
        holdmax_sum = sum(holdmax)
        holdmax_max = max(holdmax)
        holdnow_rank = scipy.stats.rankdata(numpy.subtract(holdnow_max, holdnow))
        holdnow_ratio = numpy.divide(holdnow, holdnow_sum / 100)
        holdnow_relative = numpy.divide(holdnow, holdnow_max)
        now_over_max = numpy.multiply(100, numpy.divide(holdnow, holdmax))

        holdmax_trim = holdmax[1:]
        holdmax_rank = scipy.stats.rankdata(numpy.subtract(max(holdmax_trim), holdmax_trim))
        holdmax_ratio = numpy.divide(holdmax_trim, sum(holdmax_trim) / 100)
        holdmax_relative = numpy.divide(holdmax_trim, max(holdmax_trim))

        for j in range(0, len(holdnow)):
            item_investor = QtWidgets.QTableWidgetItem(str(self.investor_label_kr[j]))
            item_investor.setTextAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignCenter)

            item_avgbuyprice = QtWidgets.QTableWidgetItem(str(format(int(avgbuyprice[j]), ',')))
            item_avgbuyprice.setTextAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignRight)

            item_holdnow = QtWidgets.QTableWidgetItem(str(format(holdnow[j], ',')))
            item_holdnow.setTextAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignRight)

            item_holdnow_rank = QtWidgets.QTableWidgetItem(str(int(holdnow_rank[j])))
            item_holdnow_rank.setTextAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignRight)

            item_holdnow_ratio = QtWidgets.QTableWidgetItem(str(int(round(holdnow_ratio[j], 0))) + "%")
            item_holdnow_ratio.setTextAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignRight)

            item_holdnow_relative = QtWidgets.QTableWidgetItem(str(round(holdnow_relative[j], 2)))
            item_holdnow_relative.setTextAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignRight)

            item_now_over_max = QtWidgets.QTableWidgetItem(str(int(round(now_over_max[j], 0))) + "%")
            item_now_over_max.setTextAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignRight)

            item_holdmax = QtWidgets.QTableWidgetItem(str(format(holdmax[j], ',')))
            item_holdmax.setTextAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignRight)

            worksheet.setItem(nrow + 0, j + 3, item_investor)
            worksheet.setItem(nrow + 1, j + 3, item_avgbuyprice)
            worksheet.setItem(nrow + 2, j + 3, item_holdnow)
            worksheet.setItem(nrow + 3, j + 3, item_holdnow_rank)
            worksheet.setItem(nrow + 4, j + 3, item_holdnow_ratio)
            worksheet.setItem(nrow + 5, j + 3, item_holdnow_relative)
            worksheet.setItem(nrow + 6, j + 3, item_now_over_max)
            worksheet.setItem(nrow + 7, j + 3, item_holdmax)

        for j in range(0, len(holdmax_trim)):
            item_holdmax_rank = QtWidgets.QTableWidgetItem(str(int(holdmax_rank[j])))
            item_holdmax_rank.setTextAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignRight)

            item_holdmax_ratio = QtWidgets.QTableWidgetItem(str(int(round(holdmax_ratio[j], 0))) + "%")
            item_holdmax_ratio.setTextAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignRight)

            item_holdmax_relative = QtWidgets.QTableWidgetItem(str(round(holdmax_relative[j], 2)))
            item_holdmax_relative.setTextAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignRight)

            worksheet.setItem(nrow + 8, j + 4, item_holdmax_rank)
            worksheet.setItem(nrow + 9, j + 4, item_holdmax_ratio)
            worksheet.setItem(nrow + 10, j + 4, item_holdmax_relative)

        worksheet.setHorizontalHeaderLabels(list(data.columns.values))
        worksheet.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        worksheet.setVisible(False)
        worksheet.resizeColumnsToContents()
        worksheet.resizeRowsToContents()
        worksheet.setVisible(True)

    def calculate_pillar_by_value(self):  # table1: value
        self.pillar = []
        self.day_table1 = self.create_forward_table(self.opt10060_1_table, self.day_pillar)
        self.pillar = []
        self.week_table1 = self.create_forward_table(self.opt10060_1_table, self.week_pillar)
        self.pillar = []
        self.month_table1 = self.create_forward_table(self.opt10060_1_table, self.month_pillar)
        self.pillar = []
        self.quarter_table1 = self.create_forward_table(self.opt10060_1_table, self.quarter_pillar)
        self.pillar = []
        self.year_table1 = self.create_forward_table(self.opt10060_1_table, self.year_pillar)

    def calculate_pillar_by_quantity(self):  # table2: quantity
        self.pillar = []
        self.day_table2 = self.create_forward_table(self.opt10060_2_table, self.day_pillar)
        self.pillar = []
        self.week_table2 = self.create_forward_table(self.opt10060_2_table, self.week_pillar)
        self.pillar = []
        self.month_table2 = self.create_forward_table(self.opt10060_2_table, self.month_pillar)
        self.pillar = []
        self.quarter_table2 = self.create_forward_table(self.opt10060_2_table, self.quarter_pillar)
        self.pillar = []
        self.year_table2 = self.create_forward_table(self.opt10060_2_table, self.year_pillar)

    def assign_pillar_tab(self):
        # TODO: create new setting parameter to choose eitehr 1.value or 2.quantity for each d/w/m/q/y pillar tab
        # Should display code_by_quantity for individual pillar tabs, ie, day, week, month, quarter and year tabs
        if True:
            self.day_data = self.day_table1
            self.week_data = self.week_table1
            self.month_data = self.month_table1
            self.quarter_data = self.quarter_table1
            self.year_data = self.year_table1
        else:
            self.day_data = self.day_table2
            self.week_data = self.week_table2
            self.month_data = self.month_table2
            self.quarter_data = self.quarter_table2
            self.year_data = self.year_table2

    def publish_tab_day(self):
        self.day_worksheet.setColumnCount(len(self.day_data.columns))
        self.day_worksheet.setRowCount(len(self.day_data.index))
        self.fill_worksheet(self.day_data, self.day_worksheet)
        self.day_worksheet.setHorizontalHeaderLabels(list(self.day_data.columns.values))
        self.day_worksheet.setAlternatingRowColors(True)
        self.day_worksheet.setStyleSheet(
            "background-color:rgb(255, 255, 204); alternate-background-color:rgb(229, 255, 204);")
        self.day_worksheet.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.day_worksheet.resizeColumnsToContents()
        self.day_worksheet.resizeRowsToContents()

    def publish_tab_week(self):
        self.week_worksheet.setColumnCount(len(self.week_data.columns))
        self.week_worksheet.setRowCount(len(self.week_data.index))
        self.fill_worksheet(self.week_data, self.week_worksheet)
        self.week_worksheet.setHorizontalHeaderLabels(list(self.week_data.columns.values))
        self.week_worksheet.setAlternatingRowColors(True)
        self.week_worksheet.setStyleSheet(
            "background-color:rgb(255, 255, 204); alternate-background-color:rgb(229, 255, 204);")
        self.week_worksheet.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.week_worksheet.resizeColumnsToContents()
        self.week_worksheet.resizeRowsToContents()

    def publish_tab_month(self):
        self.month_worksheet.setColumnCount(len(self.month_data.columns))
        self.month_worksheet.setRowCount(len(self.month_data.index))
        self.fill_worksheet(self.month_data, self.month_worksheet)
        self.month_worksheet.setHorizontalHeaderLabels(list(self.month_data.columns.values))
        self.month_worksheet.setAlternatingRowColors(True)
        self.month_worksheet.setStyleSheet(
            "background-color:rgb(255, 255, 204); alternate-background-color:rgb(229, 255, 204);")
        self.month_worksheet.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.month_worksheet.resizeColumnsToContents()
        self.month_worksheet.resizeRowsToContents()

    def publish_tab_quarter(self):
        self.quarter_worksheet.setColumnCount(len(self.quarter_data.columns))
        self.quarter_worksheet.setRowCount(len(self.quarter_data.index))
        self.fill_worksheet(self.quarter_data, self.quarter_worksheet)
        self.quarter_worksheet.setHorizontalHeaderLabels(list(self.quarter_data.columns.values))
        self.quarter_worksheet.setAlternatingRowColors(True)
        self.quarter_worksheet.setStyleSheet(
            "background-color:rgb(255, 255, 204); alternate-background-color:rgb(229, 255, 204);")
        self.quarter_worksheet.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.quarter_worksheet.resizeColumnsToContents()
        self.quarter_worksheet.resizeRowsToContents()

    def publish_tab_year(self):
        self.year_worksheet.setColumnCount(len(self.year_data.columns))
        self.year_worksheet.setRowCount(len(self.year_data.index))
        self.fill_worksheet(self.year_data, self.year_worksheet)
        self.year_worksheet.setHorizontalHeaderLabels(list(self.year_data.columns.values))
        self.year_worksheet.setAlternatingRowColors(True)
        self.year_worksheet.setStyleSheet(
            "background-color:rgb(255, 255, 204); alternate-background-color:rgb(229, 255, 204);")
        self.year_worksheet.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.year_worksheet.resizeColumnsToContents()
        self.year_worksheet.resizeRowsToContents()

    def extend_investor_df_column(self):
        for df in self.investor_list_kospi_value:
            self.calculate_supply_demand(df)

    def extend_investor_df_column_(self, table):
        for df in table:
            self.calculate_supply_demand(df)

    def calculate_quant1(self):
        for df in self.investor_list1:
            self.calculate_supply_demand(df)

    def calculate_quant2(self):
        for df in self.investor_list2:
            self.calculate_supply_demand(df)

    def calculate_quant_sector(self):
        for df in self.investor_list_sector_value:
            self.calculate_supply_demand(df)

    def calculate_supply_demand(self, df):
        df['PosCumSum'] = df['Pos'][::-1].cumsum()[::-1]
        df['PosCumMin'] = df['PosCumSum'][::-1].cummin()[::-1]
        df['Hold'] = df['PosCumSum'] - df['PosCumMin']
        df['HoldMax'] = df['Hold'][::-1].cummax()[::-1]
        df['HoldRatio'] = df['Hold'] / df['HoldMax']
        df['NetVol'] = df['Pos'].apply(lambda x: x if x > 0 else 0)
        df['NetVolCumSum'] = df['NetVol'][::-1].cumsum()[::-1]
        df["NetAmt"] = df['Close'] * df['NetVol']
        df["NetAmtCumSum"] = df['NetAmt'][::-1].cumsum()[::-1]
        df["AvgBuyPrice"] = df['NetAmtCumSum'] / df['NetVolCumSum']

    def plot_code_default(self):
        x1 = self.ohlc['Date']
        y1 = self.ohlc['Close']
        dates = (x1 >= self.security_summary_fromdate) & (x1 <= self.security_summary_todate)
        # self.field_fromdate_code.setText(x1[max(x1[dates].index)])
        # self.field_todate_code.setText(x1[min(x1[dates].index)])
        x1 = list(x1[dates])
        x1 = pandas.to_datetime(x1, format='%Y%m%d')
        y1 = list(y1[dates])

        x2 = self.individual_2['Date']
        y2 = self.individual_2['HoldRatio']
        y3 = self.foreigner_2['HoldRatio']
        dates = (x2 >= self.security_summary_fromdate) & (x2 <= self.security_summary_todate)
        x2 = list(x2[dates])
        x2 = pandas.to_datetime(x2, format='%Y%m%d')
        y2 = list(y2[dates])
        x3 = x2
        y3 = list(y3[dates])

        self.plot_code_default_close.set_title('plot_code_default', fontsize=8)
        self.plot_code_default_close.plot(x1, y1, '-', label='Close')
        # self.plot_code_default_close.set_xlabel('Date', fontsize=8)
        self.plot_code_default_close.set_ylabel('Close', fontsize=8, color='b')
        self.plot_code_default_close.tick_params('y', size=4, colors='b')
        self.plot_code_default_close.grid(True)

        self.plot_code_default_holding.plot(x2, y2, '-', label='Individual')
        self.plot_code_default_holding.plot(x3, y3, '-', label='RegdFrgnr')
        # self.plot_code_default_holding.set_xlabel('Date', fontsize=8)
        self.plot_code_default_holding.set_ylabel('HoldRatio', fontsize=8, color='g')
        self.plot_code_default_holding.tick_params('y', size=2, colors='g')
        self.plot_code_default_holding.grid(True)
        self.plot_code_default_holding.legend(loc='upper left', shadow=True, fancybox=True, fontsize='small')

        # TODO: bus days may be different due to local holiday convention, but shouldn't be material in chart
        bdays = numpy.busday_count(pandas.to_datetime(self.security_summary_fromdate, format='%Y%m%d'), pandas.to_datetime(self.security_summary_todate, format='%Y%m%d'))
        major = self.date.set_major_locator(bdays)
        minor = self.date.set_minor_locator(bdays)
        format = self.date.set_date_format(bdays)

        self.plot_code_default_close.xaxis.set_major_locator(major)
        self.plot_code_default_close.xaxis.set_major_formatter(format)
        self.plot_code_default_close.xaxis.set_minor_locator(minor)

        # self.plot_code_default_holding.xaxis.set_major_locator(major)
        # self.plot_code_default_holding.xaxis.set_major_formatter(format)
        # self.plot_code_default_holding.xaxis.set_minor_locator(minor)
        # self.plot_code_default_close.get_shared_x_axes().join(self.plot_code_default_close, self.plot_code_default_holding)

    def create_date_index(self, convention):
        date_index = [0]
        for i in convention:
            if self.date.convert_to_day(i) < self.data_end + 1:
                date_index.append(self.date.convert_to_day(i))
                self.pillar.append(i)
            else:
                break
        return date_index

    def create_forward_table(self, table, pillar):
        tenor = []
        close = []
        volume = []
        individual = []
        foreigner = []
        broker = []
        insurance = []
        fund = []
        misc = []
        bank = []
        pension = []
        privateequity = []
        sovereign = []
        corporate = []
        expatriate = []
        institution = []

        day_index = self.create_date_index(pillar)

        for i in range(0, len(self.pillar)):
            start_date = day_index[i]
            end_date = day_index[i + 1] - 1

            tenor.append(self.pillar[i])
            close.append(int(round(table.close[start_date:end_date + 1].mean(), 0)))
            volume.append(int(round(table.volume[start_date:end_date + 1].mean(), 0)))
            individual.append(table.individual[start_date:end_date + 1].sum())
            foreigner.append(table.foreigner[start_date:end_date + 1].sum())
            broker.append(table.broker[start_date:end_date + 1].sum())
            insurance.append(table.insurance[start_date:end_date + 1].sum())
            fund.append(table.fund[start_date:end_date + 1].sum())
            misc.append(table.misc[start_date:end_date + 1].sum())
            bank.append(table.bank[start_date:end_date + 1].sum())
            pension.append(table.pension[start_date:end_date + 1].sum())
            privateequity.append(table.privateequity[start_date:end_date + 1].sum())
            sovereign.append(table.sovereign[start_date:end_date + 1].sum())
            corporate.append(table.corporate[start_date:end_date + 1].sum())
            expatriate.append(table.expatriate[start_date:end_date + 1].sum())
            institution.append(table.institution[start_date:end_date + 1].sum())

        worksheet = list(zip(tenor, close, volume, individual, foreigner, broker, insurance, fund, misc, bank, pension,
                             privateequity, sovereign, corporate, expatriate, institution))
        df = pandas.DataFrame(data=worksheet, columns=['tenor', 'close', 'volume', 'individual', 'foreigner', 'broker',
                                                       'insurance', 'fund', 'misc', 'bank', 'pension', 'privateequity',
                                                       'sovereign', 'corporate', 'expatriate', 'institution'])

        return df

    def fill_worksheet(self, data, worksheet):
        for i in range(len(data.index)):
            for j in range(len(data.columns)):
                item = QtWidgets.QTableWidgetItem()
                item.setTextAlignment(QtCore.Qt.AlignVCenter | QtCore.Qt.AlignRight)
                cell = data.iat[i, j]  # cell = dataframe.iat(i, j)

                if j > 2 and cell >= 0:
                    item.setForeground(QtGui.QColor(0, 0, 255))
                elif j > 2 and cell < 0:
                    item.setForeground(QtGui.QColor(255, 0, 0))
                else:
                    item.setForeground(QtGui.QColor(64, 64, 64))

                if j > 0:
                    cell = format(cell, ',')

                item.setData(QtCore.Qt.DisplayRole, str(cell))
                worksheet.setItem(i, j, item)

    def refresh_chart_field(self):
        if self.field_todate_chart.text() == "":
            self.field_todate_chart.setText(self.field_todate_code.text())
        # data_start = self.opt10060_2_table['individual'].loc[self.opt10060_2_table['date'] == self.sdate_chart.text()].index[0]

        if (self.field_fromdate_chart.text() == "") and (self.field_pillar_chart.text() == ""):
            self.field_fromdate_chart.setText(self.field_fromdate_code.text())
            # data_end = self.opt10060_2_table['individual'].loc[self.opt10060_2_table['date'] == self.edate_chart.text()].index[0]
        elif (self.field_fromdate_chart.text() == "") and (self.field_pillar_chart.text() != ""):
            # data_end = data_start + self.date.convert_to_day(self.pillar_chart.text())
            self.field_fromdate_chart.setText(self.opt10060_2_table['date'][self.date.convert_to_day(self.field_pillar_chart.text())])
            # print(self.convert_to_day(self.pillar_chart.text()))
        else:
            self.field_pillar_chart.setText(self.PILLAR_DEFAULT)
            # data_end = data_start + self.date.convert_to_day(self.pillar_chart.text())
            self.field_fromdate_chart.setText(self.opt10060_2_table['date'][self.date.convert_to_day(self.field_pillar_chart.text())])

    def refresh_chart_field_new(self, fromdate, todate, pillar):
        if todate.text() == "":
            # todate.setText(self.field_todate_code.text())
            todate.setText(self.security_summary_todate)
            # print("todate_code", self.todate_code)

        if fromdate.text() == "" and pillar.text() == "":
            fromdate.setText(self.security_summary_fromdate)
        elif fromdate.text() == "" and pillar.text() != "":
            fromdate.setText(self.opt10060_2_table['date'][self.date.convert_to_day(pillar.text())])
        else:
            pillar.setText(self.PILLAR_DEFAULT)
            fromdate.setText(self.opt10060_2_table['date'][self.date.convert_to_day(pillar.text())])

    def publish_tab_security_summary_chart1(self, fromdate, todate, pillar, individual, regdfrgnr, institutions, secfutures, insurance,
                        investmenttrust, merchantbanks, banks, fundpensions, privateequity, govtinternationalorg,
                        othercorp, nonregdfrgnr):

        self.refresh_chart_field_new(fromdate, todate, pillar)
        # self.refresh_chart_field()

        x1 = self.ohlc['Date']
        y1 = self.ohlc['Close']
        # dates = (x1 >= self.fromdate_code) & (x1 <= self.todate_code)
        dates = (x1 >= fromdate.text()) & (x1 <= todate.text())
        x1 = list(x1[dates])
        x1 = pandas.to_datetime(x1, format='%Y%m%d')
        y1 = list(y1[dates])

        self.plot.set_title(self.security_summary_code + ' @ plot_chart_1', fontsize=8)
        self.plot.plot(x1, y1, '-', label='Close', color='k')
        # self.plot.set_xlabel('Date', fontsize=8)
        self.plot.set_ylabel('Close', fontsize=8, color='k')
        self.plot.tick_params('y', size=4, labelcolor='k')
        self.plot.grid(True)

        if individual:
            y2 = self.individual_2['Hold']
            y2 = list(y2[dates])
            self.plot_twinx.plot(x1, y2, '-', label='Individual')

        if regdfrgnr:
            y2 = self.foreigner_2['Hold']
            y2 = list(y2[dates])
            self.plot_twinx.plot(x1, y2, '-', label='RegdFrgnr')

        if institutions:
            y2 = self.institution_2['Hold']
            y2 = list(y2[dates])
            self.plot_twinx.plot(x1, y2, '-', label='Institutions')

        if secfutures:
            y2 = self.broker_2['Hold']
            y2 = list(y2[dates])
            self.plot_twinx.plot(x1, y2, '-', label='SecFutures')

        if insurance:
            y2 = self.insurance_2['Hold']
            y2 = list(y2[dates])
            self.plot_twinx.plot(x1, y2, '-', label='Insurance')

        if investmenttrust:
            y2 = self.fund_2['Hold']
            y2 = list(y2[dates])
            self.plot_twinx.plot(x1, y2, '-', label='InvestmentTrust')

        if merchantbanks:
            y2 = self.misc_2['Hold']
            y2 = list(y2[dates])
            self.plot_twinx.plot(x1, y2, '-', label='MerchantBanks')

        if banks:
            y2 = self.bank_2['Hold']
            y2 = list(y2[dates])
            self.plot_twinx.plot(x1, y2, '-', label='Banks')

        if fundpensions:
            y2 = self.pension_2['Hold']
            y2 = list(y2[dates])
            self.plot_twinx.plot(x1, y2, '-', label='FundPensions')

        if privateequity:
            y2 = self.privateequity_2['Hold']
            y2 = list(y2[dates])
            self.plot_twinx.plot(x1, y2, '-', label='PrivateEquity')

        if govtinternationalorg:
            y2 = self.sovereign_2['Hold']
            y2 = list(y2[dates])
            self.plot_twinx.plot(x1, y2, '-', label='GovtInternationalOrg')

        if othercorp:
            y2 = self.corporate_2['Hold']
            y2 = list(y2[dates])
            self.plot_twinx.plot(x1, y2, '-', label='OtherCorp')

        if nonregdfrgnr:
            y2 = self.expatriate_2['Hold']
            y2 = list(y2[dates])
            self.plot_twinx.plot(x1, y2, '-', label='NonRegdFrgnr')

        self.plot_twinx.set_ylabel('Hold', fontsize=8, color='g')
        self.plot_twinx.tick_params('y', size=4, labelcolor='g')
        # self.plot_twinx.grid(True)
        self.plot_twinx.legend(loc='upper left', shadow=True, fancybox=True, fontsize='small')

        bdays = numpy.busday_count(pandas.to_datetime(fromdate.text(), format='%Y%m%d'),
                                   pandas.to_datetime(todate.text(), format='%Y%m%d'))
        major = self.date.set_major_locator(bdays)
        minor = self.date.set_minor_locator(bdays)
        format = self.date.set_date_format(bdays)

        self.plot.xaxis.set_major_locator(major)
        self.plot.xaxis.set_major_formatter(format)
        self.plot.xaxis.set_minor_locator(minor)

    def refresh_chart2_field(self, fromdate, todate, pillar):
        if todate.text() == "":
            todate.setText(self.security_summary_todate)

        if fromdate.text() == "" and pillar.text() == "":
            fromdate.setText(self.security_summary_fromdate)
        elif fromdate.text() == "" and pillar.text() != "":
            fromdate.setText(self.opt10060_2_table['date'][self.date.convert_to_day(pillar.text())])
        else:
            pillar.setText(self.PILLAR_DEFAULT)
            fromdate.setText(self.opt10060_2_table['date'][self.date.convert_to_day(pillar.text())])

    def publish_tab_security_summary_chart2(self, fromdate, todate, pillar, individual, foreigner, broker, insurance, fund, misc, bank,
                        pension, privateequity, sovereign, corporate, expatriate, institution):
        self.refresh_chart2_field(fromdate, todate, pillar)

        x1 = self.ohlc['Date']
        y1 = self.ohlc['Close']
        dates = (x1 >= fromdate.text()) & (x1 <= todate.text())
        x1 = list(x1[dates])
        x1 = pandas.to_datetime(x1, format='%Y%m%d')
        y1 = list(y1[dates])

        self.chart2_1.set_title(self.security_summary_code + ' @ chart2_1', fontsize=8)
        self.chart2_1.plot(x1, y1, '-', label='Close', color='k')
        # self.chart2_1.set_xlabel('Date', fontsize=8)
        self.chart2_1.set_ylabel('Close', fontsize=8, color='k')
        self.chart2_1.tick_params('y', size=4, labelcolor='k')
        self.chart2_1.grid(True)

        # self.chart2_2.set_title('chart2_2: HoldingPos', fontsize=8)
        # self.chart2_2.set_xlabel('Date', fontsize=8)
        if individual:
            y2 = self.individual_2['Hold']
            y2 = list(y2[dates])
            self.chart2_2.plot(x1, y2, '-', label='Individual')

        if foreigner:
            y2 = self.foreigner_2['Hold']
            y2 = list(y2[dates])
            self.chart2_2.plot(x1, y2, '-', label='RegdFrgnr')

        if broker:
            y2 = self.broker_2['Hold']
            y2 = list(y2[dates])
            self.chart2_2.plot(x1, y2, '-', label='SecFutures')

        if insurance:
            y2 = self.insurance_2['Hold']
            y2 = list(y2[dates])
            self.chart2_2.plot(x1, y2, '-', label='Insurance')

        if fund:
            y2 = self.fund_2['Hold']
            y2 = list(y2[dates])
            self.chart2_2.plot(x1, y2, '-', label='InvestmentTrust')

        if misc:
            y2 = self.misc_2['Hold']
            y2 = list(y2[dates])
            self.chart2_2.plot(x1, y2, '-', label='MerchantBanks')

        if bank:
            y2 = self.bank_2['Hold']
            y2 = list(y2[dates])
            self.chart2_2.plot(x1, y2, '-', label='Banks')

        if pension:
            y2 = self.pension_2['Hold']
            y2 = list(y2[dates])
            self.chart2_2.plot(x1, y2, '-', label='FundPensions')

        if privateequity:
            y2 = self.privateequity_2['Hold']
            y2 = list(y2[dates])
            self.chart2_2.plot(x1, y2, '-', label='PrivateEquity')

        if sovereign:
            y2 = self.sovereign_2['Hold']
            y2 = list(y2[dates])
            self.chart2_2.plot(x1, y2, '-', label='GovtInternationalOrg')

        if corporate:
            y2 = self.corporate_2['Hold']
            y2 = list(y2[dates])
            self.chart2_2.plot(x1, y2, '-', label='OtherCorp')

        if expatriate:
            y2 = self.expatriate_2['Hold']
            y2 = list(y2[dates])
            self.chart2_2.plot(x1, y2, '-', label='NonRegdFrgnr')

        if institution:
            y2 = self.institution_2['Hold']
            y2 = list(y2[dates])
            self.chart2_2.plot(x1, y2, '-', label='Institutions')

        self.chart2_2.set_ylabel('Hold', fontsize=8, color='r')
        self.chart2_2.tick_params('y', size=4, labelcolor='r')
        self.chart2_2.grid(True)
        self.chart2_2.legend(loc='upper left', shadow=True, fancybox=True, fontsize='small')

        # self.chart2_3.set_title('HoldingPos', fontsize=7)
        # self.chart2_3.set_xlabel('Date', fontsize=7)
        if individual:
            y3 = self.individual_2['HoldRatio']
            y3 = list(y3[dates])
            self.chart2_3.plot(x1, y3, '-', label='Individual')

        if foreigner:
            y3 = self.foreigner_2['HoldRatio']
            y3 = list(y3[dates])
            self.chart2_3.plot(x1, y3, '-', label='RegdFrgnr')

        if broker:
            y3 = self.broker_2['HoldRatio']
            y3 = list(y3[dates])
            self.chart2_3.plot(x1, y3, '-', label='SecFutures')

        if insurance:
            y3 = self.insurance_2['HoldRatio']
            y3 = list(y3[dates])
            self.chart2_3.plot(x1, y3, '-', label='Insurance')

        if fund:
            y3 = self.fund_2['HoldRatio']
            y3 = list(y3[dates])
            self.chart2_3.plot(x1, y3, '-', label='InvestmentTrust')

        if misc:
            y3 = self.misc_2['HoldRatio']
            y3 = list(y3[dates])
            self.chart2_3.plot(x1, y3, '-', label='MerchantBanks')

        if bank:
            y3 = self.bank_2['HoldRatio']
            y3 = list(y3[dates])
            self.chart2_3.plot(x1, y3, '-', label='Banks')

        if pension:
            y3 = self.pension_2['HoldRatio']
            y3 = list(y3[dates])
            self.chart2_3.plot(x1, y3, '-', label='FundPensions')

        if privateequity:
            y3 = self.privateequity_2['HoldRatio']
            y3 = list(y3[dates])
            self.chart2_3.plot(x1, y3, '-', label='PrivateEquity')

        if sovereign:
            y3 = self.sovereign_2['HoldRatio']
            y3 = list(y3[dates])
            self.chart2_3.plot(x1, y3, '-', label='GovtInternationalOrg')

        if corporate:
            y3 = self.corporate_2['HoldRatio']
            y3 = list(y3[dates])
            self.chart2_3.plot(x1, y3, '-', label='OtherCorp')

        if expatriate:
            y3 = self.expatriate_2['HoldRatio']
            y3 = list(y3[dates])
            self.chart2_3.plot(x1, y3, '-', label='NonRegdFrgnr')

        if institution:
            y3 = self.institution_2['HoldRatio']
            y3 = list(y3[dates])
            self.chart2_3.plot(x1, y3, '-', label='Institutions')

        self.chart2_3.set_ylabel('HoldRatio', fontsize=6, color='g')
        self.chart2_3.tick_params('y', size=4, labelcolor='g')
        self.chart2_3.grid(True)
        self.chart2_3.legend(loc='upper left', shadow=True, fancybox=True, fontsize='small')

        bdays = numpy.busday_count(pandas.to_datetime(fromdate.text(), format='%Y%m%d'),
                                   pandas.to_datetime(todate.text(), format='%Y%m%d'))
        major = self.date.set_major_locator(bdays)
        minor = self.date.set_minor_locator(bdays)
        format = self.date.set_date_format(bdays)

        self.chart2_1.xaxis.set_major_locator(major)
        self.chart2_1.xaxis.set_major_formatter(format)
        self.chart2_1.xaxis.set_minor_locator(minor)

        # self.chart2_2.xaxis.set_major_locator(major)
        # self.chart2_2.xaxis.set_major_formatter(format)
        # self.chart2_2.xaxis.set_minor_locator(minor)

        # self.chart2_3.xaxis.set_major_locator(major)
        # self.chart2_3.xaxis.set_major_formatter(format)
        # self.chart2_3.xaxis.set_minor_locator(minor)

    def test_print(self, fromdate, todate, pillar):
        # print("fromdate_code", self.field_fromdate_code.text())
        print("fromdate_chart", fromdate.text())
        print("todate_chart", todate.text())
        print("pillar_chart", pillar.text())


class Visualisation(Strategy):
    def __init__(self):
        super(Visualisation, self).__init__()
        self.interface = BridgeInterface()
        self.strategy = Strategy()

    def test_chart_tab(self, fromdate, todate, pillar):
        self.test_print(fromdate, todate, pillar)


