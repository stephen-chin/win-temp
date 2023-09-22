class Database:
    def __init__(self):
        # self.path_kiwoom = "C:\\Users\\sc\\Dropbox\\dev\\project\\kiwoom\\data\\Kiwoom.db"
        self.path_kiwoom = "/Users/sc/dropbox/dev/project/kiwoom/data/Kiwoom.db"

    def get_connection(self, path):
        self.connection = sqlite3.connect(path)
        self.cursor = self.connection.cursor()


class SqliteKiwoom(Database):
    def __init__(self):
        super(SqliteKiwoom, self).__init__()
        self.connection = sqlite3.connect(self.path_kiwoom)
        self.cursor = self.connection.cursor()

    def get_server_path(self):

        return self.path_kiwoom


class SqliteTest(Database):
    # TODO: SqliteClass마다 커넥션/커서를 정의하지 말고 부모 클래스에 정의해 놓고 path를 매개변수로 해서 상속하는 방법은 어떨까?
    def __init__(self):
        super(SqliteTest, self).__init__()
        # path = "C:\\Users\\sc\\Dropbox\\dev\\project\\kiwoom\\db\\test.db"
        self.get_connection(self.path_kiwoom)


class TableMasterStockInfo(SqliteKiwoom):
    def __init__(self):
        super(TableMasterStockInfo, self).__init__()
        self.name = "masterstockinfo"
        self.schema = "(`code` CHAR, `name` CHAR, `exchange` CHAR, 'market' CHAR, 'marketcap' CHAR, 'sector' CHAR)"
        self.schema_ = "`code` CHAR, `name` CHAR, `exchange` CHAR, 'market' CHAR, 'marketcap' CHAR, 'sector' CHAR"
        self.column = "(code, name, exchange, market, marketcap, sector)"
        self.market = {'0': '장내', '10': '코스닥', '3': 'ELW', '8': 'ETF', '50': 'KONEX', '4': '뮤추얼펀드', '5': '신주인수권', '6': '리츠', '9': '하이일드펀드', '30': 'K-OTC'}

    def get_master_stock_info_given_code(self, code):
        query = "SELECT * FROM " + self.name + " WHERE code = '" + code + "'"
        table = pandas.read_sql_query(query, self.connection)

        return table.name[0], table.exchange[0], table.market[0], table.marketcap[0], table.sector[0]

    def get_master_stock_info_given_name(self, name):
        query = "SELECT * FROM " + self.name + " WHERE name = '" + name + "'"
        table = pandas.read_sql_query(query, self.connection)

        return table.code[0], table.exchange[0], table.market[0], table.marketcap[0], table.sector[0]

    def get_namelist_given_code(self, code):
        query = "SELECT * FROM " + self.name + " WHERE code LIKE '%" + code + "%'" + " AND (exchange = '장내' OR exchange = '코스닥')"
        self.cursor.execute(query)
        codename = self.cursor.fetchall()

        return codename

    def get_name_given_code(self, code):
        query = "SELECT name FROM " + self.name + " WHERE code = '" + code + "';"
        self.cursor.execute(query)
        codename = self.cursor.fetchall()

        return codename

    def get_codelist_given_name(self, name):
        query = "SELECT * FROM " + self.name + " WHERE name LIKE '%" + name + "%'" + " AND (exchange = '장내' OR exchange = '코스닥')"
        self.cursor.execute(query)
        codename = self.cursor.fetchall()

        return codename

    def drop_table(self):
        query = "DROP TABLE IF EXISTS " + self.name
        self.cursor.execute(query)
        self.connection.commit()

    def create_table(self):
        query = "CREATE TABLE IF NOT EXISTS " + self.name + "(" + self.schema_ + ", PRIMARY KEY (code, name, exchange)" + ")"
        self.cursor.execute(query)
        self.connection.commit()

    def insert_into(self, code, name, market_name, market, marketcap, sector):
        query = "INSERT OR IGNORE INTO " + self.name + self.column + "VALUES (?,?,?,?,?,?)"
        self.cursor.execute(query, (code, name, market_name, market, marketcap, sector))

    def commit_sqlitekiwoom(self):
        self.connection.commit()

    def get_kospi_codelist(self):
        query = "SELECT code FROM " + self.name + " WHERE market = '거래소' ORDER BY code ASC;"
        # codelist = pandas.read_sql_query(query, self.connection)
        self.cursor.execute(query)
        codelist = self.cursor.fetchall()

        return codelist

    def get_kosdaq_codelist(self):
        query = "SELECT code FROM " + self.name + " WHERE exchange = '코스닥' ORDER BY code ASC;"
        self.cursor.execute(query)
        codelist = self.cursor.fetchall()

        return codelist

    def get_etf_codelist(self):
        query = "SELECT code FROM " + self.name + " WHERE market = 'ETF' ORDER BY code ASC;"
        self.cursor.execute(query)
        codelist = self.cursor.fetchall()

        return codelist


class TableOpt10051_1(SqliteKiwoom):
    def __init__(self):
        # super().__init__()
        super(TableOpt10051_1, self).__init__()
        self.table = "opt10051_1"
        self.schema = "(`date` CHAR, `code` CHAR, `name` CHAR, `close` INTEGER, `diff` INTEGER, `volume` INTEGER, `individual` INTEGER, `foreigner` INTEGER, `institution` INTEGER, `broker` INTEGER, `insurance` INTEGER, `fund` INTEGER, `misc` INTEGER, `bank` INTEGER, `pension` INTEGER, `privateequity` INTEGER, `sovereign` INTEGER, `corporate` INTEGER, `expatriate` INTEGER)"
        self.schema_ = "`date` CHAR, `code` CHAR, `name` CHAR, `close` INTEGER, `diff` INTEGER, `volume` INTEGER, `individual` INTEGER, `foreigner` INTEGER, `institution` INTEGER, `broker` INTEGER, `insurance` INTEGER, `fund` INTEGER, `misc` INTEGER, `bank` INTEGER, `pension` INTEGER, `privateequity` INTEGER, `sovereign` INTEGER, `corporate` INTEGER, `expatriate` INTEGER"
        self.column = "(date, code, name, close, diff, volume, individual, foreigner, institution, broker, insurance, fund, misc, bank, pension, privateequity, sovereign, corporate, expatriate)"

    def drop_table(self):
        query = "DROP TABLE IF EXISTS " + self.table
        self.cursor.execute(query)
        self.connection.commit()

    def create_table(self):
        query = "CREATE TABLE IF NOT EXISTS " + self.table + "(" + self.schema_ + ", PRIMARY KEY (date, code)" + ")"
        self.cursor.execute(query)
        self.connection.commit()

    def insert_into(self, date, code, name, close, diff, volume, individual, foreigner, institution, broker, insurance, fund, misc, bank, pension, privateequity, sovereign, corporate, expatriate):
        query = "INSERT OR IGNORE INTO " + self.table + self.column + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        self.cursor.execute(query, (date, code, name, close, diff, volume, individual, foreigner, institution, broker, insurance, fund, misc, bank, pension, privateequity, sovereign, corporate, expatriate))

    def delete_from(self, code):
        query = "DELETE FROM " + self.table + " WHERE code = '" + code + "';"
        self.cursor.execute(query)
        self.connection.commit()

    def commit_sqlitekiwoom(self):
        self.connection.commit()

    def get_start_date(self):
        query = "SELECT COUNT(*) FROM " + self.table
        count = pandas.read_sql_query(query, self.connection)
        count = count['COUNT(*)'][0]

        if count:
            query = "SELECT DISTINCT date FROM " + self.table + " ORDER BY date DESC LIMIT 1"
            sdate = pandas.read_sql_query(query, self.connection)['date'][0]
        else:
            sdate = self.sdate.strftime('%Y%m%d')

        return sdate

    def get_end_date(self):
        query = "SELECT COUNT(*) FROM " + self.table
        count = pandas.read_sql_query(query, self.connection)
        count = count['COUNT(*)'][0]

        if count:
            query = "SELECT DISTINCT date FROM " + self.table + " ORDER BY date ASC LIMIT 1"
            edate = pandas.read_sql_query(query, self.connection)['date'][0]
        else:
            edate = self.END_DATE

        return edate

    @dispatch(str)
    def get_table(self, code):
        # query = "SELECT * FROM " + self.table + " WHERE date > '20030101' ORDER BY date DESC, code"
        query = "SELECT * FROM " + self.table + " WHERE date > '20150101' ORDER BY date DESC, code"
        table = pandas.read_sql_query(query, self.connection)

        return table

    @dispatch(str, str)
    def get_table(self, fromdate, todate):
        query = "SELECT * FROM " + self.table + " WHERE date >= '" + fromdate + "' AND date <= '" + todate + "' ORDER BY date DESC, code"
        table = pandas.read_sql_query(query, self.connection)

        return table

    @dispatch(str, str, str)
    def get_table(self, fromdate, todate, sector):
        query = "SELECT * FROM " + self.table + " WHERE date >= '" + fromdate + "' AND date <= '" + todate + "' AND name = '" + sector + "' ORDER BY date DESC, code"
        table = pandas.read_sql_query(query, self.connection)

        return table

    def get_summary_(self):
        query = "SELECT code, name, SUM(individual) AS individual, SUM(foreigner) AS foreigner, SUM(broker) AS broker, SUM(insurance) AS insurance, SUM(fund) AS fund, SUM(misc) AS misc, SUM(bank) AS bank, SUM(pension) AS pension, SUM(privateequity) AS privateequity, SUM(sovereign) AS sovereign, SUM(corporate) AS corporate, SUM(expatriate) AS expatriate FROM " + self.table + " WHERE date > '20180101' GROUP BY code, name ORDER BY code"
        table = pandas.read_sql_query(query, self.connection)

        return table

    def get_summary(self, fromdate, todate):
        # query = "SELECT code, name, SUM(individual) AS individual, SUM(foreigner) AS foreigner, SUM(broker) AS broker, SUM(insurance) AS insurance, SUM(fund) AS fund, SUM(misc) AS misc, SUM(bank) AS bank, SUM(pension) AS pension, SUM(privateequity) AS privateequity, SUM(sovereign) AS sovereign, SUM(corporate) AS corporate, SUM(expatriate) AS expatriate FROM " + self.table + " WHERE date < '" + sdate +"' GROUP BY code, name ORDER BY code"
        query = "SELECT code, name, SUM(individual) AS individual, SUM(foreigner) AS foreigner, SUM(broker) AS broker, SUM(insurance) AS insurance, SUM(fund) AS fund, SUM(misc) AS misc, SUM(bank) AS bank, SUM(pension) AS pension, SUM(privateequity) AS privateequity, SUM(sovereign) AS sovereign, SUM(corporate) AS corporate, SUM(expatriate) AS expatriate FROM " \
                + self.table + " WHERE date >= '" + fromdate +"' AND date <= '" + todate + "' GROUP BY code, name ORDER BY foreigner DESC"
        table = pandas.read_sql_query(query, self.connection)

        return table


class TableOpt10051_2(SqliteKiwoom):
    def __init__(self):
        # super().__init__()
        super(TableOpt10051_2, self).__init__()
        self.table = "opt10051_2"
        self.schema = "(`date` CHAR, `code` CHAR, `name` CHAR, `close` INTEGER, `diff` INTEGER, `volume` INTEGER, `individual` INTEGER, `foreigner` INTEGER, `institution` INTEGER, `broker` INTEGER, `insurance` INTEGER, `fund` INTEGER, `misc` INTEGER, `bank` INTEGER, `pension` INTEGER, `privateequity` INTEGER, `sovereign` INTEGER, `corporate` INTEGER, `expatriate` INTEGER)"
        self.schema_ = "`date` CHAR, `code` CHAR, `name` CHAR, `close` INTEGER, `diff` INTEGER, `volume` INTEGER, `individual` INTEGER, `foreigner` INTEGER, `institution` INTEGER, `broker` INTEGER, `insurance` INTEGER, `fund` INTEGER, `misc` INTEGER, `bank` INTEGER, `pension` INTEGER, `privateequity` INTEGER, `sovereign` INTEGER, `corporate` INTEGER, `expatriate` INTEGER"
        self.column = "(date, code, name, close, diff, volume, individual, foreigner, institution, broker, insurance, fund, misc, bank, pension, privateequity, sovereign, corporate, expatriate)"

    def drop_table(self):
        query = "DROP TABLE IF EXISTS " + self.table
        self.cursor.execute(query)
        self.connection.commit()

    def create_table(self):
        query = "CREATE TABLE IF NOT EXISTS " + self.table + "(" + self.schema_ + ", PRIMARY KEY (date, code)" + ")"
        self.cursor.execute(query)
        self.connection.commit()

    def insert_into(self, date, code, name, close, diff, volume, individual, foreigner, institution, broker, insurance, fund, misc, bank, pension, privateequity, sovereign, corporate, expatriate):
        query = "INSERT OR IGNORE INTO " + self.table + self.column + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        self.cursor.execute(query, (date, code, name, close, diff, volume, individual, foreigner, institution, broker, insurance, fund, misc, bank, pension, privateequity, sovereign, corporate, expatriate))

    def delete_from(self, code):
        query = "DELETE FROM " + self.table + " WHERE code = '" + code + "';"
        self.cursor.execute(query)
        self.connection.commit()

    def commit_sqlitekiwoom(self):
        self.connection.commit()

    def get_start_date(self):
        query = "SELECT COUNT(*) FROM " + self.table
        count = pandas.read_sql_query(query, self.connection)
        count = count['COUNT(*)'][0]

        if count:
            query = "SELECT DISTINCT date FROM " + self.table + " ORDER BY date DESC LIMIT 1"
            sdate = pandas.read_sql_query(query, self.connection)['date'][0]
        else:
            sdate = self.sdate.strftime('%Y%m%d')

        return sdate

    def get_end_date(self):
        query = "SELECT COUNT(*) FROM " + self.table
        count = pandas.read_sql_query(query, self.connection)
        count = count['COUNT(*)'][0]

        if count:
            query = "SELECT DISTINCT date FROM " + self.table + " ORDER BY date ASC LIMIT 1"
            edate = pandas.read_sql_query(query, self.connection)['date'][0]
        else:
            edate = self.END_DATE

        return edate

    @dispatch(str)
    def get_table(self, code):
        query = "SELECT * FROM " + self.table + " WHERE (code ='" + code + "' AND volume != 0) ORDER BY date DESC"
        table = pandas.read_sql_query(query, self.connection)

        return table

    @dispatch(str, str)
    def get_table(self, code, sdate):
        query = "SELECT * FROM " + self.table + " WHERE (code = '" + code + "' AND volume != 0 AND date <= " + sdate + ") ORDER BY date DESC"
        table = pandas.read_sql_query(query, self.connection)

        return table


class TableOpt10051_3(SqliteKiwoom):
    def __init__(self):
        # super().__init__()
        super(TableOpt10051_3, self).__init__()
        self.table = "opt10051_3"
        self.schema = "(`date` CHAR, `code` CHAR, `name` CHAR, `close` INTEGER, `diff` INTEGER, `volume` INTEGER, `individual` INTEGER, `foreigner` INTEGER, `institution` INTEGER, `broker` INTEGER, `insurance` INTEGER, `fund` INTEGER, `misc` INTEGER, `bank` INTEGER, `pension` INTEGER, `privateequity` INTEGER, `sovereign` INTEGER, `corporate` INTEGER, `expatriate` INTEGER)"
        self.schema_ = "`date` CHAR, `code` CHAR, `name` CHAR, `close` INTEGER, `diff` INTEGER, `volume` INTEGER, `individual` INTEGER, `foreigner` INTEGER, `institution` INTEGER, `broker` INTEGER, `insurance` INTEGER, `fund` INTEGER, `misc` INTEGER, `bank` INTEGER, `pension` INTEGER, `privateequity` INTEGER, `sovereign` INTEGER, `corporate` INTEGER, `expatriate` INTEGER"
        self.column = "(date, code, name, close, diff, volume, individual, foreigner, institution, broker, insurance, fund, misc, bank, pension, privateequity, sovereign, corporate, expatriate)"

    def drop_table(self):
        query = "DROP TABLE IF EXISTS " + self.table
        self.cursor.execute(query)
        self.connection.commit()

    def create_table(self):
        query = "CREATE TABLE IF NOT EXISTS " + self.table + "(" + self.schema_ + ", PRIMARY KEY (date, code)" + ")"
        self.cursor.execute(query)
        self.connection.commit()

    def insert_into(self, date, code, name, close, diff, volume, individual, foreigner, institution, broker, insurance,
                    fund, misc, bank, pension, privateequity, sovereign, corporate, expatriate):
        query = "INSERT OR IGNORE INTO " + self.table + self.column + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        self.cursor.execute(query, (
        date, code, name, close, diff, volume, individual, foreigner, institution, broker, insurance, fund, misc, bank,
        pension, privateequity, sovereign, corporate, expatriate))

    def delete_from(self, code):
        query = "DELETE FROM " + self.table + " WHERE code = '" + code + "';"
        self.cursor.execute(query)
        self.connection.commit()

    def commit_sqlitekiwoom(self):
        self.connection.commit()

    def get_start_date(self):
        query = "SELECT COUNT(*) FROM " + self.table
        count = pandas.read_sql_query(query, self.connection)
        count = count['COUNT(*)'][0]

        if count:
            query = "SELECT DISTINCT date FROM " + self.table + " ORDER BY date DESC LIMIT 1"
            sdate = pandas.read_sql_query(query, self.connection)['date'][0]
        else:
            sdate = self.sdate.strftime('%Y%m%d')

        return sdate

    def get_end_date(self):
        query = "SELECT COUNT(*) FROM " + self.table
        count = pandas.read_sql_query(query, self.connection)
        count = count['COUNT(*)'][0]

        if count:
            query = "SELECT DISTINCT date FROM " + self.table + " ORDER BY date ASC LIMIT 1"
            edate = pandas.read_sql_query(query, self.connection)['date'][0]
        else:
            edate = self.END_DATE

        return edate

    @dispatch(str)
    def get_table(self, code):
        query = "SELECT * FROM " + self.table + " WHERE date > '20030101' ORDER BY date DESC, code"
        table = pandas.read_sql_query(query, self.connection)

        return table

    @dispatch(str, str)
    def get_table(self, fromdate, todate):
        query = "SELECT * FROM " + self.table + " WHERE date >= '" + fromdate + "' AND date <= '" + todate + "' ORDER BY date DESC, code"
        table = pandas.read_sql_query(query, self.connection)

        return table

    def get_summary(self, fromdate, todate):
        # query = "SELECT code, name, SUM(individual) AS individual, SUM(foreigner) AS foreigner, SUM(broker) AS broker, SUM(insurance) AS insurance, SUM(fund) AS fund, SUM(misc) AS misc, SUM(bank) AS bank, SUM(pension) AS pension, SUM(privateequity) AS privateequity, SUM(sovereign) AS sovereign, SUM(corporate) AS corporate, SUM(expatriate) AS expatriate FROM " + self.table + " WHERE date < '" + sdate +"' GROUP BY code, name ORDER BY code"
        query = "SELECT code, name, SUM(individual) AS individual, SUM(foreigner) AS foreigner, SUM(broker) AS broker, SUM(insurance) AS insurance, SUM(fund) AS fund, SUM(misc) AS misc, SUM(bank) AS bank, SUM(pension) AS pension, SUM(privateequity) AS privateequity, SUM(sovereign) AS sovereign, SUM(corporate) AS corporate, SUM(expatriate) AS expatriate FROM " \
                + self.table + " WHERE date >= '" + fromdate +"' AND date <= '" + todate + "' GROUP BY code, name ORDER BY foreigner DESC"
        table = pandas.read_sql_query(query, self.connection)

        return table


class TableOpt10051_4(SqliteKiwoom):
    def __init__(self):
        # super().__init__()
        super(TableOpt10051_4, self).__init__()
        self.table = "opt10051_4"
        self.schema = "(`date` CHAR, `code` CHAR, `name` CHAR, `close` INTEGER, `diff` INTEGER, `volume` INTEGER, `individual` INTEGER, `foreigner` INTEGER, `institution` INTEGER, `broker` INTEGER, `insurance` INTEGER, `fund` INTEGER, `misc` INTEGER, `bank` INTEGER, `pension` INTEGER, `privateequity` INTEGER, `sovereign` INTEGER, `corporate` INTEGER, `expatriate` INTEGER)"
        self.schema_ = "`date` CHAR, `code` CHAR, `name` CHAR, `close` INTEGER, `diff` INTEGER, `volume` INTEGER, `individual` INTEGER, `foreigner` INTEGER, `institution` INTEGER, `broker` INTEGER, `insurance` INTEGER, `fund` INTEGER, `misc` INTEGER, `bank` INTEGER, `pension` INTEGER, `privateequity` INTEGER, `sovereign` INTEGER, `corporate` INTEGER, `expatriate` INTEGER"
        self.column = "(date, code, name, close, diff, volume, individual, foreigner, institution, broker, insurance, fund, misc, bank, pension, privateequity, sovereign, corporate, expatriate)"

    def drop_table(self):
        query = "DROP TABLE IF EXISTS " + self.table
        self.cursor.execute(query)
        self.connection.commit()

    def create_table(self):
        query = "CREATE TABLE IF NOT EXISTS " + self.table + "(" + self.schema_ + ", PRIMARY KEY (date, code)" + ")"
        self.cursor.execute(query)
        self.connection.commit()

    def insert_into(self, date, code, name, close, diff, volume, individual, foreigner, institution, broker, insurance,
                    fund, misc, bank, pension, privateequity, sovereign, corporate, expatriate):
        query = "INSERT OR IGNORE INTO " + self.table + self.column + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        self.cursor.execute(query, (
        date, code, name, close, diff, volume, individual, foreigner, institution, broker, insurance, fund, misc, bank,
        pension, privateequity, sovereign, corporate, expatriate))

    def delete_from(self, code):
        query = "DELETE FROM " + self.table + " WHERE code = '" + code + "';"
        self.cursor.execute(query)
        self.connection.commit()

    def commit_sqlitekiwoom(self):
        self.connection.commit()

    def get_start_date(self):
        query = "SELECT COUNT(*) FROM " + self.table
        count = pandas.read_sql_query(query, self.connection)
        count = count['COUNT(*)'][0]

        if count:
            query = "SELECT DISTINCT date FROM " + self.table + " ORDER BY date DESC LIMIT 1"
            sdate = pandas.read_sql_query(query, self.connection)['date'][0]
        else:
            sdate = self.sdate.strftime('%Y%m%d')

        return sdate

    def get_end_date(self):
        query = "SELECT COUNT(*) FROM " + self.table
        count = pandas.read_sql_query(query, self.connection)
        count = count['COUNT(*)'][0]

        if count:
            query = "SELECT DISTINCT date FROM " + self.table + " ORDER BY date ASC LIMIT 1"
            edate = pandas.read_sql_query(query, self.connection)['date'][0]
        else:
            edate = self.END_DATE

        return edate

    @dispatch(str)
    def get_table(self, code):
        query = "SELECT * FROM " + self.table + " WHERE (code ='" + code + "' AND volume != 0) ORDER BY date DESC"
        table = pandas.read_sql_query(query, self.connection)

        return table

    @dispatch(str, str)
    def get_table(self, code, sdate):
        query = "SELECT * FROM " + self.table + " WHERE (code = '" + code + "' AND volume != 0 AND date <= " + sdate + ") ORDER BY date DESC"
        table = pandas.read_sql_query(query, self.connection)

        return table


class TableOpt10060_1(SqliteKiwoom):
    def __init__(self):
        # super().__init__()
        super(TableOpt10060_1, self).__init__()
        self.name = "opt10060_1"
        self.schema = "(`code` CHAR, `date` CHAR, `close` INTEGER, `diff` INTEGER, `volume` INTEGER, `individual` INTEGER, `foreigner` INTEGER, `institution` INTEGER, `broker` INTEGER, `insurance` INTEGER, `fund` INTEGER, `misc` INTEGER, `bank` INTEGER, `pension` INTEGER, `privateequity` INTEGER, `sovereign` INTEGER, `corporate` INTEGER, `expatriate` INTEGER)"
        self.schema_ = "`code` CHAR, `date` CHAR, `close` INTEGER, `diff` INTEGER, `volume` INTEGER, `individual` INTEGER, `foreigner` INTEGER, `institution` INTEGER, `broker` INTEGER, `insurance` INTEGER, `fund` INTEGER, `misc` INTEGER, `bank` INTEGER, `pension` INTEGER, `privateequity` INTEGER, `sovereign` INTEGER, `corporate` INTEGER, `expatriate` INTEGER"
        self.column = "(code, date, close, diff, volume, individual, foreigner, institution, broker, insurance, fund, misc, bank, pension, privateequity, sovereign, corporate, expatriate)"

    def drop_table(self):
        query = "DROP TABLE IF EXISTS " + self.name
        self.cursor.execute(query)
        self.connection.commit()

    def create_table(self):
        query = "CREATE TABLE IF NOT EXISTS " + self.name + "(" + self.schema_ + ", PRIMARY KEY (code, date)" + ")"
        self.cursor.execute(query)
        self.connection.commit()

    def insert_into(self, code, date, close, diff, volume, individual, foreigner, institution, broker, insurance, fund, misc, bank, pension, privateequity, sovereign, corporate, expatriate):
        query = "INSERT OR IGNORE INTO " + self.name + self.column + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        self.cursor.execute(query, (code, date, close, diff, volume, individual, foreigner, institution, broker, insurance, fund, misc, bank, pension, privateequity, sovereign, corporate, expatriate))

    def delete_from(self, code):
        query = "DELETE FROM " + self.name + " WHERE code='" + code + "';"
        self.cursor.execute(query)
        self.connection.commit()

    def commit_sqlitekiwoom(self):
        self.connection.commit()

    @dispatch(str)
    def get_table(self, code):
        query = "SELECT * FROM " + self.name + " WHERE (code ='" + code + "' AND volume != 0) ORDER BY date DESC"
        table = pandas.read_sql_query(query, self.connection)

        return table

    @dispatch(str, str)
    def get_table(self, code, todate):
        query = "SELECT * FROM " + self.name + " WHERE (code = '" + code + "' AND volume != 0 AND date <= " + todate + ") ORDER BY date DESC"
        table = pandas.read_sql_query(query, self.connection)

        return table

    @dispatch(str, str, str)
    def get_table(self, code, fromdate, todate):
        # query = "SELECT * FROM " + self.name + " WHERE (code = '" + code + "' AND volume != 0 AND date >= " + fromdate + " AND date <= " + todate + ") ORDER BY date DESC"
        query = "SELECT * FROM " + self.name + " WHERE (code = '" + code + "' AND date >= " + fromdate + " AND date <= " + todate + ") ORDER BY date DESC"
        table = pandas.read_sql_query(query, self.connection)

        return table


class TableOpt10060_2(SqliteKiwoom):
    def __init__(self):
        super(TableOpt10060_2, self).__init__()
        self.name = "opt10060_2"
        self.schema = "(`code` CHAR, `date` CHAR, `close` INTEGER, `diff` INTEGER, `volume` INTEGER, `individual` INTEGER, `foreigner` INTEGER, `institution` INTEGER, `broker` INTEGER, `insurance` INTEGER, `fund` INTEGER, `misc` INTEGER, `bank` INTEGER, `pension` INTEGER, `privateequity` INTEGER, `sovereign` INTEGER, `corporate` INTEGER, `expatriate` INTEGER)"
        self.schema_ = "`code` CHAR, `date` CHAR, `close` INTEGER, `diff` INTEGER, `volume` INTEGER, `individual` INTEGER, `foreigner` INTEGER, `institution` INTEGER, `broker` INTEGER, `insurance` INTEGER, `fund` INTEGER, `misc` INTEGER, `bank` INTEGER, `pension` INTEGER, `privateequity` INTEGER, `sovereign` INTEGER, `corporate` INTEGER, `expatriate` INTEGER"
        self.column = "(code, date, close, diff, volume, individual, foreigner, institution, broker, insurance, fund, misc, bank, pension, privateequity, sovereign, corporate, expatriate)"

    def drop_table(self):
        query = "DROP TABLE IF EXISTS " + self.name
        self.cursor.execute(query)
        self.connection.commit()

    def create_table(self):
        query = "CREATE TABLE IF NOT EXISTS " + self.name + "(" + self.schema_ + ", PRIMARY KEY (code, date)" + ")"
        self.cursor.execute(query)
        self.connection.commit()

    def insert_into(self, code, date, close, diff, volume, individual, foreigner, institution, broker, insurance, fund, misc, bank, pension, privateequity, sovereign, corporate, expatriate):
        query = "INSERT OR IGNORE INTO " + self.name + self.column + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        self.cursor.execute(query, (code, date, close, diff, volume, individual, foreigner, institution, broker, insurance, fund, misc, bank, pension, privateequity, sovereign, corporate, expatriate))

    def delete_from(self, code):
        query = "DELETE FROM " + self.name + " WHERE code='" + code + "';"
        self.cursor.execute(query)
        self.connection.commit()

    def commit_sqlitekiwoom(self):
        self.connection.commit()

    @dispatch(str)
    def get_table(self, code):
        query = "SELECT * FROM " + self.name + " WHERE (code ='" + code + "' AND volume != 0) ORDER BY date DESC"
        table = pandas.read_sql_query(query, self.connection)

        return table

    @dispatch(str, str)
    def get_table(self, code, sdate):
        query = "SELECT * FROM " + self.name + " WHERE (code = '" + code + "' AND volume != 0 AND date <= " + sdate + ") ORDER BY date DESC"
        table = pandas.read_sql_query(query, self.connection)

        return table

    @dispatch(str, str, str)
    def get_table(self, code, fromdate, todate):
        # query = "SELECT * FROM " + self.name + " WHERE (code = '" + code + "' AND volume != 0 AND date >= " + fromdate + " AND date <= " + todate + ") ORDER BY date DESC"
        query = "SELECT * FROM " + self.name + " WHERE (code = '" + code + "' AND date >= " + fromdate + " AND date <= " + todate + ") ORDER BY date DESC"
        table = pandas.read_sql_query(query, self.connection)

        return table


class TableOpt10081(SqliteKiwoom):
    def __init__(self):
        super().__init__()
        self.name = "opt10081"
        self.schema = "(`code` CHAR, `date` CHAR, `volume` INTEGER, `value` INTEGER, `open` INTEGER, `high` INTEGER, `low` INTEGER, `price` INTEGER, `adjust` INTEGER, `sector` INTEGER, `subsector` INTEGER, `info` CHAR, `adjust_` INTEGER, `close_yesterday` INTEGER)"
        self.schema_ = "`code` CHAR, `date` CHAR, `volume` INTEGER, `value` INTEGER, `open` INTEGER, `high` INTEGER, `low` INTEGER, `price` INTEGER, `adjust` INTEGER, `sector` INTEGER, `subsector` INTEGER, `info` CHAR, `adjust_` INTEGER, `close_yesterday` INTEGER"
        self.column = "(code, date, volume, value, open, high, low, price, adjust, sector, subsector, info, adjust_, close_yesterday)"

    def drop_table(self):
        query = "DROP TABLE IF EXISTS " + self.name + ";"
        self.cursor.execute(query)
        self.connection.commit()

    def create_table(self):
        query = "CREATE TABLE IF NOT EXISTS " + self.name + "(" + self.schema_ + ", PRIMARY KEY (code, date)" + ")"
        self.cursor.execute(query)
        self.connection.commit()

    def insert_into(self, code, date, volume, value, open, high, low, price, adjust, sector, subsector, info, adjust_, close_yesterday):
        query = "INSERT OR IGNORE INTO " + self.name + self.column + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        self.cursor.execute(query, (code, date, volume, value, open, high, low, price, adjust, sector, subsector, info, adjust_, close_yesterday))

    def delete_from(self, code):
        query = "DELETE FROM " + self.name + " WHERE code='" + code + "';"
        self.cursor.execute(query)
        self.connection.commit()

    def commit_sqlitekiwoom(self):
        self.connection.commit()

    def get_table(self, code, fromdate, todate):
        query = "SELECT * FROM " + self.name + " WHERE code = '" + code + "' AND date >= " + fromdate + " AND date <= " + todate + " ORDER BY date DESC"
        table = pandas.read_sql_query(query, self.connection)

        return table


class Logic(SqliteKiwoom):
    def __init__(self):
        super().__init__()


class LogicCode(Logic):
    def __init__(self):
        super(LogicCode, self).__init__()

    def code_pick_1(self, fromdate, todate):
        query = "SELECT code, name, sector, SUM(individual) AS Individual, SUM(foreigner) AS RegdFrgnr, " + \
                "SUM(institution) AS Institutions, SUM(broker) AS SecFutures, SUM(insurance) AS Insurance, SUM(fund) AS InvestmentTrust, " + \
                "SUM(misc) AS MerchantBanks, SUM(bank) AS Banks, SUM(pension) AS FundPensions, " + \
                "SUM(privateequity) AS PrivateEquity, SUM(sovereign) AS GovtInternationalOrg, " + \
                "SUM(corporate) AS OtherCorp, SUM(expatriate) AS NonRegdFrgnr " + \
                "FROM (SELECT * FROM opt10060_1 LEFT JOIN masterstockinfo ON opt10060_1.code = masterstockinfo.code) t " + \
                "WHERE exchange = '장내' AND date >= '" + fromdate + "' " + "AND date <= '" + todate + "' " + \
                "GROUP BY t.code " + \
                "ORDER BY RegdFrgnr DESC " + \
                "LIMIT 30;"
        view = pandas.read_sql_query(query, self.connection)

        return view

    def code_pick_2(self, fromdate, todate):
        query = "SELECT code, name, sector, SUM(individual) AS Individual, SUM(foreigner) AS RegdFrgnr, " + \
                "SUM(institution) AS Institutions, SUM(broker) AS SecFutures, SUM(insurance) AS Insurance, SUM(fund) AS InvestmentTrust, " + \
                "SUM(misc) AS MerchantBanks, SUM(bank) AS Banks, SUM(pension) AS FundPensions, " + \
                "SUM(privateequity) AS PrivateEquity, SUM(sovereign) AS GovtInternationalOrg, " + \
                "SUM(corporate) AS OtherCorp, SUM(expatriate) AS NonRegdFrgnr " + \
                "FROM (SELECT * FROM opt10060_1 LEFT JOIN masterstockinfo ON opt10060_1.code = masterstockinfo.code WHERE masterstockinfo.exchange = '코스닥') t " + \
                "WHERE date >= '" + fromdate + "' " + "AND date <= '" + todate + "' " + \
                "GROUP BY t.code " + \
                "ORDER BY RegdFrgnr DESC " + \
                "LIMIT 30;"
        view = pandas.read_sql_query(query, self.connection)

        return view

if __name__ == '__main__':
    pass





