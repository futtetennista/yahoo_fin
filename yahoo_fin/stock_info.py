import requests
import pandas as pd
import ftplib
import io
import re
import json
import datetime
import sys

try:
    from requests_html import HTMLSession
except Exception:
    print("""Warning - Certain functionality
             requires requests_html, which is not installed.

             Install using:
             pip install requests_html

             After installation, you may have to restart your Python session.""")


base_url = "https://query1.finance.yahoo.com/v8/finance/chart/"

def build_url(ticker, start_date = None, end_date = None, interval = "1d"):

    if end_date is None:
        end_seconds = int(pd.Timestamp("now").timestamp())

    else:
        end_seconds = int(pd.Timestamp(end_date).timestamp())

    if start_date is None:
        start_seconds = 7223400

    else:
        start_seconds = int(pd.Timestamp(start_date).timestamp())

    site = base_url + ticker

    params = {"period1": start_seconds, "period2": end_seconds,
              "interval": interval.lower(), "events": "div,splits"}


    return site, params


def force_float(elt):

    try:
        return float(elt)
    except:
        return elt

def _convert_to_numeric(s):

    if "M" in s:
        s = s.strip("M")
        return force_float(s) * 1_000_000

    if "B" in s:
        s = s.strip("B")
        return force_float(s) * 1_000_000_000

    return force_float(s)


def get_data(ticker, start_date = None, end_date = None, index_as_date = True,
             interval = "1d", headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
):
    '''Downloads historical stock price data into a pandas data frame.  Interval
       must be "1d", "1wk", "1mo", or "1m" for daily, weekly, monthly, or minute data.
       Intraday minute data is limited to 7 days.

       @param: ticker
       @param: start_date = None
       @param: end_date = None
       @param: index_as_date = True
       @param: interval = "1d"
    '''

    if interval not in ("1d", "1wk", "1mo", "1m"):
        raise AssertionError("interval must be of of '1d', '1wk', '1mo', or '1m'")


    # build and connect to URL
    site, params = build_url(ticker, start_date, end_date, interval)
    resp = requests.get(site, params = params, headers = headers)


    if not resp.ok:
        raise AssertionError(resp.json())


    # get JSON response
    data = resp.json()

    # get open / high / low / close data
    frame = pd.DataFrame(data["chart"]["result"][0]["indicators"]["quote"][0])

    # get the date info
    temp_time = data["chart"]["result"][0]["timestamp"]

    if interval != "1m":

        # add in adjclose
        frame["adjclose"] = data["chart"]["result"][0]["indicators"]["adjclose"][0]["adjclose"]
        frame.index = pd.to_datetime(temp_time, unit = "s")
        frame.index = frame.index.map(lambda dt: dt.floor("d"))
        frame = frame[["open", "high", "low", "close", "adjclose", "volume"]]

    else:

        frame.index = pd.to_datetime(temp_time, unit = "s")
        frame = frame[["open", "high", "low", "close", "volume"]]


    frame['ticker'] = ticker.upper()

    if not index_as_date:
        frame = frame.reset_index()
        frame.rename(columns = {"index": "date"}, inplace = True)

    return frame



def tickers_sp500(include_company_data = False):
    '''Downloads list of tickers currently listed in the S&P 500 '''
    # get list of all S&P 500 stocks
    sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
    sp500["Symbol"] = sp500["Symbol"].str.replace(".", "-", regex=True)

    if include_company_data:
        return sp500

    sp_tickers = sp500.Symbol.tolist()
    sp_tickers = sorted(sp_tickers)

    return sp_tickers


def tickers_nasdaq(include_company_data = False):

    '''Downloads list of tickers currently listed in the NASDAQ'''

    ftp = ftplib.FTP("ftp.nasdaqtrader.com")
    ftp.login()
    ftp.cwd("SymbolDirectory")

    r = io.BytesIO()
    ftp.retrbinary('RETR nasdaqlisted.txt', r.write)

    if include_company_data:
        r.seek(0)
        data = pd.read_csv(r, sep = "|")
        return data

    info = r.getvalue().decode()
    splits = info.split("|")


    tickers = [x for x in splits if "\r\n" in x]
    tickers = [x.split("\r\n")[1] for x in tickers if "NASDAQ" not in x != "\r\n"]
    tickers = [ticker for ticker in tickers if "File" not in ticker]

    ftp.close()

    return tickers



def tickers_other(include_company_data = False):
    '''Downloads list of tickers currently listed in the "otherlisted.txt"
       file on "ftp.nasdaqtrader.com" '''
    ftp = ftplib.FTP("ftp.nasdaqtrader.com")
    ftp.login()
    ftp.cwd("SymbolDirectory")

    r = io.BytesIO()
    ftp.retrbinary('RETR otherlisted.txt', r.write)

    if include_company_data:
        r.seek(0)
        data = pd.read_csv(r, sep = "|")
        return data

    info = r.getvalue().decode()
    splits = info.split("|")

    tickers = [x for x in splits if "\r\n" in x]
    tickers = [x.split("\r\n")[1] for x in tickers]
    tickers = [ticker for ticker in tickers if "File" not in ticker]

    ftp.close()

    return tickers


def tickers_dow(include_company_data = False):

    '''Downloads list of currently traded tickers on the Dow'''

    site = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"

    table = pd.read_html(site, attrs = {"id":"constituents"})[0]

    if include_company_data:
        return table

    dow_tickers = sorted(table['Symbol'].tolist())

    return dow_tickers


def tickers_ibovespa(include_company_data = False):

    '''Downloads list of currently traded tickers on the Ibovespa, Brazil'''

    table = pd.read_html("https://pt.wikipedia.org/wiki/Lista_de_companhias_citadas_no_Ibovespa")[0]
    table.columns = ["Symbol", "Share", "Sector", "Type", "Site"]

    if include_company_data:
        return table

    ibovespa_tickers = sorted(table.Symbol.tolist())

    return ibovespa_tickers



def tickers_nifty50(include_company_data = False, headers = {'User-agent': 'Mozilla/5.0'}):

    '''Downloads list of currently traded tickers on the NIFTY 50, India'''

    site = "https://finance.yahoo.com/quote/%5ENSEI/components?p=%5ENSEI"
    table = pd.read_html(requests.get(site, headers=headers).text)[0]

    if include_company_data:
        return table

    nifty50 = sorted(table['Symbol'].tolist())

    return nifty50

def tickers_niftybank():
    ''' Currently traded tickers on the NIFTY BANK, India '''

    niftybank = ['AXISBANK', 'KOTAKBANK', 'HDFCBANK', 'SBIN', 'BANKBARODA', 'INDUSINDBK', 'PNB', 'IDFCFIRSTB', 'ICICIBANK', 'RBLBANK', 'FEDERALBNK', 'BANDHANBNK']

    return niftybank

def tickers_ftse_all_share():

    '''Downloads a list of the tickers traded on the FTSE'''

    def mk_ftse_all_share_url(page):
        return f"https://www.londonstockexchange.com/indices/ftse-all-share/constituents/table?lang=en&page={page}"
    table = pd.DataFrame()
    for page in range(1,32):
        page = pd.read_html(mk_ftse_all_share_url(page)[0])
        table.append(page)

    # if include_company_data:
    #     return table

    # return sorted(table.loc[:, ['CodeA-ZZ-A']]).tolist())

    return sorted(table)

def tickers_ftse100(include_company_data = False):

    '''Downloads a list of the tickers traded on the FTSE 100 index'''

    # def mk_ftse_350_share_url(page):
    #     return f"https://www.londonstockexchange.com/indices/ftse-100/constituents/table?lang=en&page={page}"
    # table = pd.DataFrame()
    # for page in range(1,7):
    #     page = pd.read_html(mk_ftse_100_share_url(page)[0])
    #     table.append(page)

    # if include_company_data:
    #     return table

    # return sorted(table.loc[:, ['CodeA-ZZ-A', 'NameA-ZZ-A']]).tolist())

    table = pd.read_html("https://en.wikipedia.org/wiki/FTSE_100_Index", attrs = {"id": "constituents"})[0]

    if include_company_data:
        return table

    return sorted(table.EPIC.tolist())


def tickers_ftse250(include_company_data = False):

    '''Downloads a list of the tickers traded on the FTSE 250 index'''

    table = pd.read_html("https://en.wikipedia.org/wiki/FTSE_250_Index", attrs = {"id": "constituents"})[0]

    table.columns = ["Company", "Ticker"]

    if include_company_data:
        return table

    return sorted(table.Ticker.tolist())

def get_quote_table(ticker , dict_result = True, headers = {'User-agent': 'Mozilla/5.0'}):

    '''Scrapes data elements found on Yahoo Finance's quote page
       of input ticker

       @param: ticker
       @param: dict_result = True
    '''

    site = "https://finance.yahoo.com/quote/" + ticker + "?p=" + ticker

    tables = pd.read_html(requests.get(site, headers=headers).text)

    data = tables[0].append(tables[1])

    data.columns = ["attribute" , "value"]

    quote_price = pd.DataFrame(["Quote Price", get_live_price(ticker)]).transpose()
    quote_price.columns = data.columns.copy()

    data = data.append(quote_price)

    data = data.sort_values("attribute")

    data = data.drop_duplicates().reset_index(drop = True)

    data["value"] = data.value.map(force_float)

    if dict_result:

        result = {key : val for key,val in zip(data.attribute , data.value)}
        return result

    return data


def get_stats(ticker, headers = {'User-agent': 'Mozilla/5.0'}):

    '''Scrapes information from the statistics tab on Yahoo Finance
       for an input ticker

       @param: ticker
    '''

    stats_site = "https://finance.yahoo.com/quote/" + ticker + \
                 "/key-statistics?p=" + ticker


    tables = pd.read_html(requests.get(stats_site, headers=headers).text)

    tables = [table for table in tables[1:] if table.shape[1] == 2]

    table = tables[0]
    for elt in tables[1:]:
        table = table.append(elt)

    table.columns = ["Attribute" , "Value"]

    table = table.reset_index(drop = True)

    return table


def get_stats_valuation(ticker, headers = {'User-agent': 'Mozilla/5.0'}):

    '''Scrapes Valuation Measures table from the statistics tab on Yahoo Finance
       for an input ticker

       @param: ticker
    '''

    stats_site = "https://finance.yahoo.com/quote/" + ticker + \
                 "/key-statistics?p=" + ticker


    tables = pd.read_html(requests.get(stats_site, headers=headers).text)

    tables = [table for table in tables if "Trailing P/E" in table.iloc[:,0].tolist()]


    table = tables[0].reset_index(drop = True)

    return table


def _parse_json(url, headers = {'User-agent': 'Mozilla/5.0'}):
    html = requests.get(url=url, headers = headers).text

    json_str = html.split('root.App.main =')[1].split('(this)')[0].split(';\n}')[0].strip()

    try:
        data = json.loads(json_str)['context']['dispatcher']['stores']['QuoteSummaryStore']
    except:
        print(sys.exc_info())
        print(json.loads(json_str)['context']['dispatcher']['stores'].keys())
        raise ValueError('Cannot find QuoteSummaryStore')
    else:
        # return data
        new_data = json.dumps(data).replace('{}', 'null')
        new_data = re.sub(r'\{[\'|\"]raw[\'|\"]:(.*?),(.*?)\}', r'\1', new_data)

        json_info = json.loads(new_data)

        return json_info


def _parse_table(json_info):

    df = pd.DataFrame(json_info)

    if df.empty:
        return df

    del df["maxAge"]

    df.set_index("endDate", inplace=True)
    df.index = pd.to_datetime(df.index, unit="s")

    df = df.transpose()
    df.index.name = "Breakdown"

    return df


def get_income_statement(ticker, country = "UK", yearly = True):

    '''Scrape income statement from Yahoo Finance for a given ticker

       @param: ticker
    '''

    subdomain, ticker_ = ("uk.", f"{ticker}.l")  if country == "UK" else ("", ticker)
    income_statement_page = f"https://{subdomain}finance.yahoo.com/quote/{ticker_}/financials?p={ticker_}"
    json_info = _parse_json(income_statement_page)

    if yearly:
        try:
            temp = json_info["incomeStatementHistory"]["incomeStatementHistory"]
        except:
            print(json_info)
    else:
        temp = json_info["incomeStatementHistoryQuarterly"]["incomeStatementHistory"]

    return _parse_table(temp)


def get_balance_sheet(ticker, yearly = True):

    '''Scrapes balance sheet from Yahoo Finance for an input ticker

       @param: ticker
    '''

    balance_sheet_site = "https://finance.yahoo.com/quote/" + ticker + \
                         "/balance-sheet?p=" + ticker

    json_info = _parse_json(balance_sheet_site)

    try:
        if yearly:
            temp = json_info["balanceSheetHistory"]["balanceSheetStatements"]
        else:
            temp = json_info["balanceSheetHistoryQuarterly"]["balanceSheetStatements"]
    except:
        e = sys.exc_info()[0]
        print(e)
        temp = []

    return _parse_table(temp)

def calculate_magic_formula(ticker, country = "UK", year = "2020"):

    '''
    Joel Greenblatt's 'Magic Formula' as illustrated in his book
    'The litte book that still beats the market'
    '''

    income_statement = get_income_statement(ticker, country)
    balance_sheet_insights = get_balance_sheet_insights(ticker, country)

    ebit = income_statement.loc['ebit'][0]
    print(f"ebit: {ebit}")
    tangible_capital_employed = calculate_tangible_capital_employed(balance_sheet_insights, year)
    print(f"tangible_capital_employed: {tangible_capital_employed}")
    net_working_capital = calculate_net_working_capital(balance_sheet_insights, year)
    print(f"net_working_capital: {net_working_capital}")

    if tangible_capital_employed == 0 and net_working_capital == 0:
        print("Cannot calculate magic formula: denominator in the formula is 0")
    else:
        return ebit / (tangible_capital_employed + net_working_capital), income_statement, balance_sheet_insights

# def calculate_ebit(income_statement, year='2020'):
#
#     '''
#     Take the total revenue (that is, all the money that the firm brings in)
#     and then remove all of the firm’s operating expenses.
#     This includes line items such as the cost of goods and production,
#     salaries, rent and associated overhead and depreciation and amortization.
#
#     Another way of thinking about this is to calculate the firm’s net income,
#     then add back interest payments and tax payments.
#     What you have left represents the operating income of the firm.
#     In other words, its total revenue reduced by its costs to stay in business.
#     '''
#
#     net_income = income_statement.loc['netIncome'].filter(regex=f"{year}.*")[0]
#     taxes = income_statement.loc['incomeTaxExpense'].filter(regex=f"{year}.*")[0]
#     # interest = - income_statement.loc['interestExpense'].filter(regex=f"{year}.*")[0]
#     other = - income_statement.loc['totalOtherIncomeExpenseNet'].filter(regex=f"{year}.*")[0]
#     # print(net_income)
#     # print(taxes)
#     # print(interest)
#     # print(other)
#     return net_income + taxes + other
#     # return net_income + taxes + interest + other

def calculate_tangible_capital_employed(balance_sheet_insights, year = '2020'):
    total_fixed_assets = balance_sheet_insights.loc['annualTotalNonCurrentAssets'][0]
    intangible_assets = balance_sheet_insights.loc['annualOtherIntangibleAssets'][0]
    if 'annualGoodwill' in balance_sheet_insights.columns:
        goodwill = balance_sheet_insights.loc['annualGoodwill'][0]
    else:
        goodwill = 0
    print(f"total_fixed_assets: {total_fixed_assets}")
    print(f"intangible_assets: {intangible_assets}")
    print(f"goodwill: {goodwill}")
    return total_fixed_assets - intangible_assets - goodwill

def calculate_net_working_capital(balance_sheet_insights, year = '2020'):

    '''
    There are at least 2 formulas to calculate this and
    there's no general agreement on which one is superior.
    The formula used here is:

    Net Working Capital = Accounts Receivable + Inventory – Accounts Payable
    '''

    accounts_receivable = balance_sheet_insights.loc['annualAccountsReceivable'][0]
    inventory = balance_sheet_insights.loc['annualInventory'][0]
    accounts_payable = balance_sheet_insights.loc['annualAccountsPayable'][0]
    print(f"accounts_receivable: {accounts_receivable}")
    print(f"inventory: {inventory}")
    print(f"accounts_payable: {accounts_payable}")

    return accounts_receivable + inventory - accounts_payable

def get_balance_sheet_insights(ticker, country = "UK", yearly = True):
    subdomain, ticker_ = ("uk.", f"{ticker}.l")  if country == "UK" else ("", ticker)
    balance_sheet_page = f"https://{subdomain}finance.yahoo.com/quote/{ticker_}/balance-sheet?p={ticker_}"

    json_info = _parse_json_series(balance_sheet_page, yearly = yearly)
    return _parse_time_series_table(json_info)

def _parse_json_series(url, headers = {'User-agent': 'Mozilla/5.0'}, yearly = True):
    html = requests.get(url=url, headers = headers).text
    json_str = html.split('root.App.main =')[1].split('(this)')[0].split(';\n}')[0].strip()

    try:
        data = json.loads(json_str)['context']['dispatcher']['stores']['QuoteTimeSeriesStore']
        clean_data = json.dumps(data).replace('{}', 'null')
        clean_data = re.sub(r'\{[\'|\"]raw[\'|\"]:(.*?),(.*?)\}', r'\1', clean_data)

        if yearly:
            json_dict = json.loads(clean_data)['timeSeries']
        else:
            json_dict = json.loads(clean_data)["timeSeriesQuarterly"]
        del json_dict['timestamp']

        def list_to_obj(json_obj):
            empty_obj = {}
            if json_obj != []:
                for i in range(0, len(json_obj)):
                    years = json_obj[i]
                    if years != None:
                        empty_obj[years['asOfDate']] = years['reportedValue']
            return empty_obj
            # return { k: json_obj[k] if json_obj != None else None for k in ['asOfDate', 'reportedValue'] }
        # obj = {}
        # for k in json_dict.keys():
        #     print(f"Looking at key '{k}'")
        #     if json_dict.get(k, None) == None or json_dict.get(k, None) == []:
        #         print("None or empty")
        #     else:
        #         print("Has elements")
        #         obj[k] = filter_data(json_dict[k])
        # return obj
        return { k: list_to_obj(json_dict[k]) for k in json_dict.keys() }

    except:
        print(sys.exc_info())

def _parse_time_series_table(json_info):
    df = pd.read_json(json.dumps(json_info), orient='index')
    # TODO: is this actually safe?
    df.fillna(0)
    df = df[df.columns.sort_values(ascending=False)]
    return df

    # if df.empty:
    #     return df

    # df.set_index("asOfDate", inplace=True)
    # df.index = pd.to_datetime(df.index)

    # df = df.transpose()
    # # # df.index.name = "Breakdown"

    # return df

def get_cash_flow(ticker, yearly = True):

    '''Scrapes the cash flow statement from Yahoo Finance for an input ticker

       @param: ticker
    '''

    cash_flow_site = "https://finance.yahoo.com/quote/" + \
            ticker + "/cash-flow?p=" + ticker


    json_info = _parse_json(cash_flow_site)

    if yearly:
        temp = json_info["cashflowStatementHistory"]["cashflowStatements"]
    else:
        temp = json_info["cashflowStatementHistoryQuarterly"]["cashflowStatements"]

    return _parse_table(temp)


def get_financials(ticker, yearly = True, quarterly = True):

    '''Scrapes financials data from Yahoo Finance for an input ticker, including
       balance sheet, cash flow statement, and income statement.  Returns dictionary
       of results.

       @param: ticker
       @param: yearly = True
       @param: quarterly = True
    '''

    if not yearly and not quarterly:
        raise AssertionError("yearly or quarterly must be True")

    financials_site = "https://finance.yahoo.com/quote/" + ticker + \
            "/financials?p=" + ticker

    json_info = _parse_json(financials_site)

    result = {}

    if yearly:

        temp = json_info["incomeStatementHistory"]["incomeStatementHistory"]
        table = _parse_table(temp)
        result["yearly_income_statement"] = table

        temp = json_info["balanceSheetHistory"]["balanceSheetStatements"]
        table = _parse_table(temp)
        result["yearly_balance_sheet"] = table

        temp = json_info["cashflowStatementHistory"]["cashflowStatements"]
        table = _parse_table(temp)
        result["yearly_cash_flow"] = table

    if quarterly:
        temp = json_info["incomeStatementHistoryQuarterly"]["incomeStatementHistory"]
        table = _parse_table(temp)
        result["quarterly_income_statement"] = table

        temp = json_info["balanceSheetHistoryQuarterly"]["balanceSheetStatements"]
        table = _parse_table(temp)
        result["quarterly_balance_sheet"] = table

        temp = json_info["cashflowStatementHistoryQuarterly"]["cashflowStatements"]
        table = _parse_table(temp)
        result["quarterly_cash_flow"] = table


    return result


def get_holders(ticker, headers = {'User-agent': 'Mozilla/5.0'}):

    '''Scrapes the Holders page from Yahoo Finance for an input ticker

       @param: ticker
    '''

    holders_site = "https://finance.yahoo.com/quote/" + \
                    ticker + "/holders?p=" + ticker


    tables = pd.read_html(requests.get(holders_site, headers=headers).text)


    table_names = ["Major Holders" , "Direct Holders (Forms 3 and 4)" ,
                   "Top Institutional Holders" , "Top Mutual Fund Holders"]


    table_mapper = {key : val for key,val in zip(table_names , tables)}


    return table_mapper

def get_analysts_info(ticker, headers = {'User-agent': 'Mozilla/5.0'}):

    '''Scrapes the Analysts page from Yahoo Finance for an input ticker

       @param: ticker
    '''


    analysts_site = "https://finance.yahoo.com/quote/" + ticker + \
                     "/analysts?p=" + ticker

    tables = pd.read_html(requests.get(analysts_site, headers=headers).text)

    table_names = [table.columns[0] for table in tables]

    table_mapper = {key : val for key , val in zip(table_names , tables)}


    return table_mapper


def get_live_price(ticker):

    '''Gets the live price of input ticker

       @param: ticker
    '''

    df = get_data(ticker, end_date = pd.Timestamp.today() + pd.DateOffset(10))


    return df.close[-1]


def _raw_get_daily_info(site):

    session = HTMLSession()

    resp = session.get(site)

    tables = pd.read_html(resp.html.raw_html)

    df = tables[0].copy()

    df.columns = tables[0].columns

    del df["52 Week Range"]

    df["% Change"] = df["% Change"].map(lambda x: float(x.strip("%+").replace(",", "")))


    fields_to_change = [x for x in df.columns.tolist() if "Vol" in x \
                        or x == "Market Cap"]

    for field in fields_to_change:

        if type(df[field][0]) == str:
            df[field] = df[field].map(_convert_to_numeric)

    session.close()

    return df


def get_day_most_active(count: int = 100):

    return _raw_get_daily_info(f"https://finance.yahoo.com/most-active?offset=0&count={count}")


def get_day_gainers(count: int = 100):

    return _raw_get_daily_info(f"https://finance.yahoo.com/gainers?offset=0&count={count}")


def get_day_losers(count: int = 100):

    return _raw_get_daily_info(f"https://finance.yahoo.com/losers?offset=0&count={count}")


def get_top_crypto():

    '''Gets the top 100 Cryptocurrencies by Market Cap'''

    session = HTMLSession()

    resp = session.get("https://finance.yahoo.com/cryptocurrencies?offset=0&count=100")

    tables = pd.read_html(resp.html.raw_html)

    df = tables[0].copy()


    df["% Change"] = df["% Change"].map(lambda x: float(str(x).strip("%").\
                                                               strip("+").\
                                                               replace(",", "")))
    del df["52 Week Range"]
    del df["1 Day Chart"]

    fields_to_change = [x for x in df.columns.tolist() if "Volume" in x \
                        or x == "Market Cap" or x == "Circulating Supply"]

    for field in fields_to_change:

        if type(df[field][0]) == str:
            df[field] = df[field].map(lambda x: _convert_to_numeric(str(x)))


    session.close()

    return df


def get_dividends(ticker, start_date = None, end_date = None, index_as_date = True,
                  headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
):
    '''Downloads historical dividend data into a pandas data frame.

       @param: ticker
       @param: start_date = None
       @param: end_date = None
       @param: index_as_date = True
    '''

    # build and connect to URL
    site, params = build_url(ticker, start_date, end_date, "1d")
    resp = requests.get(site, params = params, headers = headers)


    if not resp.ok:
        return pd.DataFrame()


    # get JSON response
    data = resp.json()

    # check if there is data available for dividends
    if "events" not in data["chart"]["result"][0] or "dividends" not in data["chart"]["result"][0]['events']:
        return pd.DataFrame()

    # get the dividend data
    frame = pd.DataFrame(data["chart"]["result"][0]['events']['dividends'])

    frame = frame.transpose()

    frame.index = pd.to_datetime(frame.index, unit = "s")
    frame.index = frame.index.map(lambda dt: dt.floor("d"))

    # sort in chronological order
    frame = frame.sort_index()

    frame['ticker'] = ticker.upper()

    # remove old date column
    frame = frame.drop(columns='date')

    frame = frame.rename({'amount': 'dividend'}, axis = 'columns')

    if not index_as_date:
        frame = frame.reset_index()
        frame.rename(columns = {"index": "date"}, inplace = True)

    return frame



def get_splits(ticker, start_date = None, end_date = None, index_as_date = True,
               headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
):
    '''Downloads historical stock split data into a pandas data frame.

       @param: ticker
       @param: start_date = None
       @param: end_date = None
       @param: index_as_date = True
    '''

    # build and connect to URL
    site, params = build_url(ticker, start_date, end_date, "1d")
    resp = requests.get(site, params = params, headers = headers)


    if not resp.ok:
        raise AssertionError(resp.json())


    # get JSON response
    data = resp.json()

    # check if there is data available for splits
    if "splits" not in data["chart"]["result"][0]['events']:
        raise AssertionError("There is no data available on stock splits, or none have occured")

    # get the split data
    frame = pd.DataFrame(data["chart"]["result"][0]['events']['splits'])

    frame = frame.transpose()

    frame.index = pd.to_datetime(frame.index, unit = "s")
    frame.index = frame.index.map(lambda dt: dt.floor("d"))

    # sort in to chronological order
    frame = frame.sort_index()

    frame['ticker'] = ticker.upper()

    # remove unnecessary columns
    frame = frame.drop(columns=['date', 'denominator', 'numerator'])

    if not index_as_date:
        frame = frame.reset_index()
        frame.rename(columns = {"index": "date"}, inplace = True)

    return frame




def get_earnings(ticker):

    '''Scrapes earnings data from Yahoo Finance for an input ticker

       @param: ticker
    '''

    result = {
        "quarterly_results": pd.DataFrame(),
        "yearly_revenue_earnings": pd.DataFrame(),
        "quarterly_revenue_earnings": pd.DataFrame()
    }

    financials_site = "https://finance.yahoo.com/quote/" + ticker + \
        "/financials?p=" + ticker

    json_info = _parse_json(financials_site)

    if "earnings" not in json_info:
        return result

    temp = json_info["earnings"]

    if temp == None:
        return result

    result["quarterly_results"] = pd.DataFrame.from_dict(temp["earningsChart"]["quarterly"])

    result["yearly_revenue_earnings"] = pd.DataFrame.from_dict(temp["financialsChart"]["yearly"])

    result["quarterly_revenue_earnings"] = pd.DataFrame.from_dict(temp["financialsChart"]["quarterly"])

    return result



### Earnings functions
def _parse_earnings_json(url, headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
):
        resp = requests.get(url, headers = headers)

        content = resp.content.decode(encoding='utf-8', errors='strict')

        page_data = [row for row in content.split(
            '\n') if row.startswith('root.App.main = ')][0][:-1]

        page_data = page_data.split('root.App.main = ', 1)[1]

        return json.loads(page_data)

def get_next_earnings_date(ticker):

    base_earnings_url = 'https://finance.yahoo.com/quote'
    new_url = base_earnings_url + "/" + ticker

    parsed_result = _parse_earnings_json(new_url)

    temp = parsed_result['context']['dispatcher']['stores']['QuoteSummaryStore']['calendarEvents']['earnings']['earningsDate'][0]['raw']

    return datetime.datetime.fromtimestamp(temp)


def get_earnings_history(ticker):

        '''Inputs: @ticker
           Returns the earnings calendar history of the input ticker with
           EPS actual vs. expected data.'''

        url = 'https://finance.yahoo.com/calendar/earnings?symbol=' + ticker

        result = _parse_earnings_json(url)

        return result["context"]["dispatcher"]["stores"]["ScreenerResultsStore"]["results"]["rows"]



def get_earnings_for_date(date, offset = 0, count = 1):

    '''Inputs: @date
       Returns a dictionary of stock tickers with earnings expected on the
       input date.  The dictionary contains the expected EPS values for each
       stock if available.'''

    base_earnings_url = 'https://finance.yahoo.com/calendar/earnings'

    if offset >= count:
        return []

    temp = pd.Timestamp(date)
    date = temp.strftime("%Y-%m-%d")

    dated_url = '{0}?day={1}&offset={2}&size={3}'.format(
        base_earnings_url, date, offset, 100)

    result = _parse_earnings_json(dated_url)

    stores = result['context']['dispatcher']['stores']

    earnings_count = stores['ScreenerCriteriaStore']['meta']['total']

    new_offset = offset + 100

    more_earnings = get_earnings_for_date(date, new_offset, earnings_count)

    current_earnings = stores['ScreenerResultsStore']['results']['rows']

    total_earnings = current_earnings + more_earnings

    return total_earnings


def get_earnings_in_date_range(start_date, end_date):

        '''Inputs: @start_date
                   @end_date

           Returns the stock tickers with expected EPS data for all dates in the
           input range (inclusive of the start_date and end_date.'''

        earnings_data = []

        days_diff = pd.Timestamp(end_date) - pd.Timestamp(start_date)
        days_diff = days_diff.days


        current_date = pd.Timestamp(start_date)

        dates = [current_date + datetime.timedelta(diff) for diff in range(days_diff + 1)]
        dates = [d.strftime("%Y-%m-%d") for d in dates]

        i = 0
        while i < len(dates):
            try:
                earnings_data += get_earnings_for_date(dates[i])
            except Exception:
                pass

            i += 1

        return earnings_data


def get_currencies(headers = {'User-agent': 'Mozilla/5.0'}):

    '''Returns the currencies table from Yahoo Finance'''

    site = "https://finance.yahoo.com/currencies"
    tables = pd.read_html(requests.get(site, headers=headers).text)

    result = tables[0]

    return result


def get_futures(headers = {'User-agent': 'Mozilla/5.0'}):

    '''Returns the futures table from Yahoo Finance'''

    site = "https://finance.yahoo.com/commodities"
    tables = pd.read_html(requests.get(site, headers=headers).text)

    result = tables[0]

    return result


def get_undervalued_large_caps(headers = {'User-agent': 'Mozilla/5.0'}):

    '''Returns the undervalued large caps table from Yahoo Finance'''

    site = "https://finance.yahoo.com/screener/predefined/undervalued_large_caps?offset=0&count=100"

    tables = pd.read_html(requests.get(site, headers=headers).text)

    result = tables[0]

    return result


def get_quote_data(ticker, headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
):

    '''Inputs: @ticker

       Returns a dictionary containing over 70 elements corresponding to the
       input ticker, including company name, book value, moving average data,
       pre-market / post-market price (when applicable), and more.'''

    site = "https://query1.finance.yahoo.com/v7/finance/quote?symbols=" + ticker

    resp = requests.get(site, headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
)

    if not resp.ok:
        raise AssertionError("""Invalid response from server.  Check if ticker is
                              valid.""")


    json_result = resp.json()
    info = json_result["quoteResponse"]["result"]

    return info[0]


def get_market_status():

    '''Returns the current state of the market - PRE, POST, OPEN, or CLOSED'''

    quote_data = get_quote_data("^dji")

    return quote_data["marketState"]

def get_premarket_price(ticker):

    '''Inputs: @ticker

       Returns the current pre-market price of the input ticker
       (returns value if pre-market price is available.'''

    quote_data = get_quote_data(ticker)

    if "preMarketPrice" in quote_data:
        return quote_data["preMarketPrice"]

    raise AssertionError("Premarket price not currently available.")

def get_postmarket_price(ticker):

    '''Inputs: @ticker

       Returns the current post-market price of the input ticker
       (returns value if pre-market price is available.'''

    quote_data = get_quote_data(ticker)

    if "postMarketPrice" in quote_data:
        return quote_data["postMarketPrice"]

    raise AssertionError("Postmarket price not currently available.")


# Company Information Functions
def get_company_info(ticker):
    '''Scrape the company information for a ticker

       @param: ticker
    '''
    site = f"https://finance.yahoo.com/quote/{ticker}/profile?p={ticker}"
    json_info = _parse_json(site)
    json_info = json_info["assetProfile"]
    info_frame = pd.DataFrame.from_dict(json_info,
                                        orient="index",
                                        columns=["Value"])
    info_frame = info_frame.drop("companyOfficers", axis="index")
    info_frame.index.name = "Breakdown"
    return info_frame


def get_company_officers(ticker):
    '''Scrape the company information and return a table of the officers

       @param: ticker
    '''
    site = f"https://finance.yahoo.com/quote/{ticker}/profile?p={ticker}"
    json_info = _parse_json(site)
    json_info = json_info["assetProfile"]["companyOfficers"]
    info_frame = pd.DataFrame.from_dict(json_info)
    info_frame = info_frame.set_index("name")
    return info_frame
