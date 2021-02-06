import json
import boto3
import urllib3
from boto3.dynamodb.conditions import Key
from decimal import Decimal

dynamodb = boto3.resource('dynamodb', endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
table = dynamodb.Table('papertrades')

http = urllib3.PoolManager()

queryurl = "https://query1.finance.yahoo.com/v7/finance/quote?symbols="

def prep_return(body):
    return {
        "statusCode": 200,
        "body": body
    }

def initrow(username):
    check_exists = table.query(
        KeyConditionExpression=Key('username').eq(username)
    )
    if len(check_exists['Items']) == 0:
        resp = table.put_item(
            Item={
                'username': username,
                'cash': 10000,
                'stocks': {}
            }
        )
        return prep_return("User creation success")
    return prep_return("User already exists")

def buystock(username, ticker, numshares):
    resp = http.request("GET", queryurl + ticker)
    resp = json.loads(resp.data.decode('utf-8'))
    if len(resp['quoteResponse']['result']) == 0:
        return prep_return("No such ticker symbol")

    marketopen = resp['quoteResponse']['result'][0]['marketState']
    if marketopen == "POST" or marketopen == "PRE" or marketopen == "CLOSED":
        return prep_return("Market not open")

    check_exists = table.query(
        KeyConditionExpression=Key('username').eq(username)
    )
    if len(check_exists['Items']) != 0:
        usercash = float(check_exists['Items'][0]['cash'])
        stocks = check_exists['Items'][0]['stocks']
    else:
        usercash = 10000
        stocks = {}

    price = resp['quoteResponse']['result'][0]['regularMarketPrice']

    if numshares * price > usercash:
        return prep_return("Not enough cash")
    usercash -= numshares * price
    if ticker in stocks:
        stocks[ticker] = int(stocks[ticker])
        stocks[ticker] += numshares
    else:
        stocks[ticker] = numshares
    stocks[ticker] = Decimal(stocks[ticker])
    table.put_item(
        Item={
            'username': username,
            'cash': Decimal(str(usercash)),
            'stocks': stocks
        }
    )

    return prep_return("Buy successful")

def sellstock(username, ticker, numshares):
    resp = http.request("GET", queryurl + ticker)
    resp = json.loads(resp.data.decode('utf-8'))
    if len(resp['quoteResponse']['result']) == 0:
        return prep_return("No such ticker symbol")

    marketopen = resp['quoteResponse']['result'][0]['marketState']
    if marketopen == "POST" or marketopen == "PRE" or marketopen == "CLOSED":
        return prep_return("Market not open")

    check_exists = table.query(
        KeyConditionExpression=Key('username').eq(username)
    )
    if len(check_exists['Items']) == 0:
        return prep_return("Not enough shares")
    usercash = float(check_exists['Items'][0]['cash'])
    stocks = check_exists['Items'][0]['stocks']

    price = resp['quoteResponse']['result'][0]['regularMarketPrice']

    if ticker not in stocks or numshares > int(stocks[ticker]):
        return prep_return("Not enough shares")
    usercash += numshares * price
    
    stocks[ticker] = int(stocks[ticker])
    stocks[ticker] -= numshares
    if stocks[ticker] == 0:
        del stocks[ticker]
    table.put_item(
        Item={
            'username': username,
            'cash': Decimal(str(usercash)),
            'stocks': stocks
        }
    )

    return prep_return("Sell successful")

def net(username):
    check_exists = table.query(
        KeyConditionExpression=Key('username').eq(username)
    )
    if len(check_exists['Items']) == 0:
        return prep_return({"Net": 0})

    stocks = check_exists['Items'][0]['stocks']
    ttl = 0
    tickers = ','.join(list(stocks.keys()))
    resp = http.request("GET", queryurl + tickers)
    resp = json.loads(resp.data.decode('utf-8'))
    for resp_stock in resp['quoteResponse']['result']:
        ttl += resp_stock['regularMarketPrice'] * int(stocks[resp_stock['symbol']])
    return prep_return(str(check_exists['Items'][0]['cash'] + Decimal(ttl)))

def portfolio(username):
    check_exists = table.query(
        KeyConditionExpression=Key('username').eq(username)
    )
    if len(check_exists['Items']) == 0:
        return prep_return("Empty portfolio")
    
    stocks = check_exists['Items'][0]['stocks']
    for tckr in stocks:
        stocks[tckr] = int(stocks[tckr])
    return prep_return(json.dumps(stocks))

def lambda_handler(event, context):
    call = event['rawPath']
    call = call[call.rindex("/") + 1:]
    print(call)
    args = event['rawQueryString']
    print(args)
    try:
        if call == "buy" or call == "sell":
            args = args.split("&")
            args = [x.split("=") for x in args]
            args = {t[0]:t[1] for t in args}
            args['numshares'] = int(args['numshares'])
        else:
            args = args.split("=")[1]
    except:
        return {}
            
    print(args)
    if call == "buy":
        return buystock(args['username'], args['ticker'], args['numshares'])
    elif call == "sell":
        return sellstock(args['username'], args['ticker'], args['numshares'])
    elif call == "portfolio":
        return portfolio(args)
    elif call == "net":
        return net(args)
    else:
        return {}
