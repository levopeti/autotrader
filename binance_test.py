import ccxt

binance = ccxt.binance({
    'apiKey': '',
    'secret': '',
    'sandbox': True,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future',  # Fő! Futures
        'adjustForTimeDifference': True
    }
})

binance.set_sandbox_mode(True)  # Extra biztosítás

# Verbose debug
binance.verbose = True
binance.load_markets()

print("Kapcsolat teszt...")
ticker = binance.fetch_ticker('XAUUSDT')
print(f"XAUUSDT: {ticker['last']}")

balance = binance.fetch_balance()
print(f"Balance USDT: {balance['USDT']['free']}")

