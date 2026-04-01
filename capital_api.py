import functools
import http.client
import json
from pprint import pprint

API_KEY = ""
LOGIN = ""
PASSWORD = ""
LIVE_URL = "api-capital.backend-capital.com"
DEMO_URL = "demo-api-capital.backend-capital.com"


def auto_refresh_token(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        response = func(*args, **kwargs)
        if response.code != 200:
            print(response.code, "refresh token")
            tokens.refresh_token()
            response = func(*args, **kwargs)
            if response.code != 200:
                print("refresh token error")
                raise ValueError(response)
        return response

    return wrapper


def new_session(print_info=False):
    conn = http.client.HTTPSConnection(DEMO_URL)
    payload = json.dumps({
        "identifier": LOGIN,
        "password": PASSWORD
    })
    headers = {
        'X-CAP-API-KEY': API_KEY,
        'Content-Type': 'application/json'
    }
    conn.request("POST", "/api/v1/session", payload, headers)
    res = conn.getresponse()
    header_dict = {k: v for k, v in res.headers.items()}
    if print_info:
        data = res.read()
        pprint(json.loads(data.decode("utf-8")))
    return header_dict


class Tokens:
    def __init__(self):
        self.X_SECURITY_TOKEN = self.CST = None
        self.refresh_token()

    def refresh_token(self):
        _header_dict = new_session()
        self.X_SECURITY_TOKEN, self.CST = _header_dict["X-SECURITY-TOKEN"], _header_dict["CST"]
        # print(self.X_SECURITY_TOKEN, self.CST)


tokens = Tokens()


def get_time():
    conn = http.client.HTTPSConnection(DEMO_URL)
    payload = ''
    headers = {}
    conn.request("GET", "/api/v1/time", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))


def ping_server(_xt, _cst):
    conn = http.client.HTTPSConnection(DEMO_URL)
    payload = ''
    headers = {
        'X-SECURITY-TOKEN': _xt,
        'CST': _cst
    }
    conn.request("GET", "/api/v1/ping", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))


def get_token():
    conn = http.client.HTTPSConnection(DEMO_URL)
    payload = ''
    headers = {
        'X-CAP-API-KEY': API_KEY
    }
    conn.request("GET", "/api/v1/session/encryptionKey", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data)
    return json.loads(data.decode("utf-8"))["encryptionKey"]


def log_out_session(_xt, _cst):
    conn = http.client.HTTPSConnection(DEMO_URL)
    payload = ''
    headers = {
        'X-SECURITY-TOKEN': _xt,
        'CST': _cst
    }
    conn.request("DELETE", "/api/v1/session", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))


def all_account(_xt, _cst):
    conn = http.client.HTTPSConnection(DEMO_URL)
    payload = ''
    headers = {
        'X-SECURITY-TOKEN': _xt,
        'CST': _cst
    }
    conn.request("GET", "/api/v1/accounts", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))


def preferences(_xt, _cst):
    conn = http.client.HTTPSConnection(DEMO_URL)
    payload = ''
    headers = {
        'X-SECURITY-TOKEN': _xt,
        'CST': _cst
    }
    conn.request("GET", "/api/v1/accounts/preferences", payload, headers)
    res = conn.getresponse()
    data = res.read()
    pprint(json.loads(data.decode("utf-8")))


def activity_history(_xt, _cst):
    _from = None
    _to = None
    last_period = 10
    detailed = "true"
    dealId = None
    filter = "source!=DEALER;type!=POSITION;status==REJECTED;epic==OIL_CRUDE,GOLD"

    conn = http.client.HTTPSConnection(DEMO_URL)
    payload = ''
    headers = {
        'X-SECURITY-TOKEN': _xt,
        'CST': _cst
    }
    conn.request("GET",
                 "/api/v1/history/activity?from={}&to={}&lastPeriod={}&detailed={}&dealId={}&filter={}".format(_from,
                                                                                                               _to,
                                                                                                               last_period,
                                                                                                               detailed,
                                                                                                               dealId,
                                                                                                               filter),
                 payload, headers)
    res = conn.getresponse()
    data = res.read()
    pprint(json.loads(data.decode("utf-8")))


@auto_refresh_token
def _create_position(position_info):
    conn = http.client.HTTPSConnection(DEMO_URL)
    payload = json.dumps(position_info)
    headers = {
        'X-SECURITY-TOKEN': tokens.X_SECURITY_TOKEN,
        'CST': tokens.CST,
        'Content-Type': 'application/json'
    }
    conn.request("POST", "/api/v1/positions", payload, headers)
    res = conn.getresponse()
    return res

def create_position(position_info):
    res = _create_position(position_info)
    data = res.read()
    return json.loads(data.decode("utf-8"))

def get_position(_xt, _cst, _deal_id):
    conn = http.client.HTTPSConnection(DEMO_URL)
    payload = ''
    headers = {
        'X-SECURITY-TOKEN': _xt,
        'CST': _cst
    }
    conn.request("GET", "/api/v1/positions/{}".format(_deal_id), payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))
    return json.loads(data.decode("utf-8"))


def update_position(_xt, _cst, _position_info, _deal_id):
    conn = http.client.HTTPSConnection(DEMO_URL)
    payload = json.dumps(_position_info)
    headers = {
        'X-SECURITY-TOKEN': _xt,
        'CST': _cst,
        'Content-Type': 'application/json'
    }
    conn.request("PUT", "/api/v1/positions/{}".format(_deal_id), payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))


def close_position(_xt, _cst, _deal_id):
    conn = http.client.HTTPSConnection(DEMO_URL)
    payload = ''
    headers = {
        'X-SECURITY-TOKEN': _xt,
        'CST': _cst
    }
    conn.request("DELETE", "/api/v1/positions/{}".format(_deal_id), payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))


def all_positions(_xt, _cst):
    conn = http.client.HTTPSConnection(DEMO_URL)
    payload = ''
    headers = {
        'X-SECURITY-TOKEN': _xt,
        'CST': _cst
    }
    conn.request("GET", "/api/v1/positions", payload, headers)
    res = conn.getresponse()
    data = res.read()
    pprint(json.loads(data.decode("utf-8")))


def markets_details(_xt, _cst):
    search_term = "gold"
    epics = "GOLD"
    conn = http.client.HTTPSConnection(DEMO_URL)
    payload = ''
    headers = {
        'X-SECURITY-TOKEN': _xt,
        'CST': _cst
    }
    conn.request("GET", "/api/v1/markets?searchTerm={}&epics={}".format(search_term, epics), payload, headers)
    res = conn.getresponse()
    data = res.read()
    pprint(json.loads(data.decode("utf-8")))


@auto_refresh_token
def _get_prices():
    epic = "GOLD"
    conn = http.client.HTTPSConnection(DEMO_URL)
    payload = ''
    headers = {
        'X-SECURITY-TOKEN': tokens.X_SECURITY_TOKEN,
        'CST': tokens.CST
    }
    conn.request("GET",
                 "/api/v1/prices/{}".format(epic),
                 payload, headers)
    res = conn.getresponse()
    return res


def get_prices():
    res = _get_prices()
    data = res.read()
    # pprint(json.loads(data.decode("utf-8")))
    return json.loads(data.decode("utf-8"))


if __name__ == '__main__':
    position = {
        "epic": "GOLD",
        "direction": "BUY",
        "size": 0.01,
        "guaranteedStop": False,
        "stopLevel": 4525,
        "profitLevel": 4545
    }
    deal_id = "00601567-0055-311e-0000-0000846f017a"

    # get_token()
    # get_time()
    # header_dict = new_session()
    # pprint(header_dict)
    # ping_server(header_dict["X-SECURITY-TOKEN"], header_dict["CST"])
    # all_account(header_dict["X-SECURITY-TOKEN"], header_dict["CST"])
    # activity_history(header_dict["X-SECURITY-TOKEN"], header_dict["CST"])
    # create_position(header_dict["X-SECURITY-TOKEN"], header_dict["CST"], position)
    # get_position(header_dict["X-SECURITY-TOKEN"], header_dict["CST"], deal_id)
    # close_position(header_dict["X-SECURITY-TOKEN"], header_dict["CST"], deal_id)
    # all_positions(header_dict["X-SECURITY-TOKEN"], header_dict["CST"])
    pprint(get_prices())
