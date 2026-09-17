# import os

# import requests
# from dotenv import load_dotenv

# load_dotenv()

# url = "https://alpha-vantage.p.rapidapi.com/query"

# querystring = {
#     "datatype":"json",
#     "output_size":"compact",
#     "interval":"5min",
#     "function":"TIME_SERIES_INTRADAY",
#     "symbol":"MSFT"
# }

# headers = {
# 	"x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
# 	"x-rapidapi-host": "alpha-vantage.p.rapidapi.com",
# 	"Content-Type": "application/json"
# }

# response = requests.get(url, headers=headers, params=querystring)

# print(response.json())

import os

import requests
from dotenv import load_dotenv

load_dotenv()

url = "https://alpha-vantage.p.rapidapi.com/query"

querystring = {"function": "GLOBAL_QUOTE", "symbol": "AAPL", "datatype": "json"}

headers = {
    "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
    "x-rapidapi-host": "alpha-vantage.p.rapidapi.com",
    "Content-Type": "application/json",
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())