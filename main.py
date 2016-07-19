import json
import math
import requests
import pandas as pd

from flask import Flask, request
app = Flask(__name__)

def chunkify(l, n):
    """Yield successive n-sized chunks from l.
    From: http://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks-in-python"""
    for i in range(0, len(l), n):
        yield l[i:i+n]

def merge_calls(*calls):
    df = pd.DataFrame()
    for call in calls:
        header = call[0]
        data = call[1:]
        tmp_df = pd.DataFrame(data)
        tmp_df.columns = header
        if df.empty:
            df = tmp_df
        else:
            df = df.merge(tmp_df, how='outer', on='GEOID')
    df = df.convert_objects(convert_numeric=True)
    return df.to_json(orient='records')


def call_census(params, get_vars):
    year = params.get('year')
    est = params.get('est')
    key = params.get('key')
    for_arg = params.get('for', '')
    in_arg = params.get('in', '')
    if in_arg:
        in_arg = '&in=' + in_arg
    api_url = 'http://api.census.gov/data/{}/acs{}?get=GEOID,{}&for={}{}&key={}'.format(year, est, get_vars, for_arg, in_arg, key)
    return requests.get(api_url).json()

@app.route('/')
def proxy():
    params = {key: value for key, value in request.args.items()}
    get_arg = [x for x in params.get('get').split(",") if x != 'GEOID']
    chunks = chunkify(get_arg, 49)
    calls = [call_census(params, ",".join(var_chunk)) for var_chunk in chunks]
    result = merge_calls(*calls)
    return result

app.run(debug=True)
