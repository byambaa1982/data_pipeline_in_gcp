def get_eurostat_data(request):
  import os
  from os import getenv
  import pandas as pd
  from flask import jsonify
  import eurostat

  #------ Get Search word here---
  request_json = request.get_json() 
  request_args = request.args 
  key_word = "" 

  if request_json and 'key_word' in request_json: 
    key_word = request_json['key_word'] 
  elif request_args and 'key_word' in request_args: 
    key_word = request_args['key_word']
  code=key_word
  print(code)
  #--------------------------------
  try:
    df = eurostat.get_data_df(code)
    print('code:{} columns:{} rows:{}'.format(code, df.shape[1], df.shape[0]))
  except: 
    df='{} not found in the Eurostat server'.format(code)
    print('{} not found in the Eurostat server'.format(code))


  return jsonify(df.shape)
