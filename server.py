import csv
import datetime
from datetime import datetime
import psycopg2
import os, sys
from decimal import Decimal
try:
    import simplejson as json
except ImportError:
    import json
from flask import Flask,request,Response,render_template
import psycopg2 # use this package to work with postgresql
app = Flask(__name__)
#from app import reqState

reqState = 0
creationTime = datetime.now()

def main(argv):
  global reqState
  global creationTime

@app.route('/')
def renderPage():
  return render_template("index.html")

@app.route('/write-log')
def writeLog():
  global creationTime
  code = request.args.get('code')
  logStr = request.args.get('logStr')
  value = request.args.get('value')
  if logStr == "initialized":
    creationTime = datetime.now()
  #print("Writing log:", logStr)
  f = open("log.csv", "a")
  f.write(str(round((datetime.now() - creationTime).total_seconds(), 2)) + "," + code + "," + logStr + "," + value + "\n")
  resp = Response(response="Success",status=200, mimetype='text/plain')
  h = resp.headers
  h['Access-Control-Allow-Origin'] = "*"
  return resp
  

@app.route('/get-state') #request state
def getState():
  global reqState
  reqState += 1
  resp = Response(response=str(reqState),status=200, mimetype='text/plain')
  h = resp.headers
  h['Access-Control-Allow-Origin'] = "*"
  return resp

@app.route('/get-data')
def getData():
  global reqState
  thisState = int(request.args.get('reqState'))
  #print(thisState, reqState)
  staleresp = Response(response=json.dumps({'stale': True}),status=200, mimetype='application/json')
  h = staleresp.headers
  h['Access-Control-Allow-Origin'] = "*"
  if reqState != thisState:
    print("STALE REQ: ABORTING")
    return staleresp

  try:
    conn = psycopg2.connect("dbname='a4database' user='cmsc828d'")
  except:
    print("I am unable to connect to the database")
    return
  
  states = request.args.get('states')
  statesList = states.split(",")
  attribute = request.args.get('attribute')
  
  
  cur = conn.cursor()
  # Get min max if necessary
  mnStr = request.args.get('min')
  mxStr = request.args.get('max')
  if mnStr == "-1" or mxStr == "-1":
    cur.execute("SELECT MIN(\"submission_date\"), MAX(\"submission_date\") FROM public.us_states_covid WHERE \"state\" = '{}';".format(statesList[0]))
    extremes = cur.fetchone()
    mn = extremes[0]
    mx = extremes[1]
  else:
    mn = datetime.strptime(mnStr, '%Y-%m-%d')
    mx = datetime.strptime(mxStr, '%Y-%m-%d')

  rangeLength = mx - mn

  if reqState != thisState:
    print("STALE REQ: ABORTING")
    return staleresp
  
  if rangeLength.days > 14600:
    interval = 365 #annual
  elif rangeLength.days > 2095:
    interval = 60 #bimonthly
  elif rangeLength.days > 500:
    interval = 7 #weekly
  else:
    interval = 1

  if reqState != thisState:
    print("STALE REQ: ABORTING")
    return staleresp
  
  # Get all rows for all states
  data = []
  dataAvg = []
  i = 0
  for s in statesList:
    if reqState != thisState:
      print("STALE REQ: ABORTING")
      return staleresp
    i += 1

    cur.execute("SELECT \"submission_date\",\"{}\" FROM public.us_states_covid WHERE \"state\" = '{}' AND ((CAST('{}' AS date) - CAST(submission_date AS date)) % '{}') = 0 AND \"submission_date\" >= '{}' AND \"submission_date\" <= '{}' ORDER BY \"submission_date\" ASC;".format(attribute, s, mx, interval, mn, mx))
    
    res = cur.fetchall()
    thisData = []
    for d in res:
      thisData.append({'date':d[0].strftime("%Y-%m-%d"), 'val':float(d[1])})
    data.append(thisData)

    cur.execute("SELECT AVG(\"{}\") FROM public.us_states_covid WHERE \"state\" = '{}' AND ((CAST('{}' AS date) - CAST(submission_date AS date)) % '{}') = 0 AND \"submission_date\" >= '{}' AND \"submission_date\" <= '{}';".format(attribute, s, mx, interval, mn, mx))
    res = cur.fetchall()
    dataAvg.append({'state':s, 'avg':float(res[0][0])})

  if reqState != thisState:
    print("STALE REQ: ABORTING")
    return staleresp
    
  # prep data for delivery
  jsonData = {'stale': False, 'data': data, 'dataAvg': dataAvg, 'min': mn.strftime("%Y-%m-%d"), 'max': mx.strftime("%Y-%m-%d")}
  resp = Response(response=json.dumps(jsonData),status=200, mimetype='application/json')
  h = resp.headers
  h['Access-Control-Allow-Origin'] = "*"
  return resp

def dbCommand(conn, comm):
  cur = conn.cursor()
  cur.execute(comm)
  try:
    data = cur.fetchall()
  except:
    data = []
    pass
  conn.commit()
  cur.close()
  return data

def absPath(path):
  if os.path.isabs(path):
    return path
  else:
    dir_path = os.getcwd()
    return os.path.join(dir_path, path)

def init(app, csvName):
  print("Initializing DB with: ", csvName)
  try:
    conn = psycopg2.connect("dbname='a4database' user='cmsc828d'")
  except Exception as err:
    errMsg('Failed to connect to the PostgreSQL server.')
    raise err
  dbCommand(conn, "DROP TABLE IF EXISTS public.us_states_covid;")
  dbCommand(conn,
    '''
      CREATE TABLE public.us_states_covid
      (
        submission_date date,
        state text,
        tot_cases integer,
        conf_cases integer,
        prob_cases integer,
        new_case integer,
        pnew_case integer,
        tot_death integer,
        conf_death integer,
        prob_death integer,
        new_death integer,
        pnew_death integer,
        created_at timestamp,
        consent_cases text,
        consent_deaths text
      );
    ''')
  dbCommand(conn,
    "COPY public.us_states_covid (submission_date,state,tot_cases,conf_cases,prob_cases,new_case,pnew_case,tot_death,conf_death,prob_death,new_death,pnew_death,created_at,consent_cases,consent_deaths) FROM '" +
    absPath(csvName) +
    "' DELIMITER ',' CSV HEADER QUOTE '\"' ESCAPE '\'\''" 
  )
  dbCommand(conn,
    '''
      CREATE INDEX idx_state_date ON public.us_states_covid(state,submission_date);
    ''')
  dbCommand(conn,
    '''
      CLUSTER public.us_states_covid USING idx_state_date;
    ''')
  return conn

def initLog():
  global creationTime
  creationTime = datetime.now()
  try:
    os.remove("log.csv")
  except OSError:
    pass
  f = open("log.csv", 'a')
  f.write("time,category,type,val\n")
  f.close()

if __name__ == "__main__":
  conn = init(app, "us_states_covid.csv")
  initLog()
  app.run(debug=True,port=8000)
