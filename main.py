from flask import Flask, request, jsonify
from slack_sdk import WebClient
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import requests, json
import logging
import datetime


#logging
logging.basicConfig(level=logging.INFO)

#service
#start service here
SCOPES = ['INSERT SCOPES']
creds = Credentials.from_service_account_file('slack_key.json', scopes=SCOPES) #IMPORT SLACK KEY
service = build('sheets', 'v4', credentials=creds)
spreadsheet_id = 'SPREADSHEET KEY' #should set as environment variable
slack_token = 'SLACK KEY'
client = WebClient(token=slack_token)

#app
app = Flask(__name__)

#populate variables
name = ""
start_time = ""
end_time =""
success_percentage = ""
total_conversion = ""

@app.route('/slack/events', methods=['POST']) #basica
def slack_event():
    '''
    This function is called when a slack event is triggered.
    :return:
    '''
    global name, start_time, end_time #i'm handling the variables awfully here. should change for better code practice
    data = request.json
    logging.info('Received data: %s', data)
    # Extracting name
    text = data['event']['text']
    name = text.split("http.host: ")[1].split("\n")[0].replace("'", "").split(" ")[0]
    end_time = float(data['event']['event_ts'])
    start_time = end_time - 3600  # 1 hour before
    print("name: ", name)
    print("start_time: ", start_time)
    print("end_time: ", end_time)
    fetch_from_newrelic(name,start_time,end_time)
    return jsonify({'status': 'ok'})

def fetch_from_newrelic(name, start_time, end_time):
    '''
    This function fetches data from NR and populates the variables
    :param name:
    :param start_time:
    :param end_time:
    :return:
    '''
    # Convert Unix timestamp to datetime
    start_time_dt = datetime.datetime.fromtimestamp(start_time, datetime.timezone(datetime.timedelta(hours=-7)))
    end_time_dt = datetime.datetime.fromtimestamp(end_time, datetime.timezone(datetime.timedelta(hours=-7)))

    # Convert datetime to string in ISO 8601 format which is handled by NR, excluding seconds
    start_time_str = start_time_dt.strftime('%Y-%m-%d %H:%M-0700')
    end_time_str = end_time_dt.strftime('%Y-%m-%d %H:%M-0700')

    # The NRQL query with name and specific range of time
    results_query = """{{actor {{
        account(id: SOME ACCOUNT ID) {{
          nrql(
            query: "FROM Timer SELECT uniqueCount(CustomData_r1.artifact.id) as 'Unique Doc Count', count(CustomData_process.exit_code) as 'Conversion Attempts' WHERE Name = 'ConversionService.Complete' AND CustomData_process.exit_code IS NOT NULL AND CustomData_conversion.type.id = -1 AND CustomData_process.exit_code NOT IN (0, 4, 3328, 9, 11, 10, 59, 14) AND `CustomData_r1.tenant.name` = '{name}' LIMIT MAX FACET CustomData_process.exit_code, CustomData_message, CustomData_file.type SINCE '{start_time_str}' UNTIL '{end_time_str}'"
          ) {{
            results
            }}
            }}
        }}
        }}""".format(name=name, start_time_str=start_time_str, end_time_str=end_time_str)

    success_percentage_query = '''{{
          actor {{
            account(SOME ACCOUNT ID) {{
              nrql(
                query: "SELECT latest(r1.source.name), latest(r1.ring.id), latest(cCnt) as 'Total Conversions', latest(sCnt) as 'Success Conversions', latest(sPer) as 'Success Percentage', latest(eCnt) as 'Error Conversoins', latest(ePer) as 'Error Percentage' FROM (SELECT count(CustomData_process.exit_code) as cCnt, filter(count(CustomData_process.exit_code), WHERE CustomData_process.exit_code IN (0, 4, 3328, 9, 11, 10, 59, 14)) as sCnt, percentage(count(*), WHERE CustomData_process.exit_code IN (0, 4, 3328, 9, 11, 10, 59, 14)) AS sPer, filter(count(CustomData_process.exit_code), WHERE CustomData_process.exit_code NOT IN (0, 4, 3328, 9, 11, 10, 59, 14)) as eCnt, percentage(count(*), WHERE CustomData_process.exit_code NOT IN (0, 4, 3328, 9, 11, 10, 59, 14)) AS ePer FROM Timer WHERE Name = 'ConversionService.Complete' AND CustomData_process.exit_code IS NOT NULL AND CustomData_conversion.type.id = -1 FACET CustomData_r1.tenant.name LIMIT MAX) JOIN ( SELECT r1.source.name, r1.ring.id, http.host FROM lookup(TenantLookup) ) ON `CustomData_r1.tenant.name` = http.host WHERE CustomData_r1.tenant.name = '{name}' FACET CustomData_r1.tenant.name LIMIT MAX SINCE '{start_time_str}' UNTIL '{end_time_str}'"
              ) {{
                results
              }}
            }}
          }}
        }}'''.format(name=name, start_time_str=start_time_str, end_time_str=end_time_str)

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Api-Key' : "NRAK-16PL7JREKNJVUBNIMKVI0FMIICK"
    }
    SP_body = {
        'query': success_percentage_query,
        'accountID': 0000000
    }
    results_body = {
        'query': results_query,
        'accountID': 0000000
    }
    response = requests.post('https://api.newrelic.com/graphql', headers=headers, data = json.dumps(results_body))
    sp_response = requests.post('https://api.newrelic.com/graphql', headers=headers, data = json.dumps(SP_body))
    data = response.json()
    sp_data = sp_response.json()
    update_google_sheets(data, sp_data, end_time, name)

    return
def update_google_sheets(data, sp_data,end_time,name):
    # Convert the timestamp to a datetime object
    SPREADSHEET_ID = '1PQCxTDftKu0nPicnGmLHWJbbkUp0UAFYO1X3STN3izY'  # Replace with your Spreadsheet ID
    SHEET_NAME = 'Sheet1'
    last_row = get_last_row(service, SPREADSHEET_ID, SHEET_NAME)

    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Sheet1!A:A').execute()
    rows = result.get('values', [])
    next_row = len(rows) + 2  # Next row after the last populated cell, plus two for the space

    dt_object = datetime.datetime.utcfromtimestamp(end_time)
    date = dt_object.strftime("%d-%b")
    results = data['data']['actor']['account']['nrql']['results']
    total_conversions = round(sp_data['data']['actor']['account']['nrql']['results'][0]['Total Conversions'], 2)
    success_percentage = round(sp_data['data']['actor']['account']['nrql']['results'][0]['Success Percentage'], 2)

    # Prepare the last result
    last_result_data = results[-1]
    facet = last_result_data['facet']
    code, error_message, file_type = facet
    conversion_attempts = str(last_result_data['Conversion Attempts'])
    unique_doc_count = str(last_result_data['Unique Doc Count'])
    total_conversions_str = str(int(total_conversions)) if total_conversions.is_integer() else str(round(total_conversions, 2))
    success_percentage_str = str(success_percentage)
    last_result = [date, name, code, file_type, conversion_attempts, unique_doc_count, total_conversions_str,
                   success_percentage_str]
    print(last_result)
    print(last_row)
    if last_row == last_result:
        print("Data already processed, skipping.")
        return
    for result in results:
        facet = result['facet']
        code, error_message, file_type = facet
        conversion_attempts = result['Conversion Attempts']
        unique_doc_count = result['Unique Doc Count']
        row_data = [date, name, code, file_type, conversion_attempts, unique_doc_count, total_conversions,success_percentage]
        body = {
            'values': [row_data]
        }

        range_str = f'Sheet1!A{next_row}:H{next_row}'
        print(range_str)
        result = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=range_str,
                                                        valueInputOption='RAW', body=body).execute()

        # Increment the row number
        next_row += 1

def get_last_row(service, SPREADSHEET_ID, SHEET_NAME):
    range_str = f'{SHEET_NAME}!A:H'  # Assuming the data is in columns A to H
    result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=range_str).execute()
    rows = result.get('values', [])
    return rows[-1] if rows else None

if __name__ == '__main__':
    app.run(port=3000)
