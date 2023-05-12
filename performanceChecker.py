'''

Author: Ejakhegbe Abumere - - Manager @FHI360UK
Version: 1.1Beta
Description: This script import participants into DHIS2
1. Install python (https://phoenixnap.com/kb/how-to-install-python-3-windows) --Minimum of python 3.6 required
2. Open Command Prompt
3. Install pandas (run > pip install pandas), (run > pip install pandas pyAesCrypt click maskpass openpyxl xlsxwriter xlrd)
4. Create a folder and copy the directory eg. c:/folder
5. Navigate to the folder created above. In command prompt, type > cd c:/folder
6. run performanceChecker.py

Usage:
To import: Training Event
1 - Ensure the template is followed
2 - by Default the exports workbook will generate two sheets
      1) events timeline
      2) performance (> python performanceChecker.py --batch_start_day=2023-05-08 --batch_end_day=2023-05-12 --org_unit=DM9CWymNEVa --program=FVhEHQDNfxm --metadata=events)
     - program is the UID of the program
     - orgUnit is the UID of the OU parent ie in this case I am using root

'''


import requests as rq
import json
from io import StringIO
import pandas as pd
from datetime import datetime, timedelta


import os
import pickle
import click
import pyAesCrypt
import maskpass  # importing maskpass library
from os.path import exists

class run:
    def __init__(self,
                 batch_start_day,
                 batch_end_day,
                 orgUnit=None,
                 program=None,
                 metadata=None):


        dhis_file = "dhis-credentials.dat"
        pickle_file_enc = f"{dhis_file}.aes"
        # password_decrypt = sys.argv[1]  # "visit_rwanda"
        password_decrypt = "visit_rwanda"  # "visit_rwanda"
        pyAesCrypt.decryptFile(pickle_file_enc, f"2_{dhis_file}", password_decrypt)

        credentials = pickle.load(open(f"2_{dhis_file}", "rb"))

        os.remove(f"2_{dhis_file}")
        username = credentials[0]
        password = credentials[1]
        target_url = credentials[2]
        self.username = username
        self.password = password
        self.target_url = target_url
        self.metadata = metadata
        self.orgUnit = orgUnit
        self.program = program
        self.batch_end_day = batch_end_day
        self.batches = [f'programStartDate={batch_start_day}&programEndDate={batch_end_day}']
        self.performance()

    def performance(self):

        data_list = []

        for batching in self.batches:
            dayRanges = batching.split('=')
            batchStart = dayRanges[1].split("&programEndDate")[0]
            batchEnd = dayRanges[2]
            delta = timedelta(days=3)

            batchStart_datetime_object = datetime.strptime(batchStart, '%Y-%m-%d')
            batchEnd_datetime_object = datetime.strptime(batchEnd, '%Y-%m-%d')

            while batchStart_datetime_object <= batchEnd_datetime_object:
                if self.batch_end_day is None:
                    newbatchEnd_datetime_object = (batchStart_datetime_object + delta).strftime("%Y-%m-%d")
                    batch = f'startDate={batchStart_datetime_object.strftime("%Y-%m-%d")}&endDate={newbatchEnd_datetime_object}'
                else:
                    batch = f'startDate={batchStart_datetime_object.strftime("%Y-%m-%d")}'
                    # batch = f'programStartDate={batchStart_datetime_object.strftime("%Y-%m-%d")}&programEndDate={newbatchEnd_datetime_object}'


                # teiapi = f"{self.target_url}{self.metadata}.json?orgUnit={self.orgUnit}&ouMode=DESCENDANTS&program={self.program}&{batch}&paging=false&fields=*"  # ,!relationships
                teiapi = f"{self.target_url}{self.metadata}.json?orgUnit={self.orgUnit}&ouMode=DESCENDANTS&program={self.program}&{batch}&paging=false&children=true&fields=*"  # ,!relationships


                print(teiapi)

                tei_list_api_response = rq.get(teiapi, auth=(self.username, self.password))
                tei_list_api_list = json.load(StringIO(tei_list_api_response.text))
                # Convert the nested JSON object to a flat table
                df = pd.json_normalize(tei_list_api_list['events'])

                sheet1 = df[df.columns]

                # Group the data by 'storedBy', and aggregate the counts of 'createdByUserInfo' and 'lastUpdatedByUserInfo'
                sheet2 = df.groupby('storedBy').agg(countCreated=('createdByUserInfo.username', 'count'),
                                                    countUpdated=(
                                                    'lastUpdatedByUserInfo.username', 'count')).reset_index()

                # Rename the columns of the second sheet
                sheet2.columns = ['storedBy', 'countCreated', 'countUpdated']

                data_list.append([sheet1, sheet2])

                batchStart_datetime_object += delta

        # Process the data once the while loop is completed
        sheet1_list = []
        sheet2_list = []

        for data in data_list:
            sheet1_list.append(data[0])
            sheet2_list.append(data[1])

        sheet1_df = pd.concat(sheet1_list)
        sheet2_df = pd.concat(sheet2_list)
        #
        # sheet1_df = sheet1_df.drop_duplicates(inplace=True)
        # sheet2_df = sheet2_df.drop_duplicates(inplace=True)

        # Export the data to an Excel file with two sheets
        with pd.ExcelWriter('event performance output.xlsx') as writer:
            sheet1_df.to_excel(writer, sheet_name='events timeline', index=False)
            sheet2_df.to_excel(writer, sheet_name='performance', index=False)


@click.command()
@click.option(
    '--batch_start_day',
    default=None,
    help="batch_start_day"
)
@click.option(
    '--batch_end_day',
    default= None,
    help="batch_end_day"
)
@click.option(
    '--metadata',
    default='events',
    help="metadata"
)

@click.option(
    '--program',
    default=None,
    help="program"
)

@click.option(
    '--org_unit',
    default=None,
    help="org_unit"
)

def main(batch_start_day, batch_end_day, org_unit, program, metadata):
    run(batch_start_day=batch_start_day,
        batch_end_day=batch_end_day,
        orgUnit=org_unit,
        program=program,
        metadata=metadata)

if __name__ == "__main__":
    file_exists = exists("dhis-credentials.dat")
    try:
        if not file_exists:
            print("Security File Does Not Exist")
            dhis_file = "dhis-credentials.dat"
            dhis_username = input("Please enter your username: ")
            target_url = input("Please enter url eg format: https://url/api/29/ ")
            dhis_password = maskpass.askpass(prompt="Please enter your password: ", mask="*")
            passphrase = "visit_rwanda"  # input("please enter your passphrase: ")

            credentials = [dhis_username, dhis_password, target_url]
            pickle.dump(credentials, open(dhis_file, "wb"))

            # passphrase = "visit_rwanda"#

            pickle_file_enc = f"{dhis_file}.aes"
            # encrypt
            pyAesCrypt.encryptFile(dhis_file, pickle_file_enc, passphrase)

            pyAesCrypt.decryptFile(pickle_file_enc, f"2_{dhis_file}", passphrase)
            print(f"credentials configured! Always use {passphrase} as your passphrase ")
            start_again = 1
            print("Start again")
        else:
            main()
    except Exception as e:
        print(e)


