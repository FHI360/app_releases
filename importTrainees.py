'''

Author: Ejakhegbe Abumere - - Manager @FHI360UK
Version: 1.1Beta
Description: This script import participants into DHIS2
1. Install python (https://phoenixnap.com/kb/how-to-install-python-3-windows) --Minimum of python 3.6 required
2. Open Command Prompt
3. Install pandas (run > pip install pandas), (run > pip install pandas pyAesCrypt click maskpass openpyxl xlsxwriter xlrd)
4. Create a folder and copy the directory eg. c:/folder
5. Navigate to the folder created above. In command prompt, type > cd c:/folder
6. run importTrainees.py

Usage:
To import: Training Event
1 - Ensure the template is followed
2 - by Default the training sheet will be processed, To run workshop, put the record in training sheet (> python importTrainees.py)
3 - To run workshop, put the record in workshop sheet (> python importTrainees.py --tei_withevents workshop)
4 - To delete, put the record in activity_to_delete sheet (> python importTrainees.py --activity_to_delete activity_to_delete)

'''

import pandas as pd
import requests as rq
import sys
import os
import pickle
import click
import pyAesCrypt
import maskpass  # importing maskpass library
from os.path import exists
from datetime import datetime

class run:
    def __init__(self,
                 tei_withevents,
                 withevent=True,
                 activity_to_delete=None,
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
        self.username=username
        self.password=password
        self.target_url=target_url

        if withevent and activity_to_delete is None:
            print("Processing with with events")
            for activity in tei_withevents:
                print(f"Activty {activity} processing")
                self.dfRaw = None
                self.attributes = {}
                self.dfRaw = pd.read_excel("import.xlsx", sheet_name=activity)
                batch = 400
                batchStart = 0
                if len(self.dfRaw) > 0:
                    totalRows = len(self.dfRaw)//batch
                    loop = 0
                    while loop <= totalRows+1:
                        # pass
                        self.df = self.dfRaw.iloc[batchStart:batchStart+batch].copy().reset_index()
                        if len(self.df) > 0:
                            print(self.df)
                            print(f"processing {batchStart} to {batchStart+batch}")

                            self.activity = activity
                            column_names_raw = list(self.df.columns.values)
                            self.column_names = []
                            self.date_columns = []
                            for attr in column_names_raw:
                                if ':' in attr:
                                    self.column_names.append(attr)
                                if ':Date' in attr:
                                    self.date_columns.append(attr)
                            # print(column_names)

                            # filter trackedAttribute and Datavalues
                            self.df_datavalues = self.df[self.column_names]

                            if len(self.df)>0:
                                print(activity)
                                self.postwithEvent()
                            else:
                                print(f'{activity} sheet empty')
                            batchStart = batchStart + batch
                        loop = loop + 1
        elif withevent==False and activity_to_delete is None:
            print("Processing with no events")
            self.df = pd.read_excel("import.xlsx", sheet_name='teiwithoutevents')
            print(self.df)
            print("withevent==False and deleteTEIs==False and deleteEvents==False")
            # self.postWithoutEvents()
        if activity_to_delete is not None:
            toDelete = None
            while toDelete is None:
                print("Please enter yes or no.")
                toDelete = self.deleteConfirmation()
                print(f'Delete response is {toDelete}')
                if toDelete is not None:
                    break
            if toDelete == True:
                self.df = pd.read_excel("import.xlsx", sheet_name=activity_to_delete) # activity_to_delete shoule be exact sheet name
                print(self.df)
                self.delete(metadata)

    def ping(self):
        connected = False
        try:
            r = self.session.get(f'{self.base_url}system/ping', timeout=10)
        except rq.RequestException as e:
            raise SystemExit(1)
        else:
            if r.ok:
                print(f"[{self.klass}] Connection established.")
                connected = True
            else:
                print(f"[{self.klass}] Connection could not be established. {r.text}")
                raise SystemExit(1)
        return connected

    def deleteConfirmation(self):
        deleteDecision = None
        answer = input("Are you sure you want to delete, Enter yes or no: ")
        if answer == "yes":
            deleteDecision = True
            print("deleting metadata")
        elif answer == "no":
            print("stopping script")
            sys.exit()
        else:
            print("Enter yes or no")
            deleteDecision = None
        return deleteDecision
    def postWithoutEvents(self):
            self.df["DOB"] = self.df["DOB"].astype(str)
            self.df["FirstName"]=self.df["FirstName"].replace("'", "*", regex=True)
            self.df["LastName"]=self.df["LastName"].replace("'", "*", regex=True)
            # print(self.df["DOB"])
            # self.df["DOB"] = self.df['DOB'].replace(to_replace='/', value='-', regex=True)
            # self.df[['day','mth', 'yr']] = self.df["DOB"].str.split("-",expand=True)
            # self.df["DOB"] = self.df['yr']+'-'+self.df['mth']+'-'+self.df['day']
            # del self.df['yr'], self.df['mth'], self.df['day']
            attributes = {

                "FirstName":"NjpuYA30Vny",
                "LastName":"xzYxa8ln2Jg",
                "Phone number":"ls0TZ2qIQY4",
                "DOB":"NaO1e2lGnI5",
                "Participant Category":"oRJhK50Ttid",
                "Participant Other":"O5pbKNrcDli",
                "Position": "GT99Xd3kyVN",
                "Trained by Soma Umenye":"evZ86Mw2abv",
                "Class level - P1":"KtgGs144OFY",
                "Class level - P2":"JxkXlhN81kM",
                "Class level - P3":"o1oMFTphlqy",
                "Phone number 2":"S9WzJy87oWr"

            }
            n = 0
            already_processed = []

            tei = []
            for row in self.df.values:
                prepEvent=[]
                #filter unique teis
                print(f"Processing -> {self.df['trackedEntityInstance'][n]}")

                if self.df['trackedEntityInstance'][n] not in already_processed:
                    item = {
                        "program":self.df["program"][n],
                        "orgUnit":self.df['orgUnit'][n],
                        "trackedEntityInstance":self.df['trackedEntityInstance'][n],
                        "trackedEntityType":self.df["trackedEntityType"][n],
                        "enrollments":[
                            {"storedBy":"aejakhegbe",
                             "program":self.df["program"][n],
                             "orgUnit":self.df['orgUnit'][n],
                             "trackedEntityInstance":self.df['trackedEntityInstance'][n],
                             "enrollment":self.df["enrollment"][n],
                             "trackedEntityType":self.df["trackedEntityType"][n]
                             }
                        ],
                        "attributes": [
                            {
                                "attribute":attributes["FirstName"],
                                "value":str(self.df['FirstName'][n])
                            },
                            {
                                "attribute":attributes["LastName"],
                                "value":str(self.df['LastName'][n])
                            },
                            {
                                "attribute":attributes["DOB"],
                                "value":self.df['DOB'][n]
                            },
                            {
                               "attribute":attributes["Phone number"],
                               "value":str(self.df['Phone number'][n])
                            },
                            {
                                "attribute":attributes["Phone number 2"],
                                "value":str(self.df['Phone number 2'][n])
                            },
                            {
                                "attribute":attributes["Participant Category"],
                                "value":self.df['Participant Category'][n]
                            },
                            {
                                "attribute":attributes["Participant Other"],
                                "value":self.df['Participant Other'][n]
                            },
                            {
                                "attribute":attributes["Position"],
                                "value":self.df['Position'][n]
                            },
                            {
                                "attribute":attributes["Trained by Soma Umenye"],
                                "value":self.df['Trained by Soma Umenye'][n]
                            },
                            {
                                "attribute":attributes["Class level - P1"],
                                "value":self.df['Class level - P1'][n]
                            },
                            {
                                "attribute":attributes["Class level - P2"],
                                "value":self.df['Class level - P2'][n]
                            },
                            {
                                "attribute":attributes["Class level - P3"],
                                "value":self.df['Class level - P3'][n]
                            }
                        ]
                    }
                    attr_no = 0
                    for i in item["attributes"]:
                        if str(item["attributes"][attr_no]["value"]) == 'False' \
                                or item["attributes"][attr_no]["value"] == 'false' \
                                or item["attributes"][attr_no]["value"] == False \
                                or item["attributes"][attr_no]["value"] == "nan":
                            print(f'{item["attributes"][attr_no]["attribute"]} -- to delete')
                            del item["attributes"][attr_no]
                        attr_no = attr_no +1

                    'Some False escaped - not sure why but did this again'
                    attr_no = 0
                    for i in item["attributes"]:
                        if str(item["attributes"][attr_no]["value"]) == 'False' \
                                or item["attributes"][attr_no]["value"] == 'false' \
                                or item["attributes"][attr_no]["value"] == False \
                                or item["attributes"][attr_no]["value"] == "nan":
                            print(f'{item["attributes"][attr_no]["attribute"]} -- to delete')
                            del item["attributes"][attr_no]
                        attr_no = attr_no +1

                    tei.append(item)
                n = n + 1
            print(str(tei))
            trackedEntityInstances = {"trackedEntityInstances": tei}
            tei_update_api_list_data = self.cleanup(str(trackedEntityInstances)).encode() #will produce bytes object encoded with utf-8
            print(tei_update_api_list_data)
            params = {'importStrategy': 'CREATE_AND_UPDATE'} # CREATE_AND_UPDATE, DELETE, CREATE
            post_event_list_api_response = rq.post(f"{self.target_url}trackedEntityInstances", data=tei_update_api_list_data,
                                                   headers={'content-type': 'application/json'}, params=params,
                                                   auth=(self.username, self.password))
            d = post_event_list_api_response.json()
            print(d)
    def postwithEvent(self):
        self.df["enrollmentDate"] = self.df['enrollmentDate'].replace(to_replace='/', value='-', regex=True)
        self.df["incidentDate"] = self.df['incidentDate'].replace(to_replace='/', value='-', regex=True)

        self.df[['yr','mth', 'day']] = self.df["enrollmentDate"].astype(str).str.split("-",expand=True)
        self.df["enrollmentDate"] = self.df['yr']+'-'+self.df['mth']+'-'+self.df['day']
        del self.df['yr'], self.df['mth'], self.df['day']
        self.df[['yr','mth', 'day']] = self.df["incidentDate"].astype(str).str.split("-",expand=True)
        self.df["incidentDate"] = self.df['yr']+'-'+self.df['mth']+'-'+self.df['day']
        del self.df['yr'], self.df['mth'], self.df['day']

        for dateColumn in self.date_columns:

            self.df_datavalues[dateColumn] = self.df_datavalues[dateColumn].replace(to_replace='/', value='-', regex=True)
            self.df_datavalues[['yr','mth', 'day']] = self.df_datavalues[dateColumn].astype(str).str.split("-",expand=True)
            self.df_datavalues[dateColumn] = self.df_datavalues['yr'].astype(str)+'-'+self.df_datavalues['mth'].astype(str)+'-'+self.df_datavalues['day'].astype(str)
            self.df[dateColumn] = self.df_datavalues[dateColumn].astype(str)
            del self.df_datavalues['yr'], self.df_datavalues['mth'], self.df_datavalues['day']
            print(self.df_datavalues[dateColumn])

        n = 0
        already_processed = []

        tei = []
        for row in self.df.values:
            prepEvent=[]
            #filter unique teis
            print(f"Processing -> {self.df['trackedEntityInstance'][n]}")

            if self.df['trackedEntityInstance'][n] not in already_processed:
                df_new = self.df[self.df.trackedEntityInstance == self.df['trackedEntityInstance'][n]].copy().reset_index()
                # print(df_new)
                if len(df_new)>1:
                        evt_no = 0
                        dataValues = []
                        TrackedAttributes = []
                        #Populate datavalues
                        for datavalue in self.column_names:
                            if 'datavalue' in datavalue:
                                spl = datavalue.split(":")
                                dataValues.append(
                                    {
                                        "dataElement":spl[2],
                                        "value":self.df_datavalues[datavalue][evt_no]
                                    }
                                )
                        #Populate TrackedAttributes
                        for TrackedAttribute in self.column_names:
                            if 'TrackedAttribute' in TrackedAttribute:
                                spl = TrackedAttribute.split(":")
                                TrackedAttributes.append(
                                    {
                                        "attribute":spl[2],
                                        "value":self.df_datavalues[TrackedAttribute][evt_no]
                                    }
                                )
                        #Populate events
                        for evt in df_new.values:
                            prepEvent.append(
                                {
                                    "program":df_new["program"][evt_no],
                                    "event":df_new["event"][evt_no],
                                    "programStage":df_new["programStage"][evt_no],
                                    "orgUnit":df_new['orgUnit'][evt_no],
                                    "trackedEntityInstance":df_new['trackedEntityInstance'][evt_no],
                                    "enrollment":df_new["enrollment"][evt_no],
                                    "enrollmentStatus":"ACTIVE",
                                    "status":"ACTIVE",
                                    "eventDate":df_new["incidentDate"][evt_no],
                                    "dataValues":dataValues
                                }
                            )
                            evt_no = evt_no + 1

                        item = {
                            "program":self.df["program"][n],
                            "orgUnit":self.df['orgUnit'][n],
                            "trackedEntityInstance":self.df['trackedEntityInstance'][n],
                            "trackedEntityType":self.df["trackedEntityType"][n],
                            "enrollments":[
                                {"storedBy":"aejakhegbe",
                                 "program":self.df["program"][n],
                                 "orgUnit":self.df['orgUnit'][n],
                                 "trackedEntityInstance":self.df['trackedEntityInstance'][n],
                                 "enrollment":self.df["enrollment"][n],
                                 "trackedEntityType":self.df["trackedEntityType"][n],
                                 "enrollmentDate":self.df["enrollmentDate"][n], # taking the first event date as registration
                                 "incidentDate":self.df["incidentDate"][n], # taking the first event date as registration
                                 "events":prepEvent}
                            ],
                                 "attributes": TrackedAttributes
                        }

                        evt_no = 0

                        for e in item["enrollments"][evt_no]["events"]:
                            rm = 0
                            for i in item["enrollments"][0]["events"][evt_no]["dataValues"]:
                                if str(item["enrollments"][0]["events"][evt_no]["dataValues"][rm]['value']) == 'False' \
                                        or item["enrollments"][0]["events"][evt_no]["dataValues"][rm]['value'] == 'false' \
                                        or item["enrollments"][0]["events"][evt_no]["dataValues"][rm]['value'] == False:
                                    del item["enrollments"][0]["events"][evt_no]["dataValues"][rm]
                                rm = rm + 1
                            evt_no = evt_no +1
                        'Some False escaped - not sure why but did this again'
                        evt_no = 0
                        for e in item["enrollments"][evt_no]["events"]:
                            rm = 0
                            for i in item["enrollments"][0]["events"][evt_no]["dataValues"]:
                                if str(item["enrollments"][0]["events"][evt_no]["dataValues"][rm]['value']) == 'False' \
                                        or item["enrollments"][0]["events"][evt_no]["dataValues"][rm]['value'] == 'false' \
                                        or item["enrollments"][0]["events"][evt_no]["dataValues"][rm]['value'] == False:
                                    del item["enrollments"][0]["events"][evt_no]["dataValues"][rm]
                                rm = rm + 1
                            evt_no = evt_no +1
                        tei.append(item)
                        already_processed.append(self.df['trackedEntityInstance'][n])
                else:
                        print("Only one instance exist")
                        #Populate datavalues
                        dataValues = []
                        TrackedAttributes = []
                        for datavalue in self.column_names:
                            if 'datavalue' in datavalue:
                                spl = datavalue.split(":")
                                dataValues.append(
                                    {
                                        "dataElement":spl[2],
                                        "value":self.df_datavalues[datavalue][n]
                                    }
                                )
                        #Populate TrackedAttributes
                        for TrackedAttribute in self.column_names:
                            if 'TrackedAttribute' in TrackedAttribute:
                                spl = TrackedAttribute.split(":")
                                TrackedAttributes.append(
                                    {
                                        "attribute":spl[2],
                                        "value":self.df_datavalues[TrackedAttribute][n]
                                    }
                                )
                        item = {
                            "program":self.df["program"][n],
                            "orgUnit":self.df['orgUnit'][n],
                            "trackedEntityInstance":self.df['trackedEntityInstance'][n],
                            "trackedEntityType":self.df["trackedEntityType"][n],
                            "enrollments":[
                                {"storedBy":"aejakhegbe",
                                 "program":self.df["program"][n],
                                 "orgUnit":self.df['orgUnit'][n],
                                 "trackedEntityInstance":self.df['trackedEntityInstance'][n],
                                 "enrollment":self.df["enrollment"][n],
                                 "trackedEntityType":self.df["trackedEntityType"][n],
                                 "enrollmentDate":self.df["enrollmentDate"][n],
                                 "incidentDate":self.df["incidentDate"][n],
                                 "events":[{
                                     "program":self.df["program"][n],
                                     "event":self.df["event"][n],
                                     "programStage":self.df["programStage"][n],
                                     "orgUnit":self.df['orgUnit'][n],
                                     "trackedEntityInstance":self.df['trackedEntityInstance'][n],
                                     "enrollment":self.df["enrollment"][n],
                                     "enrollmentStatus":"ACTIVE",
                                     "status":"ACTIVE",
                                     "eventDate":self.df["incidentDate"][n],
                                     "dataValues":dataValues
                                 }]}
                            ],
                            "attributes": TrackedAttributes
                        }
                        rm = 0
                        for i in item["enrollments"][0]["events"][0]["dataValues"]:
                                if str(item["enrollments"][0]["events"][0]["dataValues"][rm]['value']) == 'False' \
                                        or item["enrollments"][0]["events"][0]["dataValues"][rm]['value'] == 'false' \
                                        or item["enrollments"][0]["events"][0]["dataValues"][rm]['value'] == False:
                                    del item["enrollments"][0]["events"][0]["dataValues"][rm]
                                rm = rm + 1
                        'Some False escaped - not sure why but did this again'
                        rm = 0
                        for i in item["enrollments"][0]["events"][0]["dataValues"]:
                            if str(item["enrollments"][0]["events"][0]["dataValues"][rm]['value']) == 'False' \
                                    or item["enrollments"][0]["events"][0]["dataValues"][rm]['value'] == 'false' \
                                    or item["enrollments"][0]["events"][0]["dataValues"][rm]['value'] == False:
                                del item["enrollments"][0]["events"][0]["dataValues"][rm]
                            rm = rm + 1
                        tei.append(item)

            n = n + 1
        trackedEntityInstances = {"trackedEntityInstances": tei}
        tei_update_api_list_data = self.cleanup(str(trackedEntityInstances))
        print(tei_update_api_list_data)
        params = {'importStrategy': 'CREATE_AND_UPDATE'} # CREATE_AND_UPDATE, DELETE, CREATE
        post_event_list_api_response = rq.post(f"{self.target_url}trackedEntityInstances", data=tei_update_api_list_data,
                                               headers={'content-type': 'application/json'}, params=params,
                                               auth=(self.username, self.password))
        d = post_event_list_api_response.json()
        print(d)
    def delete(self,metadata):
        print("Deleting initiated")
        print(f'length of data to delete {len(self.df)}')
        n=0
        for row in self.df.values:
            post_event_list_api_response = rq.delete(f"{self.target_url}{metadata}s/{self.df[metadata][n]}", headers={'content-type': 'application/json'}, auth=(self.username, self.password))
            d = post_event_list_api_response.json()
            print(d)
            n = n + 1
    def cleanup(self, clean):
        dictionary = {
            'False': 'false',
            'True': 'true',
            '\'': '"',
            '*': '\''
        }
        for key in dictionary.keys():
            clean = clean.replace(key, dictionary[key])
        return clean


@click.command()
@click.option(
    '--withevent',
    default=True,
    help="withevent"
)
@click.option(
    '--activity_to_delete',
    default=None,
    help="activity_to_delete"
)
@click.option(
    '--metadata',
    default="trackedEntityInstance",
    help="metadata"
)
@click.option('--tei_withevents',
              default=('training',),
              multiple=True)
def main(tei_withevents, withevent, activity_to_delete, metadata):
    run(tei_withevents,
        withevent=withevent,
        activity_to_delete=activity_to_delete,
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



