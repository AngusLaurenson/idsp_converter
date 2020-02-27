import pandas as pd
import geopandas as gpd
import os
import re
from fuzzywuzzy import fuzz
from tqdm import tqdm

''' INPUT FILES '''

source = '/users/rsg/anla/podcast/country_disease_outbreaks/india/idsp_reporting/idsp_raw_txts/'

''' INPUT TEXT FILES '''

text_files = []
for root, dirnames, filenames in os.walk(source):
    for filename in filenames:
        if filename.endswith(('.txt')):
            text_files.append(os.path.join(root, filename))

''' DISEASE LIST '''
with open("disease_names.txt","r") as f:
    disease_names = f.read().split("\n")

''' STATE, DISTRICT AND SUB-DISTRICT DATA '''
IND_3 = gpd.read_file('/users/rsg/anla/podcast/country_district_shape_files/INDIA/GADM_IND_v2/IND_adm3.shp')
district_names = set(IND_3.NAME_2).union(set(IND_3.NAME_3))
state_names = set(IND_3.NAME_1)

''' REGEX DEFINITIONS '''

# regex code can grab dates with different spelling mistakes and delimiters
date_regex = "(?<=\s)\d+\s?.?[/\.-][\s.]?\d+.?[/\.-]\d+"

# id regex contains state code / district code / year / week / idcode
id_regex = "[A-Z]{2}/[A-Z]{3}?\s?/\d+\s?/\s?\d+\s?/\s?\d+"

# cases / deaths regex
cases_deaths_regex = "(?<=\s)\d+\*?\s?[\s/\.-]\s?\d+\*?(?=\s)"

# cases deaths date regex, for splitting records post 2016
cases_deaths_date_regex = "(?<=\s)\d+\*?\s?[\s/\.-]\s?\d+\*?(?=\s)\s+(?<=\s)\d+\s?.?[/\.-][\s.]?\d+.?[/\.-]\d+"

''' HELPER FUNCTIONS '''

def read_text(fname):
    with open(fname) as f:
        dump = f.read()
        return dump.replace('\n',' ')

def quick_match(hypotheses, target):
    for h in hypotheses:
        if h in target:
            return h
    return False

def fuzzy_match(hypotheses, target, thresh=80):
    # returns the hypothesis which best matches the target
    match = []

    # build a list of (score, state) tuples
    for h in hypotheses:
        match.append((fuzz.partial_ratio(h,target), h))

    # sort the list of tuples to take the state with highest score
    match.sort()
    if match[-1][0] >= thresh:
        return match[-1][-1]
    else:
        return False

def layered_match(hypotheses, target):
    try:
        return quick_match()
    except:
        return fuzzy_match(hypotheses, target)

''' SPLIT A TEXT DUMP INTO RECORDS '''

def split_records(dump):
    # splits a raw text dump into separate records

    # for post 2016 the split by id_regex should work
    id_codes = re.findall(id_regex, dump)

    if len(id_codes) > 1:
        body = re.split(id_regex,dump)[1:]
        records = map(lambda x:' '.join(x), zip(id_codes, body))

        return list(records)

    else:
        cases_deaths = re.findall(cases_deaths_date_regex, dump)
        bodies = re.split(cases_deaths_date_regex,dump)

        records_a = map(lambda x:' '.join(x), zip(bodies[::2],cases_deaths[::2],bodies[1::2]))
        records_b = map(lambda x:' '.join(x), zip(bodies[1::2],cases_deaths[1::2],bodies[2::2]))
        return [j for i in zip(records_a,records_b) for j in i]

''' EXTRACT DATA FROM A RECORD STRING '''

def parse_record(record):
    # given a record string. Extract the values of interest

    # ID CODE
    id_code = None
    id_code = re.findall(id_regex,record)


    # CASES DEATHS START_DATE
    cases_deaths_date = re.findall(cases_deaths_date_regex, record)
    cases, deaths = re.findall('\d+', cases_deaths_date[0])[:2]

    start_date = re.findall(date_regex,cases_deaths_date[0])[0].replace('/','-').replace('.','-')

    # SPLIT RECORD FOR EASE OF SEARCHING
    front,back = re.split(cases_deaths_date_regex, record)[:2]

    # REPORT DATE
    try:
        dates = re.findall(date_regex, back)
        report_date = dates[0].replace('/','-').replace('.','-')
    except:
        report_date = False

    # walk backwards from the centre and check for diseases
    # only last 10 words as the disease should be very close
    front_words = front.split(' ')[:]

    # a copy needed as its pop()'d
    fw = front_words.copy()

    # DISEASE MATCHING
    disease = False
    while (disease == False) & (fw.__len__() > 0):
        disease = layered_match(disease_names, fw.pop())

    # LOCATION MATCHING

    # STATE LEVEL
    fw = front_words.copy()
    state = False
    # while (state == False) & (fw.__len__() > 0):
    state = layered_match(state_names, fw)

    # DISTRICT LEVEL
    fw = front_words.copy()
    district = False
    # while (district == False) & (fw.__len__() > 0):
    district = layered_match(district_names, fw)

    # UNDER SURVEILLANCE?
    if 'under_surv' in back:
        under_surveillance = True
    else:
        under_surveillance = False

    return [id_code, disease, state,
                     district, cases, deaths,
                     start_date, report_date,
                     under_surveillance, record]

''' MAIN LOOP FOR PROCESSING '''
# this could be improved with mapping etc.

df = pd.DataFrame(columns=['id_code', 'disease', 'state',
                       'district', 'cases', 'deaths',
                       'start_date', 'report_date',
                       'under_surveillance', 'raw','filename'])
failed_records = []
for text_file in tqdm(text_files):
    # read file and split into records
    dump = read_text(text_file)
    records = split_records(dump)
    for record in records:
        # extract values and append to dataframe
        try:
            values = parse_record(record)
            df.loc[len(df)] = values + [text_file.split('/')[-1]]
        except:
            print('failed to parse record')
            failed_records.append((text_file.split('/')[-1],record))

''' write out results '''

from datetime import date

today = date.today()

# dd/mm/YY
d1 = today.strftime("%d-%m-%Y")
print("d1 =", d1)

df.to_csv('idsp_regex_'+d1+'.csv')

with open('idsp_regex_failed_'+d1+'.txt') as f:
    f.write('\n'.join([str(x) for x in failed_records]))
