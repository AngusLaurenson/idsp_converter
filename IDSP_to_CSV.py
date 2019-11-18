import pandas as pd
import re
import sys

# load a list of file names
txt_files = sys.argv[:]

# use regex to get only the essential information
regex_2016_onwards = "\w+/\w+/\d+/\d+/\d+\D*[\d\s/\.-]*\w*\s\w*"
raw_events = []
for txt_file in txt_files:
    with open(txt_file,"r") as f:
        dump = f.read()
        dump = dump.replace("\n"," ")

        raw_events = raw_events + re.findall(regex_2016_onwards, dump)

print("total number of outbreaks", raw_events.__len__())

diseases = ['ACUTE DIARRHEAL DISEASE', 'ACUTE ENCEPHALITIS SYNDROME', 'ACUTE FLACCID PARALYSIS', 'ACUTE RESPIRATORY INFECTION', 'ANY OTHER STATE SPECIFIC DISEASE', 'BACILLARY DYSENTERY', 'CHICKEN POX', 'CHIKUNGUNYA', 'DENGUE', 'DIPHTHERIA', 'DOG BITE', 'ENTERIC FEVER', 'FEVER OF UNKNOWN ORIGIN', 'LEPTOSPIROSIS', 'MALARIA', 'MEASLES', 'MENINGITIS', 'PERTUSSIS', 'PNEUMONIA', 'SNAKE BITE', 'UNUSUAL SYNDROMES NOT CAPTURED ABOVE', 'VIRAL HEPATITIS', 'FOOD POISONING', 'CHOLERA']

states = ['ANDAMAN & NICOBAR ISLANDS', 'ANDHRA PRADESH', 'ARUNACHAL PRADESH', 'ASSAM', 'BIHAR', 'CHANDIGARH', 'CHHATTISGARH', 'DADRA & NAGAR HAVELI', 'DAMAN & DIU', 'DELHI', 'GOA', 'GUJARAT', 'HARYANA', 'HIMACHAL PRADESH', 'JAMMU & KASHMIR', 'JHARKHAND', 'KARNATAKA', 'KERALA', 'LAKSHADWEEP', 'MADHYA PRADESH', 'MAHARASHTRA', 'MANIPUR', 'MEGHALAYA', 'MIZORAM', 'NAGALAND', 'ORISSA', 'PONDICHERRY', 'PUNJAB', 'RAJASTHAN', 'SIKKIM', 'TAMIL NADU', 'TRIPURA', 'UTTAR PRADESH', 'UTTARANCHAL', 'WEST BENGAL']

errors = {"state":0,"disease":0}

def outbreak_parser(outbreak_string):
    # takes a single string corresponding to the outbreak
    # splits it up into the separate fields and returns list
    # a bit sketchy, can't handle edge cases

    words = outbreak_string.split(" ")

    ID_code = words[0]

    status = re.findall("under \w+",outbreak_string.lower())[0]


    dates = re.findall("\d\d-\d\d-\d\d", outbreak_string)

    start_date = dates[0]
    try:
        end_date = dates[1]
    except:
        end_date = "Ongoing"
        pass

    cases, deaths = re.findall("\d\d\s\d\d",outbreak_string)[0].split(" ")

    # default values of the harder to obtain fields
    state, district, disease = None, None, None

    # check through all states
    for s in states:
        if s in outbreak_string.upper():
            state = s

    if states == None:
        errors["state"] += 1

    # check through all diseases
    for d in diseases:
        if d in outbreak_string.upper():
            disease = d

    if disease == None:
        errors["disease"] += 1

    # what is left must be the district
    re_string = "".join(["(?<=",state,"\s)","(.*)?","(?=\s",disease,")"])

    try:
        district = re.search(re_string,outbreak_string.upper())[0]
    except error:
        errors[error] += 1
        pass

    return [ID_code, state, district, disease, cases, deaths, start_date, end_date, status]

outbreaks = pd.DataFrame(columns = ["ID_code", "state", "district", "disease", "cases", "deaths", "start_date", "end_date", "status"])

failed = 0
for i, raw in enumerate(raw_events):
    try:
        outbreaks.loc[i] = outbreak_parser(raw)
    except:
        failed += 1
        pass

print("number of failed entries = ", failed)

for key in errors.keys():
    print("number of {} errors = {}".format(key,errors[key]))

# write out the dataframe to csv file for later analysis
outbreaks.to_csv(r'IDSP_data.csv')
