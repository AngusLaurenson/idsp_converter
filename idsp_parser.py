"""
Scrapes disease outbreak data from text files generated by pdftotext on PDF files published by IDSP.IN. These are consolidated weekly reports on disease outbreaks across India.

Author: Angus Laurenson, Nov 2019

Usage: This script is intended to be run as command line tool, i.e.

>> python3 idsp_parser /path/to/txtfiles/*.txt

Known issues: IDSP pdf files have different sections. One of them is updates about previously reported outbreaks. This generates duplicate reporting and the correct response is to drop the earlier records in favour of the updated record. Currently they are treated as separate outbreaks. Post 2016 the unique ID for the outbreak can be used. Pre 2016 a solution has yet to be invented and in both cases is yet to be implemented.
 + Solution can be implemented downstream, using pandas to compare rows for duplicates...

Wishlist: pdftotext within this program, fuzzy matching to overcome spelling differences and error,

Notes: SOPs for strings is capitalised words as in .title() string method. Lead taken from the datasets themselves.
"""

from fuzzywuzzy import fuzz
import geopandas as gpd
import pandas as pd
import re
import sys
from tqdm import tqdm

# use regex to get only the essential information
regex_post_2016 = "\w+/\w+/\d+/\d+/\d+"
regex_pre_2016 = "(\d\.\s\w.*?)(?=\d\.\s)"

def fuzzy_match(hypotheses, target):
    # returns the hypothesis which best matches the target
    match = []

    # build a list of (score, state) tuples
    for h in hypotheses:
        match.append((fuzz.token_set_ratio(h,target), h))

    # sort the list of tuples to take the state with highest score
    match.sort()
    return match[-1][-1]

def outbreak_parser(outbreak):

    # for validation, include raw outbreak line
    raw_string = outbreak

    # default values to account for missing values
    ID_code, state, district, disease, cases, deaths, start_date, report_date, status, comments = "?"*10

    # easy to locate fields
    ID_code = outbreak.split(" ")[0]
    status = re.findall("Under \w+",outbreak)[0]
    comments = re.findall("((?<="+status+").*)", outbreak)[0]

    # start stop dates
    dates = re.findall("\d+[/.-]\d+[/.-]\d+", outbreak)

    try:
        # replace to help later to_datetime()
        start_date = dates[0].replace('/','-')
    except:
        pass
    try:
        report_date = dates[1]
    except:
        pass

    try:
        cases, deaths = re.findall("(?<=\s)\d+\*?[\s/\.-]\d+\*?(?=\s)",outbreak)[0].split(" ")
    except:
        pass

    # use a fuzzy match to robustly get state
    state = fuzzy_match(state_district_dict.keys(), outbreak)

    # search only the districts within the state
    district = fuzzy_match(state_district_dict[state], outbreak)

    # IDSP doctrine gives investigators freedom to record
    # diseases which they deem important. Therefore a
    # comprehensive list of diseases is impossible.
    # However we are interested in Cholera not rare wildcards

    for d in disease_names:
        if d in outbreak:
            disease = d
            break

    return [ID_code, state, district, disease, cases, deaths, start_date, report_date, status, comments, raw_string]

if __name__ == '__main__':
    # load a list of file names
    txt_files = sys.argv[:]

    # create a list of strings, one for each outbreak
    # input string is broken up by matching the ID field
    outbreaks_raw = []
    for txt_file in tqdm(txt_files[:]):
        with open(txt_file,"r") as f:
            dump = f.read()
            dump = dump.replace("\n"," ")

            # determine the year IOT handle the format
            # take as the mode of all 4 digit numbers
            years = list(re.findall("\d{4}",dump))
            year = max(set(years), key=years.count)

            if float(year) >= 2016:
                outbreaks_raw += re.findall(f"{regex_post_2016}.*?(?={regex_post_2016})",dump)
            else:
                outbreaks_raw += re.findall(regex_pre_2016,dump)


    print("total number of outbreaks", outbreaks_raw.__len__())

    # try and create a dictionary of {state : [districts,]}
    # for finding the districts.

    IND_2 = gpd.read_file("gadm36_IND_2.shp")

    state_district_dict = {}
    for d in IND_2[['NAME_1','NAME_2']].values:
        # built a dictionary of lists
        # keys are states, values are its districts
        try:
            state_district_dict[d[0]].append(d[-1])
        except:
            state_district_dict[d[0]] = [d[-1]]

    # load list of diseases, not comprehensive
    with open("disease_names.txt","r") as f:
        disease_names = f.read().split("\n")

    # error tracking dict()
    errors = {"state":0,"disease":0,"IndexErrors":0}


    # Create a dataframe which contains all the outbreaks as rows
    # and all the data fields as columns
    outbreaks = pd.DataFrame(columns = ["ID_code", "state", "district", "disease", "cases", "deaths", "start_date", "report_date", "status", "comments", "raw"])

    for i, raw in enumerate(tqdm(outbreaks_raw)):
        # this part accumulates a significant number of errors
        # I think outbreak_parser is failing?
        try:
            outbreaks.loc[i] = outbreak_parser(raw)
        except IndexError:
            # print(outbreak_parser(raw))
            errors['IndexErrors'] += 1
        finally:
            pass

    # report the number of reports that we failed to read
    for key in errors.keys():
        print("number of {} errors = {}".format(key,errors[key]))

    # write out the dataframe to csv file for later analysis
    outbreaks.to_csv(r'IDSP_data.csv')
