"""
Scrapes disease outbreak data from text files generated by pdftotext on PDF files published by IDSP.IN. Run as command line tool

Author: Angus Laurenson

Known issues: IDSP PDF files changed format in 2016, program fails on pre 2016 files

Wishlist: pdftotext within this program, fuzzy matching to overcome spelling differences and error,

Notes: SOPs for strings is capitalised words as in .title() string method. Lead taken from the datasets themselves.
"""

import pandas as pd
import re
import sys

# use regex to get only the essential information
regex_post_2016 = "\w+/\w+/\d+/\d+/\d+"
regex_pre_2016 = "(\d\.\s\w.*?)(?=\d\.\s)"

def outbreak_parser(outbreak):

    # default values to account for missing values
    ID_code, state, district, disease, cases, deaths, start_date, end_date, status, comments = "?"*10

    # easy to locate fields
    ID_code = outbreak.split("_")[0]
    status = re.findall("Under \w+",outbreak)[0]
    comments = re.findall("((?<="+status+").*)", outbreak)[0]

    # start stop dates
    dates = re.findall("\d\d[\.-]\d\d[\.-]\d\d", outbreak)
    try:
        start_date = dates[0]
    except:
        pass
    try:
        end_date = dates[1]
    except:
        pass

    try:
        cases, deaths = re.findall("(?<=\s)\d+[\s/\.-]\d+(?=\s)",outbreak)[0].split(" ")
    except:
        pass

    # search through state, district and disease for matches.
    for s in state_names:
        if s in outbreak:
            state = s
            break

    for d in district_names:
        if d in outbreak:
            district = d
            break

    for d in disease_names:
        if d in outbreak:
            disease = d
            break

    return [ID_code, state, district, disease, cases, deaths, start_date, end_date, status, comments]

if __name__ == '__main__':
    # load a list of file names
    txt_files = sys.argv[:]

    # create a list of strings, one for each outbreak
    # input string is broken up by matching the ID field
    outbreaks_raw = []
    for txt_file in txt_files[:]:
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

    # states and districts as downloaded from
    # https://gadm.org/download_country_v3.html
    # new line delimited text files required
    with open("state_names.txt","r") as f:
        state_names = f.read().split("\n")
    with open("district_names.txt","r") as f:
        district_names = f.read().split("\n")
    with open("disease_names.txt","r") as f:
        disease_names = f.read().split("\n")

    # error tracking dict()
    errors = {"state":0,"disease":0,"IndexErrors":0}


    # Create a dataframe which contains all the outbreaks as rows
    # and all the data fields as columns
    outbreaks = pd.DataFrame(columns = ["ID_code", "state", "district", "disease", "cases", "deaths", "start_date", "end_date", "status", "comments"])

    for i, raw in enumerate(outbreaks_raw):
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
