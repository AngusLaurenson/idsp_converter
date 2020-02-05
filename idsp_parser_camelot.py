'''
DESCRIPTION:
script to extract india's integrated disease surveilance program (IDSP) database from pdf files to csv for further processing.

AUTHOR:
Angus Laurenson
anla@pml.ac.uk

SOURCE:
Code lifted from Vinayak Mehta's gist
https://gist.github.com/vinayak-mehta/e5949f7c2410a0e12f25d3682dc9e873

WISHLIST:
 * determine current, late or followup reporting
 * split cases/deaths column into two separate cols, cases | deaths


'''
import camelot
import scipy as sp
import pandas as pd
from glob import glob
from fuzzywuzzy import fuzz
from tqdm import tqdm

# get a list of all pdf files
pdfs = glob('/users/rsg/anla/podcast/country_disease_outbreaks/india/idsp_reporting/idsp_raw_pdfs/*/*.pdf') +\
glob('/users/rsg/anla/podcast/country_disease_outbreaks/india/idsp_reporting/idsp_raw_pdfs/*.pdf')


def try_read_pdf(pdf):
    try:
        tables = camelot.read_pdf(pdf,
                         flavour='lattice',
                         pages='2-end',
                         line_scale=40,
                         strip_text='\n',
                         )
        return tables
    except:
        print(' PdfReadError: Could not read malformed PDF file')

# create a generator of csv files to be parsed
table_gen = map(try_read_pdf, tqdm(pdfs))

# parse the tables as taken from jist
headers = [
    'unique_id',
    'state',
    'district',
    'disease_illness',
    'num_cases',
    'num_deaths',
    'date_of_start_of_outbreak',
    'date_of_reporting',
    'current_status',
    'comment_action_taken',
    'reported_late',
    'under_surveillance'
]
ten_headers = [
    'unique_id',
    'state',
    'district',
    'disease_illness',
    'num_cases',
    'num_deaths',
    'date_of_start_of_outbreak',
    'date_of_reporting',
    'current_status',
    'comment_action_taken'
]
nine_headers = [
    'unique_id',
    'state',
    'district',
    'disease_illness',
    'num_cases',
    'num_deaths',
    'date_of_start_of_outbreak',
    'current_status',
    'comment_action_taken'
]


def append_tables(all_tables):
    """Append all tables in PDFs

    Parameters
    ----------
    all_tables : list

    """
    df = pd.DataFrame(columns=headers)
    failed_reads = 0
    for tables in all_tables:
        # skip over failed reads.
        try:
            for table in tables:
                columns = list(table.df.iloc[0])
                if table.shape[1] == 10:
                    temp = table.df.copy()
                    if 'unique' in columns[0].lower():
                        temp = temp.iloc[1:]
                    temp.columns = ten_headers
                    temp['reported_late'] = False
                    temp['under_surveillance'] = False
                    df = pd.concat([df, temp], sort=False)
                elif table.shape[1] == 9:
                    temp = table.df.copy()
                    if 'disease' in columns[0].lower():
                        c = temp.iloc[0]
                        temp = temp.iloc[2:]
                        temp.columns = nine_headers
                        if 'reportedlate' in c[0].lower().replace(' ', ''):
                            temp['reported_late'] = True
                            temp['under_surveillance'] = False
                        elif 'undersurv' in c[0].lower().replace(' ', ''):
                            temp['reported_late'] = False
                            temp['under_surveillance'] = True
                        df = pd.concat([df, temp], sort=False)
                    else:
                        temp.columns = nine_headers
                        temp['reported_late'] = True
                        temp['under_surveillance'] = False
                        df = pd.concat([df, temp], sort=False)
        except:
            failed_reads += 1
            pass
    print(failed_reads)
    return df

df = append_tables(table_gen)
df.to_csv('idsp_database.csv')
