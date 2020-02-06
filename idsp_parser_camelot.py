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

def fuzzy_match(hypotheses, target, thresh=95):
    # returns the hypothesis which best matches the target
    match = []

    # build a list of (score, state) tuples
    for h in hypotheses:
        match.append((fuzz.partial_ratio(h,target), h))

    # sort the list of tuples to take the state with highest score
    match.sort()
    if match[-1][0] > thresh:
        return match[-1]
    else:
        return None

def append_tables(all_tables):
    """Append all tables in PDFs

    Parameters
    ----------
    all_tables : list

    """
    df = pd.DataFrame(columns=headers)
    failed_reads = []
    for i, tables in enumerate(all_tables):
        # skip over failed reads.
        # try:
        for table in tables:

            # deal with data due to column number
            columns = list(table.df.iloc[0])

            if (table.shape[1] == 9) or (table.shape[1] == 10) is True:
                temp = table.df.copy()

                followup = False
                try:
                    title = fuzzy_match('FOLLOW UP OF DISEASE OUTBREAKS  OF PREVIOUS WEEKS', columns, thresh=95)[1]
                    followup = True
                except:
                    pass

                if table.shape[1] == 10:

                    # drop first row of col names
                    if 'unique' in columns[0].lower():
                        temp = temp.iloc[1:]

                    # set column names
                    temp.columns = ten_headers

                    # get late reporting
                    temp['reported_late'] = False
                    temp['under_surveillance'] = False
                    temp['follow_up'] = followup
                    temp['source'] = pdfs[i]

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

                        temp['source'] = pdfs[i]
                        temp['follow_up'] = followup
                        df = pd.concat([df, temp], sort=False)
                    else:
                        temp.columns = nine_headers
                        temp['reported_late'] = True
                        temp['under_surveillance'] = False
                        temp['follow_up'] = followup
                        temp['source'] = pdfs[i]
                        df = pd.concat([df, temp], sort=False)

                # record where the data came from & add title col
                # temp['origin'] = pdfs[i]
                # temp['title'] = title
                # df = pd.concat([df, temp], sort=False)
            else:
                # do not append
                pass
        # except:
        #     failed_reads.append(pdfs[i])
        #     pass

    return df, failed_reads

# def append_tables(all_tables):
#     """Append all tables in PDFs
#
#     Parameters
#     ----------
#     all_tables : list
#
#     """
#     df = pd.DataFrame(columns=headers)
#     for tables in all_tables:
#         for table in tables:
#             columns = list(table.df.iloc[0])
#             if table.shape[1] == 10:
#                 temp = table.df.copy()
#                 if 'unique' in columns[0].lower():
#                     temp = temp.iloc[1:]
#                 temp.columns = ten_headers
#                 temp['reported_late'] = False
#                 temp['under_surveillance'] = False
#                 df = pd.concat([df, temp], sort=False)
#             elif table.shape[1] == 9:
#                 temp = table.df.copy()
#                 if 'disease' in columns[0].lower():
#                     c = temp.iloc[0]
#                     temp = temp.iloc[2:]
#                     temp.columns = nine_headers
#                     if 'reportedlate' in c[0].lower().replace(' ', ''):
#                         temp['reported_late'] = True
#                         temp['under_surveillance'] = False
#                     elif 'undersurv' in c[0].lower().replace(' ', ''):
#                         temp['reported_late'] = False
#                         temp['under_surveillance'] = True
#                     df = pd.concat([df, temp], sort=False)
#                 else:
#                     temp.columns = nine_headers
#                     temp['reported_late'] = True
#                     temp['under_surveillance'] = False
#                     df = pd.concat([df, temp], sort=False)
#     return df

if __name__ == '__main__':

    # create a generator of csv files to be parsed
    table_gen = map(try_read_pdf, tqdm(pdfs[::52]))

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

    df, failed_reads = append_tables(table_gen)
    df.to_csv('idsp_database.csv')
    with open('idsp_database_failed_pdfs.txt','w') as f:
        f.write('\n'.join(failed_reads))
