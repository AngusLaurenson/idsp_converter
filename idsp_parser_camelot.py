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

def fuzzy_match(hypotheses, target, thresh=5):
    # returns the hypothesis which best matches the target
    match = []

    # build a list of (score, state) tuples
    for h in hypotheses:
        match.append((fuzz.token_sort_ratio(h,target), h))

    # sort the list of tuples to take the state with highest score
    match.sort()

    if match[-1][0] >= thresh:
        return match[-1][-1]
    elif thresh == None:
        return match[-1]


def append_tables(all_tables):
    """Append all tables in PDFs

    Parameters
    ----------
    all_tables : list

    """
    df = pd.DataFrame(columns=headers)
    failed_reads = []
    for i, tables in enumerate(all_tables):
        try:
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
                else:
                    # do not append
                    pass
        except:
            pass
    return df, failed_reads

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

    # ------------------ READ THE TABLE DATA --------------- #
    df, failed_reads = append_tables(table_gen)

    # ------------------ CLEAN DATAFRAME ------------------- #

    # match diseases in records to list of diseases to unify spelling
    with open('disease_names.txt','r') as f:
        disease_names = f.read().split('\n')
    df.disease_illness = df.disease_illness.apply(lambda x:fuzzy_match(disease_names, str(x), thresh=90))

    df.date_of_reporting.fillna('')

    for i, row in df.iterrows():
        try:
            # split cases / deaths column
            # use regex to extract digits
    #         cases, deaths = row.num_cases.split('/')
            cases, deaths = re.findall('\d+',row.num_cases)
            # shuffle right dates to correct columns
            date_outbreak = row.num_deaths
            date_reporting = row.date_of_start_of_outbreak

            for j, var in enumerate(['num_cases',
                        'num_deaths',
                        'date_of_start_of_outbreak',
                        'date_of_reporting'
                       ]):
                df.at[i,var] = [int(cases),int(deaths),date_outbreak,date_reporting][j]
        except:
            pass

    # set columns to type integer
    def extract_digits(x):
        try:
            return re.findall('\d+',x)[0]
        except:
            return x

    df[['num_cases','num_deaths']] = df[['num_cases','num_deaths']].apply(extract_digits)

    #
    # df[['num_cases','num_deaths']] = df[['num_cases','num_deaths']].apply(lambda x:pd.to_numeric(x,errors='coerce'))

    df.to_csv('idsp_database.csv')
    with open('idsp_database_failed_pdfs.txt','w') as f:
        f.write('\n'.join(failed_reads))
