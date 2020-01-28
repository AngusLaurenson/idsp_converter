'''
merge outbreak data with geometry
'''

df = pd.read_csv("/users/rsg/anla/podcast/country_disease_outbreaks/india/idsp_reporting/IDSP_data.csv")
IND_2 = gpd.read_file("/data/datasets/Projects/PODCAST/country_district_shape_files/INDIA/gadm36_IND_2.shp")

# convert strings of numbers to numbers for processing
# date time objects cannot be put into a shape file tho...
df[['cases','deaths']] = df[['cases','deaths']].apply(lambda x : pd.to_numeric(x, errors='coerce'))

# drop useless columns from incidence data
df = df.drop(columns=['Unnamed:','status'])

# get just geometry and district name
district_locations = IND_2[['NAME_1','NAME_2','geometry']]\
                    .rename(columns={'NAME_1':'state','NAME_2':'district'})\
                    .dissolve(by=['state','district'],aggfunc='first')

master = district_locations.merge(df, on=['state','district'])

master.to_file('idsp_outbreak.shp')
