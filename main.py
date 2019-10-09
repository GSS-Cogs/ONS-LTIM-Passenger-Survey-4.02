# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.2.4
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # International Passenger Survey 4.02, main reason for migration by citizenship
#
# Convert all tabs from latest Excel spreadsheet
# https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/internationalmigration/datasets/ipsmainreasonformigrationbycitizenship

# +
from gssutils import *

scraper = Scraper('https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/' \
                  'internationalmigration/datasets/ipsmainreasonformigrationbycitizenship')
scraper
# -

tabs = scraper.distributions[0].as_databaker()


# Each tab is of the same form, with "software readable codes":
# > The datasheets can be imported directly into suitable software. When importing the datasheets into other software import only rows 8 to 26, starting at column C.

# +
def citizenship_code(s):
    code = pathify(s)
    assert code.startswith('cit-'), code
    code = code[4:]
    assert code.endswith('-est'), code
    code = code[:-4]
    return code.replace('-/-', '-')

def flow_code(s):
    return pathify(s[:s.find(',')])

def reason_code(s):
    return pathify(s[s.find(',')+1:].strip())

tidied_sheets = []

for tab in tabs:
    if not tab.name.startswith('Data'):
        continue
    year = int(tab.excel_ref('A2').value[-4:])

    start = tab.excel_ref('C8')
    end = tab.excel_ref('C26')
    codes = start.fill(DOWN) & end.expand(UP)
    observations = codes.fill(RIGHT)
    citizenship = start.shift(RIGHT).fill(RIGHT)
    # sheets B, C and D repeat 'All citizenships', 'British' and 'Stateless' from sheet A
    if not tab.name.endswith('A'):
        citizenship = citizenship - citizenship.regex(r'CIT (All|British|Stateless)')
    citizenship_ci = citizenship.regex(r'.*CI\s*$')
    citizenship_est = citizenship - citizenship_ci
    observations_est = observations & citizenship_est.fill(DOWN)
    observations_ci = observations & citizenship_ci.fill(DOWN)
    cs_est = ConversionSegment(observations_est, [
        HDimConst('Year', year),
        HDim(codes, 'Code', DIRECTLY, LEFT),
        HDim(citizenship_est, 'IPS Citizenship', DIRECTLY, ABOVE),
        HDim(observations_ci, 'CI', DIRECTLY, RIGHT),
        HDimConst('Measure Type', 'Count'),
        HDimConst('Unit', 'people-thousands')
    ])

    savepreviewhtml(cs_est)
    tidy_sheet = cs_est.topandas()
    tidy_sheet['IPS Citizenship'] = tidy_sheet['IPS Citizenship'].apply(citizenship_code)
    tidy_sheet['Migration Flow'] = tidy_sheet['Code'].apply(flow_code)
    tidy_sheet['Reason for migration'] = tidy_sheet['Code'].apply(reason_code)
    tidy_sheet = tidy_sheet[pd.isna(tidy_sheet['DATAMARKER'])].copy() # Todo: data markers
    tidy_sheet.drop(columns=['Code'], inplace=True)
    tidy_sheet.rename(columns={'OBS': 'Value', 'DATAMARKER': 'IPS Marker'}, inplace=True)
    tidy_sheet = tidy_sheet[['Year', 'Reason for migration', 'Migration Flow', 'IPS Citizenship', 'CI',
                             'Value','IPS Marker', 'Measure Type', 'Unit']]
    tidied_sheets.append(tidy_sheet)
tidy = pd.concat(tidied_sheets)

from IPython.core.display import HTML
for col in ['Reason for migration', 'Migration Flow', 'IPS Citizenship', 'Measure Type', 'Unit','IPS Marker' ]:
    tidy[col] = tidy[col].astype('category')
    display(HTML(f"<h2>{col}</h2>"))
    display(tidy[col].cat.categories)

# +
tidy['Reason for migration'] = tidy['Reason for migration'].cat.rename_categories({
    'definite-job': 'work-related-definite-job',
    'looking-for-work': 'work-related-looking-for-work',
    'other-reasons': 'other',
    'work-related-reasons': 'work-related-all'
})

out = Path('out')
out.mkdir(exist_ok=True, parents=True)
tidy.to_csv(out / 'observations.csv', index=False)
# -

tidy['IPS Marker'] = tidy['IPS Marker'].cat.rename_categories({
    'z': 'not-applicable',
    '.': 'no-contact',
    '0~': 'rounds-to-zero'})

# +
from gssutils.metadata import THEME

scraper.dataset.family = 'migration'
scraper.dataset.theme = THEME['population']
with open(out / 'dataset.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())
# -

csvw = CSVWMetadata('https://gss-cogs.github.io/ref_migration/')
csvw.create(out / 'observations.csv', out / 'observations.csv-schema.json')

tidy['Measure Type'].cat.categories = tidy['Measure Type'].cat.categories.map(pathify)
tidy.to_csv(out / 'observations-alt.csv', index = False)
csvw.create(out / 'observations-alt.csv', out / 'observations-alt.csv-metadata.json', with_transform=True,
            base_url='http://gss-data.org.uk/data/', base_path='gss_data/migration/ons-ltim-passenger-survey-4-01',
            dataset_metadata=scraper.dataset.as_quads())


