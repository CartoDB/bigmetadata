'''
Generator for the `Metadata_2016_GCP_DataPack.csv` file from `au_metadata_columns.csv`
Note: `au_metadata_columns.csv` is a CSV with the columns extracted manually from `Metadata_2016_GCP_DataPack.xlsx`
'''

import csv
import os
import tempfile

tables = {
    'G01': ['Selected Person Characteristics by Sex', 'people', 'age_gender'],
    'G03': ['Place of Usual Residence on Census Night by Age', 'people', 'age_gender'],
    'G04': ['Age by Sex', 'people', 'age_gender'],
    'G05': ['Registered Marital Status by Age by Sex', 'people', 'families'],
    'G06': ['Social Marital Status by Age by Sex', 'people', 'families'],
    'G07': ['Indigenous Status by Age by Sex ', 'people', 'race_ethnicity'],
    'G08': ['Ancestry by Country of Birth of Parents', 'people', 'migration|race_ethnicity'],
    'G09': ['Country of Birth of Person by Age by Sex', 'people', 'nationality'],
    'G10': ['Country of Birth of Person by Year of Arrival in Australia', 'people', 'migration|nationality'],
    'G11': ['Proficiency in Spoken English/Language by Year of Arrival in Australia by Age', 'people',
            'language|migration'],
    'G12': ['Proficiency in Spoken English/Language of Parents by Age of Dependent Children', 'people',
            'language|families'],
    'G13': ['Language Spoken at Home by Proficiency in Spoken English/Language by Sex', 'people', 'language'],
    'G14': ['Religious Affiliation by Sex', 'people', 'religion'],
    'G15': ['Type of Educational Institution Attending (Full/Part-Time Student Status by Age) by Sex', 'people',
            'education'],
    'G16': ['Highest Year of School Completed by Age by Sex', 'people', 'education'],
    'G17': ['Total Personal Income (Weekly) by Age by Sex', 'people', 'age_gender|income'],
    'G18': ['Core Activity Need for Assistance by Age by Sex', 'people', 'income'],
    'G19': ['Voluntary Work for an Organisation or Group by Age by Sex ', 'people', 'employment'],
    'G20': ['Unpaid Domestic Work:  Number of House by Age by Sex ', 'people', 'employment'],
    'G21': ['Unpaid Assistance to a Person with a Disability by Age by Sex', 'people', 'employment'],
    'G22': ['Unpaid Child Care by Age by Sex', 'people', 'families'],
    'G23': ['Relationship in Household by Age by Sex', 'people', 'families|age_gender'],
    'G24': ['Number of Children Ever Born by Age of Parent', 'people', 'families|age_gender'],
    'G25': ['Family Composition', 'people', 'families'],
    'G26': ['Family Composition and Country of Birth of Parents by Age of Dependent Children', 'people',
            'families|nationality'],
    'G27': ['Family Blending', 'people', 'families|nationality'],
    'G28': ['Total Family Income (Weekly) by Family Composition', 'households', 'income|families'],
    'G29': ['Total Household Income (Weekly) by Household Composition', 'households', 'income'],
    'G30': ['Number of Motor Vehicles by Dwellings', 'vehicles', 'housing|transportation'],
    'G31': ['Household Composition by Number of Persons Usually Resident', 'people', 'housing'],
    'G32': ['Dwelling Structure', 'households', 'housing'],
    'G33': ['Tenure Type and Landlord Type by Dwelling Structure', 'housing_units', 'housing'],
    'G34': ['Mortgage Repayment (Monthly) by Dwelling Structure', 'households', 'housing'],
    'G35': ['Mortgage Repayment (Monthly) by Family Composition', 'households', 'families'],
    'G36': ['Rent (Weekly) by Landlord Type', 'households', 'housing'],
    'G37': ['Dwelling Internet Connection by Dwelling Structure', 'households', 'housing'],
    'G38': ['Dwelling Structure by Number of Bedrooms', 'housing_units', 'housing'],
    'G39': ['Dwelling Structure by Household Composition and Family Composition', 'housing_units', 'families|housing'],
    'G40': ['Selected Labour Force, Education and Migration Characteristics by Sex', 'people', 'employment'],
    'G41': ['Place of Usual Residence 1 Year Ago by Sex', 'people', 'housing'],
    'G42': ['Place of Usual Residence 5 Years Ago by Sex', 'people', 'housing'],
    'G43': ['Labour Force Status by Age by Sex', 'people', 'employment'],
    'G44': ['Labour Force Status by Sex of Parents by Age of Dependent Children for One Parent Families', 'people',
            'employment'],
    'G45': ['Labour Force Status by Sex of Parents by Age of Dependent Children for Couple Families', 'people',
            'employment'],
    'G46': ['Non-School Qualification:  Level of Education by Age by Sex', 'people', 'education|age_gender'],
    'G47': ['Non-School Qualification:  Field of Study by Age by Sex', 'people', 'education|age_gender'],
    'G48': ['Non-School Qualification:  Field of Study by Occupation by Sex', 'people', 'education|age_gender'],
    'G49': ['Non-School Qualification:  Level of Education by Occupation  by Sex', 'people', 'education|age_gender'],
    'G50': ['Non-School Qualification:  Level of Education by Industry of Employment by Sex', 'people',
            'education|age_gender'],
    'G51': ['Industry of Employment by Age by Sex', 'people', 'employment|age_gender'],
    'G52': ['Industry of Employment by Hours Worked by Sex', 'people', 'employment|age_gender'],
    'G53': ['Industry of Employment by Occupation', 'people', 'employment|age_gender'],
    'G54': ['Total Family Income (Weekly) by Labour Force Status of Partners for Couple Families with No Children',
            'people', 'income'],
    'G55': ['Total Family Income (Weekly) by Labour Force Status of Parents/Partners for Couple Families with Children',
            'people', 'income'],
    'G56': ['Total Family Income (Weekly) by Labour Force Status of Parent for One Parent Families', 'people',
            'income'],
    'G57': ['Occupation by Age by Sex', 'people', 'employment'],
    'G58': ['Occupation by Hours Worked by Sex', 'people', 'employment'],
    'G59': ['Method of Travel to Work by Sex', 'people', 'transportation'],
}

DENOMINATOR_FILTERS = ['LPLFS', 'P_Wife_in_RM_', 'P_Educ_85_years_and_over', 'P_Neg_Nil_incme_85_yrs_ovr',
                       'P_Tot_85_years_and_over', 'P_Hbn_in_RM', 'P_Ptn_in_DFM', '_OthF', '_CF_C_und15',
                       '_DS_sd_row', '1PF_C_und15', 'DS_Flat_apart', '1PFNC_und15', 'DS_Other_dwg', 'DS_ns',
                       'DS_Separate_hse', 'DS_Sept_house', '_CFNC', '_CF', '_1PF', '_DS_sd_rw_tc_h_th',
                       'P_NS_Tot', '_DS_Flt_apart', '_sd_rw_tce_hs_th', '_DS_over_ns',
                       'P_0_149_Tot', 'P_150_299_Tot', 'P_300_449_Tot', 'P_450_599_Tot', 'P_600_799_Tot',
                       'P_800_999_Tot', 'P_1000_1199_Tot', 'P_1200_1399_Tot', 'P_1400_1599_Tot', 'P_1600_1799_Tot',
                       'P_1800_1999_Tot', 'P_2000_2199_Tot', 'P_2200_2399_Tot', 'P_2400_2599_Tot', 'P_2600_2999_Tot',
                       'P_3000_3999_Tot', 'P_4000_4999_Tot', 'P_5000over_Tot',
                       'P_0_299_Tot', 'P_300_449_Tot', 'P_450_599_Tot', 'P_600_799_Tot', 'P_800_999_Tot',
                       'P_1000_1399_Tot', 'P_1400_1799_Tot', 'P_1800_2399_Tot', 'P_2400_2999_Tot', 'P_3000_3999_Tot',
                       'P_4000_over_Tot', 'P_LonePnt_0_14', 'P_CU15_', 'P_DpStu_', 'P_NDpChl_0_14', 'P_OthRI_0_14', 
                       'P_URI_in_FH_0_14', 'P_GrpH_Mem_0_14', 'P_LonePsn_0_14', ]
DENOMINATOR_ALIASES = {
    'all': {
        'P_Y8b_15_19': 'P_Y8b_15_19_yrs',
        'P_Y8b_20_24': 'P_Y8b_20_24_yrs',
        'P_Y8b_25_34': 'P_Y8b_25_34_yrs',
        'P_Y8b_35_44': 'P_Y8b_35_44_yrs',
        'P_Y8b_45_54': 'P_Y8b_45_54_yrs',
        'P_Y8b_55_64': 'P_Y8b_55_64_yrs',
        'P_Y8b_65_74': 'P_Y8b_65_74_yrs',
        'P_Y8b_75_84': 'P_Y8b_75_84_yrs',
        'P_Hghst_yr_schl_ns_15_19': 'P_Hghst_yr_schl_ns_15_19_yrs',
        'P_Hghst_yr_schl_ns_20_24': 'P_Hghst_yr_schl_ns_20_24_yrs',
        'P_Hghst_yr_schl_ns_25_34': 'P_Hghst_yr_schl_ns_25_34_yrs',
        'P_Hghst_yr_schl_ns_35_44': 'P_Hghst_yr_schl_ns_35_44_yrs',
        'P_Hghst_yr_schl_ns_45_54': 'P_Hghst_yr_schl_ns_45_54_yrs',
        'P_Hghst_yr_schl_ns_55_64': 'P_Hghst_yr_schl_ns_55_64_yrs',
        'P_Hghst_yr_schl_ns_65_74': 'P_Hghst_yr_schl_ns_65_74_yrs',
        'P_Hghst_yr_schl_ns_75_84': 'P_Hghst_yr_schl_ns_75_84_yrs',
        'P_Hghst_yr_schl_ns_85_ovr': 'P_Hghst_yr_schl_ns_85_yrs_ovr',
    },
    'G16A': {
        'P_Tot_15_19': 'P_Tot_15_19_yrs',
        'P_Tot_20_24': 'P_Tot_20_24_yrs',
        'P_Tot_25_34': 'P_Tot_25_34_yrs',
        'P_Tot_35_44': 'P_Tot_35_44_yrs',
        'P_Tot_45_54': 'P_Tot_45_54_yrs',
        'P_Tot_55_64': 'P_Tot_55_64_yrs',
        'P_Tot_65_74': 'P_Tot_65_74_yrs',
        'P_Tot_75_84': 'P_Tot_75_84_yrs',
    }
}


def _is_gender_column(short, gender):
    return short.startswith('{}_'.format(gender)) or short.endswith('_{}'.format(gender))


def _filter_denominators(denominators, datapack):
    filtered = denominators
    for filter_ in DENOMINATOR_FILTERS:
        filtered = list(filter(lambda x: filter_ not in x, filtered))
    filtered = [DENOMINATOR_ALIASES['all'].get(x, x) for x in filtered]
    filtered = [DENOMINATOR_ALIASES.get(datapack, {}).get(x, x) for x in filtered]

    return filtered


with open(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                       'au_metadata_columns.csv'), 'r') as infile:
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                           'Metadata_2016_GCP_DataPack.csv'), 'a') as outfile:
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tempfile_:
            reader = csv.reader(infile, delimiter=',', quotechar='"')
            tempwriter = csv.writer(tempfile_, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer = csv.writer(outfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(['Sequential', 'Short', 'Name', 'DENOMINATORS', 'Tablename', 'unit', 'subsection',
                            'Column heading description in profile', 'AGG', 'order', 'Table descr'])
            for row in reader:
                table = row[3][:3]

                if table not in tables.keys():
                    continue

                sequential = row[0]
                short = row[1]
                name = row[2].replace('_', ' ')
                datapack = row[3]
                profile_table = row[4]
                description = row[5]
                denominators = []

                if short.startswith('P_') or short.endswith('_P') and short != 'Tot_P_P':
                    denominators.append('Tot_P_P')
                elif short.startswith('M_'):
                    denominators.append('Tot_P_P')
                    denominators.append('Tot_P_M')
                    denominators.append('P_' + short[2:])
                elif short.endswith('_M'):
                    denominators.append('Tot_P_P')
                    if short != 'Tot_P_M':
                        denominators.append('Tot_P_M')
                    denominators.append(short[:short.rfind('_M')] + '_P')
                elif short.startswith('F_'):
                    denominators.append('Tot_P_P')
                    denominators.append('Tot_P_F')
                    denominators.append('P_' + short[2:])
                elif short.endswith('_F'):
                    denominators.append('Tot_P_P')
                    if short != 'Tot_P_F':
                        denominators.append('Tot_P_F')
                    denominators.append(short[:short.rfind('_F')] + '_P')

                denominators = _filter_denominators(denominators, datapack)

                tempwriter.writerow([sequential, short, name, '|'.join(denominators), datapack,
                                    tables[table][1], tables[table][2], description, None,
                                    sequential[1:], tables[table][0]])

            searches = [(_is_gender_column, 'P'), (_is_gender_column, 'F'), (_is_gender_column, 'M')]
            for search in searches:
                tempfile_.seek(0)
                tempreader = csv.reader(tempfile_, delimiter=',', quotechar='"')
                for row in tempreader:
                    short = row[1]
                    if search[0](short, search[1]):
                        writer.writerow(row)

            tempfile_.seek(0)
            tempreader = csv.reader(tempfile_, delimiter=',', quotechar='"')
            for row in tempreader:
                short = row[1]
                for search in searches:
                    if search[0](short, search[1]):
                        break
                else:
                    writer.writerow(row)
