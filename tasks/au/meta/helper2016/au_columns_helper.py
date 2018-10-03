'''
Generator for the `Metadata_2016_GCP_DataPack.csv` file from `au_metadata_columns.csv`
Note: `au_metadata_columns.csv` is a CSV with the columns extracted manually from `Metadata_2016_GCP_DataPack.xlsx`
'''

import csv
import os
import tempfile

tables = {
    "G01": ["Selected Person Characteristics by Sex", "people", "age_gender"],
    "G03": ["Place of Usual Residence on Census Night by Age", "people", "age_gender"],
    "G04": ["Age by Sex", "people", "age_gender"],
    "G05": ["Registered Marital Status by Age by Sex", "people", "families"],
    "G06": ["Social Marital Status by Age by Sex", "people", "families"],
    "G07": ["Indigenous Status by Age by Sex ", "people", "race_ethnicity"],
    "G08": ["Ancestry by Country of Birth of Parents", "people", "migration|race_ethnicity"],
    "G09": ["Country of Birth of Person by Age by Sex", "people", "nationality"],
    "G10": ["Country of Birth of Person by Year of Arrival in Australia", "people", "migration|nationality"],
    "G11": ["Proficiency in Spoken English/Language by Year of Arrival in Australia by Age", "people", "language|migration"],
    "G12": ["Proficiency in Spoken English/Language of Parents by Age of Dependent Children", "people", "language|families"],
    "G13": ["Language Spoken at Home by Proficiency in Spoken English/Language by Sex", "people", "language"],
    "G14": ["Religious Affiliation by Sex", "people", "religion"],
    "G15": ["Type of Educational Institution Attending (Full/Part-Time Student Status by Age) by Sex", "people", "education"],
    "G16": ["Highest Year of School Completed by Age by Sex", "people", "education"],
    "G17": ["Total Personal Income (Weekly) by Age by Sex", "people", "age_gender|income"],
    "G18": ["Core Activity Need for Assistance by Age by Sex", "people", "income"],
    "G19": ["Voluntary Work for an Organisation or Group by Age by Sex ", "people", "employment"],
    "G20": ["Unpaid Domestic Work:  Number of House by Age by Sex ", "people", "employment"],
    "G21": ["Unpaid Assistance to a Person with a Disability by Age by Sex", "people", "employment"],
    "G22": ["Unpaid Child Care by Age by Sex", "people", "families"],
    "G23": ["Relationship in Household by Age by Sex", "people", "families|age_gender"],
    "G24": ["Number of Children Ever Born by Age of Parent", "people", "families|age_gender"],
    "G25": ["Family Composition", "people", "families"],
    "G26": ["Family Composition and Country of Birth of Parents by Age of Dependent Children", "people", "families|nationality"],
    "G27": ["Family Blending", "people", "families|nationality"],
    "G28": ["Total Family Income (Weekly) by Family Composition", "households", "income|families"],
    "G29": ["Total Household Income (Weekly) by Household Composition", "households", "income"],
    "G30": ["Number of Motor Vehicles by Dwellings", "vehicles", "housing|transportation"],
    "G31": ["Household Composition by Number of Persons Usually Resident", "people", "housing"],
    "G32": ["Dwelling Structure", "households", "housing"],
    "G33": ["Tenure Type and Landlord Type by Dwelling Structure", "housing_units", "housing"],
    "G34": ["Mortgage Repayment (Monthly) by Dwelling Structure", "households", "housing"],
    "G35": ["Mortgage Repayment (Monthly) by Family Composition", "households", "families"],
    "G36": ["Rent (Weekly) by Landlord Type", "households", "housing"],
    "G37": ["Dwelling Internet Connection by Dwelling Structure", "households", "housing"],
    "G38": ["Dwelling Structure by Number of Bedrooms", "housing_units", "housing"],
    "G39": ["Dwelling Structure by Household Composition and Family Composition", "housing_units", "families|housing"],
    "G40": ["Selected Labour Force, Education and Migration Characteristics by Sex", "people", "employment"],
    "G41": ["Place of Usual Residence 1 Year Ago by Sex", "people", "housing"],
    "G42": ["Place of Usual Residence 5 Years Ago by Sex", "people", "housing"],
    "G43": ["Labour Force Status by Age by Sex", "people", "employment"],
    "G44": ["Labour Force Status by Sex of Parents by Age of Dependent Children for One Parent Families", "people", "employment"],
    "G45": ["Labour Force Status by Sex of Parents by Age of Dependent Children for Couple Families", "people", "employment"],
    "G46": ["Non-School Qualification:  Level of Education by Age by Sex", "people", "education|age_gender"],
    "G47": ["Non-School Qualification:  Field of Study by Age by Sex", "people", "education|age_gender"],
    "G48": ["Non-School Qualification:  Field of Study by Occupation by Sex", "people", "education|age_gender"],
    "G49": ["Non-School Qualification:  Level of Education by Occupation  by Sex", "people", "education|age_gender"],
    "G50": ["Non-School Qualification:  Level of Education by Industry of Employment by Sex", "people", "education|age_gender"],
    "G51": ["Industry of Employment by Age by Sex", "people", "employment|age_gender"],
    "G52": ["Industry of Employment by Hours Worked by Sex", "people", "employment|age_gender"],
    "G53": ["Industry of Employment by Occupation", "people", "employment|age_gender"],
    "G54": ["Total Family Income (Weekly) by Labour Force Status of Partners for Couple Families with No Children", "people", "income"],
    "G55": ["Total Family Income (Weekly) by Labour Force Status of Parents/Partners for Couple Families with Children", "people", "income"],
    "G56": ["Total Family Income (Weekly) by Labour Force Status of Parent for One Parent Families", "people", "income"],
    "G57": ["Occupation by Age by Sex", "people", "employment"],
    "G58": ["Occupation by Hours Worked by Sex", "people", "employment"],
    "G59": ["Method of Travel to Work by Sex", "people", "transportation"],
}


def _is_gender_column(short, gender):
    return short.startswith('{}_'.format(gender)) or short.endswith('_{}'.format(gender))


def _is_other_column(short, _):
    return not short.startswith('P_') and not short.endswith('_P') \
           and not short.startswith('M_') and not short.endswith('_M') \
           and not short.startswith('F_') and not short.endswith('_F')


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
                    denominators.append(short[:short.find('_M')] + '_P')
                elif short.startswith('F_'):
                    denominators.append('Tot_P_P')
                    denominators.append('Tot_P_F')
                    denominators.append('P_' + short[2:])
                elif short.endswith('_F'):
                    denominators.append('Tot_P_P')
                    if short != 'Tot_P_F':
                        denominators.append('Tot_P_F')
                    denominators.append(short[:short.find('_F')] + '_P')

                tempwriter.writerow([sequential, short, name, '|'.join(denominators), datapack,
                                    tables[table][1], tables[table][2], description, None,
                                    sequential[1:], tables[table][0]])

            searches = [(_is_gender_column, 'P'), (_is_gender_column, 'F'), (_is_gender_column, 'M'),
                        (_is_other_column, '')]
            for search in searches:
                tempfile_.seek(0)
                tempreader = csv.reader(tempfile_, delimiter=',', quotechar='"')
                for row in tempreader:
                    short = row[1]
                    if search[0](short, search[1]):
                        writer.writerow(row)
