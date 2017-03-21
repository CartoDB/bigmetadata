from luigi import IntParameter, WrapperTask
from xlrd import open_workbook
from xlrd.xldate import xldate_as_tuple

from tasks.util import (TableTask, ColumnsTask, TempTableTask, DownloadUnzipTask,
                        shell, LOGGER)
from tasks.meta import current_session, OBSColumn
from collections import OrderedDict

from tasks.us.ny.nyc.columns import NYCColumns
from tasks.us.ny.nyc.tags import NYCTags
from tasks.poi import POIColumns
from datetime import datetime

import os


class DownloadPermitIssuanceMonthly(DownloadUnzipTask):

    month = IntParameter()
    year = IntParameter()

    URL = 'https://www1.nyc.gov/assets/buildings/foil/per{month}{year}excel.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(
            url=self.URL.format(month=('0' + str(self.month))[-2:],
                                year=('0' + str(self.year))[-2:]),
            output=self.output().path
        ))


class PermitIssuanceXLS2TempTableTask(TempTableTask):

    month = IntParameter()
    year = IntParameter()

    def requires(self):
        return DownloadPermitIssuanceMonthly(year=self.year, month=self.month)

    def run(self):
        book = open_workbook(os.path.join(
            self.input().path,
            'per{}{}.xls'.format(
                ('0' + str(self.month))[-2:], ('0' + str(self.year))[-2:])),
            formatting_info=True)

        sheet = book.sheets()[0]

        session = current_session()
        allvals = []
        for rownum, row in enumerate(sheet.get_rows()):
            if rownum == 2:
                coldefs = ['"{}" VARCHAR'.format(cell.value.replace(u'"', u'').strip()) for cell in row if cell.value]
                session.execute('CREATE TABLE {output} ({coldefs})'.format(
                    coldefs=', '.join(coldefs),
                    output=self.output().table
                ))
            elif rownum > 2:
                # Have to escape colons as they are interpreted as symbols otherwise
                vals = []
                for cell in row:
                    # type 0 is empty
                    if cell.ctype == 0:
                        pass
                    # type 2 is numeric, which is always a float, even if it
                    # should be an integer
                    elif cell.ctype == 2:
                        if cell.value == int(cell.value):
                            vals.append(unicode(int(cell.value)))
                        else:
                            vals.append(unicode(cell.value))
                    # type 3 is date
                    elif cell.ctype == 3:
                        vals.append(u"'{}-{}-{}'".format(*xldate_as_tuple(cell.value, 0)))
                    # everything else just pass in as unicode string, unless
                    # it's blank, in which case send in NULL
                    else:
                        if cell.value:
                            vals.append(u"'{}'".format(unicode(cell.value)\
                                                       .replace(u":", u"::") \
                                                       .replace(u"'", u"''")))
                        else:
                            vals.append('NULL')

                # Kill occasional erroneous blank last column
                if vals[-1] == "NULL":
                    vals = vals[0:-1]

                if len(vals) < len(coldefs):
                    vals.extend(["NULL"] * (len(coldefs) - len(vals)))

                if len(vals) != len(coldefs):
                    #import pdb
                    #pdb.set_trace()
                    LOGGER.error('FIXME: cannot parse year %s month %s row %s',
                                 self.year, self.month, rownum)
                    continue

                allvals.append(u', '.join(vals))
        try:
            session.execute(u'INSERT INTO {output} VALUES ({allvals})'.format(
                output=self.output().table,
                allvals=u'), ('.join(allvals)
            ))
        except Exception as err:
            import pdb
            pdb.set_trace()
            print(err)
            raise


class PermitColumns(ColumnsTask):

    def requires(self):
        return {
            'tags': NYCTags(),
        }

    def version(self):
        return 4

    def columns(self):
        #nyc = self.input()['nyc']
        #poi = self.input()['poi']
        return OrderedDict([
            ('job_num', OBSColumn(
                type='TEXT',
                weight=1,
                name='Department of Buildings Job Number'
            )),
            ('job_doc_num', OBSColumn(
                type='TEXT',
                weight=1,
                name='Department of Buildings Job document number'
            )),
            ('job_type', OBSColumn(
                type='TEXT',
                weight=1,
                name='Job Type'
            )),
            ('self_cert', OBSColumn(
                type='TEXT',
                weight=1,
                name='Self-cert'
            )),
            ('bldg_type', OBSColumn(
                type='Text',
                weight=1,
                name='Building Type',
            )),
            ('residential', OBSColumn(
                type='Text',
                weight=1,
                name='Residential',
            )),
            ('special_dist_1', OBSColumn(
                type='Text',
                weight=1,
                name='Special district 1',
            )),
            ('special_dist_2', OBSColumn(
                type='Text',
                weight=1,
                name='Special district 2',
            )),
            ('work_type', OBSColumn(
                type='Text',
                weight=1,
                name='Work Type',
                extra={
                    'categories': {
                        'BL': 'Boiler',
                        'CC': 'Curb Cut',
                        'CH': 'Chute',
                        'EQ': 'Construction Equipment',
                        'AL': 'Alteration',
                        'DM': 'Demolition & Removal',
                        'FP': 'Fire Suppression',
                        'FS': 'Fuel Storage',
                        'MH': 'Mechanical/HVAC',
                        'SD': 'Standpipe',
                        'FN': 'Fence',
                        'SP': 'Sprinkler',
                        'SF': 'Scaffold',
                        'EA': 'Earthwork Only',
                        'OT': 'Other-General Construction, Partitions, '
                              'Marquees, BPP (Builder Pavement Plan), etc.',
                        'NB': 'New Building',
                        'EW': 'Equipment Work',
                        'SG': 'Sign',
                        'FA': 'Fire Alarm',
                        'FB': 'Fuel Burning',
                        'AR': 'Architectural',
                        'FO': 'Foundation',
                        'ME': 'Mechanical',
                        'NP': 'No Plans',
                        'PL': 'Plumbing',
                        'SH': 'Sidewalk Shed',
                        'ST': 'Structural',
                        'ZO': 'Zoning',
                    }
                }
            )),
            ('permit_status', OBSColumn(
                type='Text',
                weight=1,
                name='Permit Status',
            )),
            ('filing_status', OBSColumn(
                type='Text',
                weight=1,
                name='Filing Status',
            )),
            ('permit_type', OBSColumn(
                type='Text',
                weight=1,
                name='Permit Type',
                extra={
                    'categories': {
                        'BL': 'Boiler',
                        'CC': 'Curb Cut',
                        'CH': 'Chute',
                        'EQ': 'Construction Equipment',
                        'AL': 'Alteration',
                        'DM': 'Demolition & Removal',
                        'FP': 'Fire Suppression',
                        'FS': 'Fuel Storage',
                        'MH': 'Mechanical/HVAC',
                        'SD': 'Standpipe',
                        'FN': 'Fence',
                        'SP': 'Sprinkler',
                        'SF': 'Scaffold',
                        'EA': 'Earthwork Only',
                        'OT': 'Other-General Construction, Partitions, '
                              'Marquees, BPP (Builder Pavement Plan), etc.',
                        'NB': 'New Building',
                        'EW': 'Equipment Work',
                        'SG': 'Sign',
                        'FA': 'Fire Alarm',
                        'FB': 'Fuel Burning',
                        'AR': 'Architectural',
                        'FO': 'Foundation',
                        'ME': 'Mechanical',
                        'NP': 'No Plans',
                        'PL': 'Plumbing',
                        'SH': 'Sidewalk Shed',
                        'ST': 'Structural',
                        'ZO': 'Zoning',
                    }
                }
            )),
            ('permit_sequence', OBSColumn(
                type='Numeric',
                weight=1,
                name='Permit Sequence Number',
            )),
            ('permit_subtype', OBSColumn(
                type='Text',
                name='Permit Subtype',
                weight=1,
                extra={
                    'categories': {
                        'BL': 'Boiler',
                        'CC': 'Curb Cut',
                        'CH': 'Chute',
                        'EQ': 'Construction Equipment',
                        'AL': 'Alteration',
                        'DM': 'Demolition & Removal',
                        'FP': 'Fire Suppression',
                        'FS': 'Fuel Storage',
                        'MH': 'Mechanical/HVAC',
                        'SD': 'Standpipe',
                        'FN': 'Fence',
                        'SP': 'Sprinkler',
                        'SF': 'Scaffold',
                        'EA': 'Earthwork Only',
                        'OT': 'Other-General Construction, Partitions, '
                              'Marquees, BPP (Builder Pavement Plan), etc.',
                        'NB': 'New Building',
                        'EW': 'Equipment Work',
                        'SG': 'Sign',
                        'FA': 'Fire Alarm',
                        'FB': 'Fuel Burning',
                        'AR': 'Architectural',
                        'FO': 'Foundation',
                        'ME': 'Mechanical',
                        'NP': 'No Plans',
                        'PL': 'Plumbing',
                        'SH': 'Sidewalk Shed',
                        'ST': 'Structural',
                        'ZO': 'Zoning',
                    }
                }
            )),
            ('oil_gas', OBSColumn(
                type='Text',
                weight=1,
                name='Oil gas',
            )),
            ('site_fill', OBSColumn(
                type='Text',
                weight=1,
                name='Site fill',
            )),
            ('filing_date', OBSColumn(
                type='Date',
                weight=1,
                name='Filing date',
            )),
            ('issuance_date', OBSColumn(
                type='Date',
                weight=1,
                name='Issuance date',
            )),
            ('expiration_date', OBSColumn(
                type='Date',
                weight=1,
                name='Expiration date',
            )),
            ('job_start_date', OBSColumn(
                type='Date',
                weight=1,
                name='Job start date',
            )),
            ('permittee_first_last_name', OBSColumn(
                type='Text',
                weight=1,
                name='Permittee first & last name',
            )),
            ('permittee_business_name', OBSColumn(
                type='Text',
                weight=1,
                name='Permittee business name',
            )),
            ('permittee_phone', OBSColumn(
                type='Text',
                weight=1,
                name='Permittee phone',
            )),
            ('permittee_license_type', OBSColumn(
                type='Text',
                weight=1,
                name='Permittee license type',
            )),
            ('permittee_license_number', OBSColumn(
                type='Text',
                weight=1,
                name='Permittee license number',
            )),
            ('permittee_other_title', OBSColumn(
                type='Text',
                weight=1,
                name='Permittee Other Title',
            )),
            ('acts_as_superintendent', OBSColumn(
                type='Text',
                weight=1,
                name='Acts as superintent',
            )),
            ('hic_license', OBSColumn(
                type='Text',
                weight=1,
                name='HIC License',
            )),
            ('site_safety_mgrs_name', OBSColumn(
                type='Text',
                weight=1,
                name="Site Safety Manager's name",
            )),
            ('site_safety_mgr_business_name', OBSColumn(
                type='Text',
                weight=1,
                name="Site Safety Manager's Business Name",
            )),
            ('superintendent_first_last_name', OBSColumn(
                type='Text',
                weight=1,
                name="Superintendent first & last name",
            )),
            ('superintendent_business_name', OBSColumn(
                type='Text',
                weight=1,
                name="Superintent business name",
            )),
            ('owner_business_type', OBSColumn(
                type='Text',
                weight=1,
                name="Owner's business type",
            )),
            ('non_profit', OBSColumn(
                type='Text',
                weight=1,
                name="Non-Profit",
            )),
            ('owner_business_name', OBSColumn(
                type='Text',
                weight=1,
                name="Owner's business name",
            )),
            ('owner_first_last_name', OBSColumn(
                type='Text',
                weight=1,
                name="Owner's first and last name",
            )),
            ('owner_house_street', OBSColumn(
                type='Text',
                weight=1,
                name="Owner's house street",
            )),
            ('city_state_zip', OBSColumn(
                type='Text',
                weight=1,
                name='City, state and zip',
            )),
            ('owner_phone_number', OBSColumn(
                type='Text',
                weight=1,
                name="Owner's phone number",
            )),
        ])

    def tags(self, input_, col_key, col):
        return [input_['tags']['nyc']]


class PermitIssuance(TableTask):

    def version(self):
        return 2

    def requires(self):
        data_tables = {}
        now = datetime.now()
        for year in xrange(3, 18):
            # 2003 only has from March onwards but we skip it because of
            # different schema -- no Self-Cert
            # 2004 onwards seems similar but no "Site Fill"
            if year < 10:
                continue
            # current year, only grab to prior month
            elif year == now.year - 2000:
                months = xrange(1, now.month)
            # grab all months
            else:
                months = xrange(1, 13)

            for month in months:
                data_tables[year, month] = \
                        PermitIssuanceXLS2TempTableTask(year=year, month=month)

        return {
            'poi_columns': POIColumns(),
            'nyc_columns': NYCColumns(),
            'permit_columns': PermitColumns(),
            'data': data_tables,
        }

    def columns(self):
        input_ = self.input()
        poi = input_['poi_columns']
        nyc = input_['nyc_columns']
        permit = input_['permit_columns']
        return OrderedDict([
            ('bbl', nyc['bbl']),
            ('borough', nyc['borough']),
            ('bin', nyc['bin']),
            ('house_number', poi['house_number']),
            ('street_name', poi['street_name']),
            ('job_num', permit['job_num']),
            ('job_doc_num', permit['job_doc_num']),
            ('job_type', permit['job_type']),
            ('self_cert', permit['self_cert']),
            ('block', nyc['block']),
            ('lot', nyc['lot']),
            ('cd', nyc['cd']),
            ('zip', poi['postal_code']),
            ('bldg_type', permit['bldg_type']),
            ('residential', permit['residential']),
            ('special_dist_1', permit['special_dist_1']),
            ('special_dist_2', permit['special_dist_2']),
            ('work_type', permit['work_type']),
            ('permit_status', permit['permit_status']),
            ('filing_status', permit['filing_status']),
            ('permit_type', permit['permit_type']),
            ('permit_sequence', permit['permit_sequence']),
            ('permit_subtype', permit['permit_subtype']),
            ('oil_gas', permit['oil_gas']),
            ('site_fill', permit['site_fill']),
            ('filing_date', permit['filing_date']),
            ('issuance_date', permit['issuance_date']),
            ('expiration_date', permit['expiration_date']),
            ('job_start_date', permit['job_start_date']),
            ('permittee_first_last_name', permit['permittee_first_last_name']),
            ('permittee_business_name', permit['permittee_business_name']),
            ('permittee_phone', permit['permittee_phone']),
            ('permittee_license_type', permit['permittee_license_type']),
            ('permittee_license_number', permit['permittee_license_number']),
            ('permittee_other_title', permit['permittee_other_title']),
            ('acts_as_superintendent', permit['acts_as_superintendent']),
            ('hic_license', permit['hic_license']),
            ('site_safety_mgrs_name', permit['site_safety_mgrs_name']),
            ('site_safety_mgr_business_name', permit['site_safety_mgr_business_name']),
            ('superintendent_first_last_name', permit['superintendent_first_last_name']),
            ('superintendent_business_name', permit['superintendent_business_name']),
            ('owner_business_type', permit['owner_business_type']),
            ('non_profit', permit['non_profit']),
            ('owner_business_name', permit['owner_business_name']),
            ('owner_first_last_name', permit['owner_first_last_name']),
            ('owner_house_street', permit['owner_house_street']),
            ('city_state_zip', permit['city_state_zip']),
            ('owner_phone_number', permit['owner_phone_number']),
        ])

    def timespan(self):
        return 'current'

    def populate(self):
        input_ = self.input()
        session = current_session()
        for yearmonth, data in input_['data'].iteritems():
            year, month = yearmonth
            try:
                session.execute('''
                    INSERT INTO {output}
                    SELECT CASE SUBSTR(LOWER("Borough"), 1, 5)
                             WHEN 'state' THEN '5'
                             WHEN 'queen' THEN '4'
                             WHEN 'brook' THEN '3'
                             WHEN 'manha' THEN '1'
                             WHEN 'bronx' THEN '2'
                             ELSE NULL
                           END || LPAD("Block", 5, '0') || LPAD("Lot", 4, '0'),
                           "Borough"::Text,
                           "Bin #"::Text,
                           "House #"::Text,
                           "Street Name"::Text,
                           "Job #"::Text,
                           "Job doc. #"::Text,
                           "Job Type"::Text,
                           "Self-Cert"::Text,
                           "Block"::Text,
                           "Lot"::Text,
                           "Community Board"::Text,
                           "Zip Code"::Text,
                           "Bldg Type"::Text,
                           "Residential"::Text,
                           "Special District 1"::Text,
                           "Special District 2"::Text,
                           "Work Type"::Text,
                           "Permit Status"::Text,
                           "Filing Status"::Text,
                           "Permit Type"::Text,
                           "Permit Sequence #"::Numeric,
                           "Permit Subtype"::Text,
                           "Oil Gas"::Text,
                           "Site Fill"::Text,
                           NullIf("Filing Date", '//0')::Date,
                           NullIf("Issuance Date", '//0')::Date,
                           NullIf("Expiration Date", '//0')::Date,
                           NullIf(NullIf(NullIf(NullIf("Job Start Date",
                                '0'), '20070009'), '//0'), '4553451R')::Date,
                           "Permittee's First & Last Name"::Text,
                           "Permittee's Business Name"::Text,
                           "Permittee's Phone #"::Text,
                           "Permittee's License Type"::Text,
                           "Permittee's License #"::Text,
                           "Permittee's Other Title"::Text,
                           "Acts As Superintendent"::Text,
                           "HIC License"::Text,
                           "Site Safety Mgr's Name"::Text,
                           "Site Safety Mgr Business Name"::Text,
                           "Superintendent First & Last Name"::Text,
                           "Superintendent Business Name"::Text,
                           "Owner's Business Type"::Text,
                           "Non-Profit"::Text,
                           "Owner's Business Name"::Text,
                           "Owner's First & Last Name"::Text,
                           "Owner's House Street"::Text,
                           "City, State, Zip"::Text,
                           "Owner's Phone #"::Text
                    FROM {intable}
                    WHERE "Borough" IS NOT NULL
                '''.format(output=self.output().table,
                           intable=data.table))
            except Exception as err:
                LOGGER.error('%s', err)
                session.rollback()
                raise
        session.execute('''
            create index on {output} (bbl)
        '''.format(output=self.output().table))
