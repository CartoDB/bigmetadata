from tasks.meta import current_session
from tasks.util import TempTableTask, WrapperTask, TableToCartoViaImportAPI, query_cartodb

from tasks.us.ny.nyc.dob import PermitIssuance, PermitColumns
from tasks.us.ny.nyc.dcp import MapPLUTO, MapPLUTOColumns

from luigi import Parameter, IntParameter, Task


def _case_for(colname, col, ifnull=None):
    '''
    generate case statement for the categories part of a column
    '''
    session = current_session()
    ifnull = ifnull or colname
    whens = [" WHEN '{k}' THEN '{v}' ".format(k=k.replace("'", "''"),
                                              v=v.replace("'", "''"))
             for k, v in col.get(session).extra['categories'].iteritems()]
    return ' CASE "{colname}" {whens} ELSE {ifnull} END '.format(
        colname=colname,
        whens='\n'.join(whens),
        ifnull=ifnull
    )


class PointPLUTO(TempTableTask):

    release = Parameter()

    def requires(self):
        return MapPLUTO(release=self.release)

    def run(self):
        session = current_session()
        session.execute('''
            CREATE TABLE {output} AS SELECT * FROM {input}
        '''.format(output=self.output().table,
                   input=self.input().table))
        session.execute('''
            UPDATE {output} SET wkb_geometry = ST_PointOnSurface(wkb_geometry)
        '''.format(output=self.output().table))


class Permits(TempTableTask):

    year = IntParameter()

    def requires(self):
        return {
            'permits': PermitIssuance(),
            'pluto': PointPLUTO(release='16v2'),
            'permit_cols': PermitColumns(),
        }

    def run(self):
        input_ = self.input()
        permit_cols = input_['permit_cols']
        session = current_session()
        session.execute('''
            CREATE TABLE {output} AS
            SELECT pluto.wkb_geometry, permits.*,
                   {permit_type} permit_type_text,
                   {permit_subtype} permit_subtype_text,
                   {work_type} work_type_text
            FROM {permits} permits, {pluto} pluto
            WHERE pluto.bbl = permits.bbl
              AND date_part('year', issuance_date) = '{year}'
                        '''.format(permits=input_['permits'].table,
                                   pluto=input_['pluto'].table,
                                   year=self.year,
                                   permit_type=_case_for(
                                       'permit_type', permit_cols['permit_type']),
                                   permit_subtype=_case_for(
                                       'permit_subtype', permit_cols['permit_subtype']),
                                   work_type=_case_for(
                                       'work_type', permit_cols['work_type']),
                                   output=self.output().table))


class PermitPLUTO(TempTableTask):

    year = IntParameter()

    def requires(self):
        return {
            'permits': Permits(year=self.year),
            'pluto': PointPLUTO(release='16v2'),
            'pluto_cols': MapPLUTOColumns(),
        }

    def run(self):
        input_ = self.input()
        pluto_cols = input_['pluto_cols']
        session = current_session()
        session.execute('''
            CREATE TABLE {output} AS
            SELECT pluto.*,
                        {bldgclass_text} bldgclass_text,
                        {landuse_text} landuse_text,
                        json_agg(json_build_object(
                                     'bin', bin,
                                     'house_number', house_number,
                                     'street_name', street_name,
                                     'job_num', job_num,
                                     'job_doc_num', job_doc_num,
                                     'job_type', job_type,
                                     'self_cert', self_cert,
                                     'bldg_type', bldg_type,
                                     'residential', residential,
                                     'special_dist_1', special_dist_1,
                                     'special_dist_2', special_dist_2,
                                     'work_type_text', work_type_text,
                                     'permit_status', permit_status,
                                     'filing_status', filing_status,
                                     'permit_type_text', permit_type_text,
                                     'permit_sequence', permit_sequence,
                                     'permit_subtype_text', permit_subtype_text,
                                     'oil_gas', oil_gas,
                                     'site_fill', site_fill,
                                     'filing_date', filing_date,
                                     'issuance_date', issuance_date,
                                     'expiration_date', expiration_date,
                                     'job_start_date', job_start_date,
                                     'permittee_first_last_name', permittee_first_last_name,
                                     'permittee_business_name', permittee_business_name,
                                     'permittee_phone', permittee_phone,
                                     'permittee_license_type', permittee_license_type,
                                     'permittee_license_number', permittee_license_number,
                                     'permittee_other_title', permittee_other_title,
                                     'acts_as_superintendent', acts_as_superintendent,
                                     'hic_license', hic_license,
                                     'site_safety_mgrs_name', site_safety_mgrs_name,
                                     'site_safety_mgr_business_name', site_safety_mgr_business_name,
                                     'superintendent_first_last_name', superintendent_first_last_name,
                                     'superintendent_business_name', superintendent_business_name,
                                     'owner_business_type', owner_business_type,
                                     'non_profit', non_profit,
                                     'owner_business_name', owner_business_name,
                                     'owner_first_last_name', owner_first_last_name,
                                     'owner_house_street', owner_house_street,
                                     'city_state_zip', city_state_zip,
                                     'owner_phone_number', owner_phone_number
                       )) popup
            FROM {permits} permits, {pluto} pluto
            WHERE pluto.bbl = permits.bbl
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
                 ,10,11,12,13,14,15,16,17,18,19
                 ,20,21,22,23,24,25,26,27,28,29
                 ,30,31,32,33,34,35,36,37,38,39
                 ,40,41,42,43,44,45,46,47,48,49
                 ,50,51,52,53,54,55,56,57,58,59
                 ,60,61,62,63,64,65,66,67,68,69
                 ,70,71,72,73,74,75,76,77,78,79
                 ,80,81,82,83,84,85,86
                        '''.format(permits=input_['permits'].table,
                                   pluto=input_['pluto'].table,
                                   bldgclass_text=_case_for('bldgclass', pluto_cols['bldgclass']),
                                   landuse_text=_case_for('landuse', pluto_cols['landuse']),
                                   year=self.year,
                                   output=self.output().table))



class AllPermitData(WrapperTask):

    def requires(self):
        for year in xrange(2010, 2018):
            yield PermitPLUTO(year=year)
            yield Permits(year=year)


class Upload(WrapperTask):

    username = Parameter()
    api_key = Parameter()

    def requires(self):

        #yield TableToCartoViaImportAPI(
        #    username=self.username,
        #    api_key=self.api_key,
        #    schema='observatory',
        #    table=PermitIssuance().output()._tablename,
        #    outname='permits'
        #)
        #yield TableToCartoViaImportAPI(
        #    username=self.username,
        #    api_key=self.api_key,
        #    schema=PointPLUTO(release='16v2').output()._schema,
        #    table=PointPLUTO(release='16v2').output()._tablename,
        #    outname='pluto_16v2',
        #)
        for year in xrange(2010, 2018):
            yield TableToCartoViaImportAPI(
                username=self.username,
                api_key=self.api_key,
                schema=PermitPLUTO(year=year).output()._schema,
                table=PermitPLUTO(year=year).output()._tablename,
                outname='pluto_permits_{year}'.format(year=year)
            )
            yield TableToCartoViaImportAPI(
                username=self.username,
                api_key=self.api_key,
                schema=Permits(year=year).output()._schema,
                table=Permits(year=year).output()._tablename,
                outname='permits_{year}'.format(year=year)
            )


class TweakACRIS(Task):

    username = Parameter()
    api_key = Parameter()
    year = Parameter()

    def run(self):
        self._complete = True
        queries = [
            '''
              ALTER TABLE acris_real_property_groupby_{year}
              ADD COLUMN bldgarea_orig Numeric
            ''',
            '''
              UPDATE acris_real_property_groupby_{year}
              SET bldgarea_orig = bldgarea
            ''',
            '''
              UPDATE acris_real_property_groupby_{year}
              SET bldgarea = CASE WHEN bldgarea_orig > 50000 THEN NULL ELSE bldgarea_orig END
            ''',
            '''
              ALTER TABLE acris_real_property_groupby_{year}
              ADD COLUMN lotarea_orig Numeric
            ''',
            '''
              UPDATE acris_real_property_groupby_{year}
              SET lotarea_orig = lotarea
            ''',
            '''
              UPDATE acris_real_property_groupby_{year}
              SET lotarea = CASE WHEN lotarea_orig > 50000 THEN NULL ELSE lotarea_orig END
            '''
        ]
        for q in queries:
            resp = query_cartodb(q.format(year=self.year),
                                 carto_url='https://' + self.username + '.carto.com',
                                 api_key=self.api_key)
            if 'already exists' in resp.text:
                continue
            else:
                assert resp.status_code == 200

    def complete(self):
        return getattr(self, '_complete', False)


class TweakAllACRIS(WrapperTask):

    def requires(self):

        for year in xrange(2013, 2018):
            yield TweakACRIS(year=str(year))
