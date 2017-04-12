# http://www.caixabankresearch.com/anuario

from luigi import Task, Parameter, LocalTarget

from tasks.util import (shell, classpath, TagsTask, TableTask, ColumnsTask,
                        DownloadUnzipTask, MetaWrapper)
from tasks.es.cnig import GeomRefColumns, Geometry
from tasks.meta import OBSColumn, OBSTag, DENOMINATOR, current_session
from tasks.tags import UnitTags, SubsectionTags, SectionTags

from xlrd import open_workbook
from collections import OrderedDict
import os

class DownloadAnuario(DownloadUnzipTask):

    year = Parameter()

    URL = 'http://www.caixabankresearch.com/documents/10180/266550/AE{year2}' \
            '_Datos_estadisticos_municipales-provinciales.zip'

    def download(self):
        shell('wget -O {output}.zip {url}'.format(
            output=self.output().path,
            url=self.URL.format(year2=self.year[-2:])
        ))


class AnuarioColumns(ColumnsTask):

    def version(self):
        return 2

    def requires(self):
        return {
            'subsections': SubsectionTags(),
            'sections': SectionTags(),
            'units': UnitTags(),
            'source': SourceTags(),
            'license': LicenseTags(),
        }

    def columns(self):
        input_ = self.input()
        spain = input_['sections']['spain']
        subsections = input_['subsections']
        units = input_['units']

        telephones = OBSColumn(
            name='Number of fixed telephone lines (landlines)',
            description='',
            type='Numeric',
            weight=2,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['telephones']],
            extra={'source': {'name': u'Tel\xe9fonos  fijos'}},
        )
        motor_vehicles = OBSColumn(
            name='Number of motor vehicles',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['vehicles']],
            extra={'source': {'name': u'Veh\xedculos de motor'}},
        )
        automobiles = OBSColumn(
            name='Number of automobiles',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['vehicles']],
            extra={'source': {'name': u'Autom\xf3viles'}},
        )
        trucks_vans = OBSColumn(
            name='Number of trucks and vans',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['vehicles']],
            extra={'source': {'name': u'Camiones y furgonetas'}},
        )
        motorcycles = OBSColumn(
            name='Number of motorcycles',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['vehicles']],
            extra={'source': {'name': u'Motocicletas'}},
        )
        buses = OBSColumn(
            name='Number of Buses',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['vehicles']],
            extra={'source': {'name': u'Autobuses'}},
        )
        industrial_tractors = OBSColumn(
            name='Number of Industrial Tractors',
            description='',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['vehicles']],
            extra={'source': {'name': u'Tractores  industriales'}},
        )
        industrial_construction_businesses = OBSColumn(
            name='Industrial and construction businesses',
            description='Number of industrial businesses (industry and '
            'construction) subject to business tax (IAE). The number of '
            'industrial businesses is practically equivalent to the number of '
            'industrial establishments in each municipality. Industrial '
            'businesses are broken down into industrial and construction.'
            'These comprise 1) energy and water; 2) extraction and processing of '
            'energy minerals and derived products, chemical industry; 3) '
            'metal processing industries, precision mechanics; 4) '
            'manufacturing; 5) construction.',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Actividades industriales (industria y construcci\xf3n)'
            }},
        )
        industrial_businesses = OBSColumn(
            name='Industrial businesses',
            description='The number of businesses involved in industry, '
            'comprising 1) energy and water; 2) extraction and processing of '
            'energy minerals and derived products, chemical industry; 3) '
            'metal processing industries, precision mechanics; 4) '
            'manufacturing.',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Actividades industriales: industria'
            }},
            targets={industrial_construction_businesses: DENOMINATOR},
        )
        energy_water_businesses = OBSColumn(
            name='Energy & water businesses',
            description='',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Energ\xeda y agua'
            }},
            targets={industrial_businesses: DENOMINATOR},
        )
        chemical_energy_extraction_businesses = OBSColumn(
            name='Chemical and energy extraction businesses',
            description='',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Extracci\xf3n y transf. min.energ y deriv.; ind.qu\xedm'
            }},
            targets={industrial_businesses: DENOMINATOR},
        )
        metal_precision_mechanics_businesses = OBSColumn(
            name='Metal processing and precision mechanics businesses',
            description='',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Industrias transf. de metales; mec. precisi\xf3n'
            }},
            targets={industrial_businesses: DENOMINATOR},
        )
        manufacturing_businesses = OBSColumn(
            name='Manufacturing businesses',
            description='',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Industrias manufactureras'
            }},
            targets={industrial_businesses: DENOMINATOR},
        )
        construction_businesses = OBSColumn(
            name='Construction businesses',
            description='',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Actividades industriales: construcci\xf3n'
            }},
            targets={industrial_construction_businesses: DENOMINATOR},
        )
        wholesale_businesses = OBSColumn(
            name='Wholesale businesses',
            description='Number of businesses in wholesale trade.  Derived from '
            'business tax (IAE), which constitute a good approximation of the '
            'number of existing in each municipality.',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Actividades comerciales mayoristas'
            }},
        )
        food_drink_tobacco_businesses = OBSColumn(
            name='Food, drink and tobacco wholesale businesses',
            description='',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Materias primas agrarias; alim., bebidas y tabaco'
            }},
            targets={wholesale_businesses: DENOMINATOR},
        )
        textile_clothing_footwear_leather_businesses = OBSColumn(
            name='Textile, clothing, footwear and leather goods wholesale businesses',
            description='',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Textiles, confecci\xf3n, calzado y art\xedculos de cuero'
            }},
            targets={wholesale_businesses: DENOMINATOR},
        )
        pharmaceutical_perfume_household_businesses = OBSColumn(
            name='Pharmaceutical, perfume, and household goods wholesale businesses',
            description='Wholesale businesses involved in pharmaceutical '
            'products, perfumery, and goods for the maintenance and operation '
            'of the household (tableware and glassware, cutlery, drugstore and '
            'cleaning, etc.)',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Productos farmac; perfum. y mant. hogar'
            }},
            targets={wholesale_businesses: DENOMINATOR},
        )
        consumer_durable_businesses = OBSColumn(
            name='Consumer durables wholesale businesses',
            description='Wholesale businsses involved in consumer durables '
            '(motor vehicles, furniture, appliances, electronics, hardware, etc.)',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Comercio al por mayor de art. consumo duradero'
            }},
            targets={wholesale_businesses: DENOMINATOR},
        )
        interindustrial_mining_chemical_businesses = OBSColumn(
            name='Interindustrial mining and chemical wholesale businesses',
            description='Wholesale businesses involved in interindustrial '
            'mining and chemical (coal, iron and steel, minerals, non-ferrous '
            'metals, oil and fuels, industrial chemicals, etc.);',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Comercio al por mayor interindustrial'
            }},
            targets={wholesale_businesses: DENOMINATOR},
        )
        other_wholesale_interindustrial_businesses = OBSColumn(
            name='Other wholesale interindustrial businesesses',
            description='Other wholesale interindustrial (textile fibers, '
            'building materials, agricultural machinery, textile machinery, '
            'office equipment, etc.)',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Otro comercio al por mayor interindustrial'
            }},
            targets={wholesale_businesses: DENOMINATOR},
        )
        other_wholesale_businesses = OBSColumn(
            name='Other wholesale businesses',
            description='Other wholesale businesses not specified above (export '
            'trade, toys and sporting goods, etc.).',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Otro comercio al por mayor no especificado'
            }},
            targets={wholesale_businesses: DENOMINATOR},
        )
        retail_businesses = OBSColumn(
            name='Retail trade businesses',
            description='Number of retail trade businesses subject to business '
            'tax (IAE). These businesses are identified with the National '
            'Classification of Economic Activities (NACE 93) considered '
            'retail. For statistical purposes, the number of commercial '
            'businesses can be considered an approach to trade, of which '
            'there is no census data (a commercial establishment may have '
            'establishments one or more activities or business licenses).',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Actividades comerciales minoristas'
            }},
        )
        food_retail_businesses = OBSColumn(
            name='Food retail trade businesses',
            description='All retail businesses involved in the trade of food',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Act. com. alimentaci\xf3n'
            }},
            targets={retail_businesses: DENOMINATOR},
        )
        trad_food_retail_businesses = OBSColumn(
            name='Traditional food retail trade businesses',
            description='Traditional trade of "Food" includes all food products '
            'purchased in the traditional retail trade (greengrocers, butchers, '
            'fishmongers, bakeries, tobacconists, drinks [alcoholic and not '
            'alcoholic] and other food).',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Act. com. tradicional'
            }},
            targets={food_retail_businesses: DENOMINATOR},
        )
        supermarkets = OBSColumn(
            name='Supermarkets',
            description='Supermarkets together include any kind of food and '
            'beverage products in self-service establishments whose floor area '
            'is less than 120 square meters (self), from 120 to 399 square '
            'meters (small supermarkets) and greater than or equal to 400 '
            'meters square (large supermarkets).',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Act. com. supermercados'
            }},
            targets={food_retail_businesses: DENOMINATOR},
        )
        non_food_retail_businesses = OBSColumn(
            name='Retail trade businesses not involved in food',
            description='All retail businesses not involved in the trade of food',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Act. com. total no alimentaci\xf3n'
            }},
            targets={retail_businesses: DENOMINATOR},
        )
        clothing_footwear_businesses = OBSColumn(
            name='Clothing and footwear retail businesses',
            description='Retail businesses selling garments and clothing '
            'accessories for men, women and children, footwear, clothing and '
            'footwear, leather and leather goods.',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Act. com vestido y calzado'
            }},
            targets={non_food_retail_businesses: DENOMINATOR},
        )
        home_goods_businesses = OBSColumn(
            name='Home goods retail businesses',
            description='Retail businesses selling furniture, home textiles, '
            'household appliances, household electrical equipment, cookware, '
            'utensils, crockery and glassware, household cleaning products '
            'and cleaning supplies; garden tools',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Act. com. hogar'
            }},
            targets={non_food_retail_businesses: DENOMINATOR},
        )
        other_non_food_retail_businesses = OBSColumn(
            name='Other non-food retail businesses',
            description='Retail businesses selling radio recreational items, '
            'TV, videos, CDs, sporting goods,. toys, musical instruments, '
            'photography, etc., stationery, books, newspapers and magazines, '
            'perfumes, cosmetics, watches, jewelery; smoking articles, '
            'travel goods, plants and flowers and pets, etc',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Act. com. resto no alimentaci\xf3n'
            }},
            targets={non_food_retail_businesses: DENOMINATOR},
        )
        mixed_integrated_trade_businesses = OBSColumn(
            name='Mixed or integrated trade businesses',
            description='',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Act. com. c. mixto y otros'
            }},
            targets={retail_businesses: DENOMINATOR},
        )
        department_stores = OBSColumn(
            name='Department stores',
            description='Establishments that offer a relatively wide and '
            'shallow assortment of consumer goods, with a range of low prices '
            'and low service. The sales area is organized into various '
            'sections and self-service or preselection.',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Act. com. grandes almacenes'
            }},

            targets={mixed_integrated_trade_businesses: DENOMINATOR},
        )
        superstores = OBSColumn(
            name='Superstores ("Hypermarkets")',
            description='Establishments that offer self-service a wide range '
            'of food products and non-food commonly consumed. They have their '
            'own parking lots and other miscellaneous services to customers. '
            'A hypermarket concept is well accepted internationally as follows: '
            'a) surface exceeding 2,500 m2 sale; b) sale in self-service '
            'consumer products, predominantly food; c) practice a policy of '
            'margins and reduced price; d) extended hours, usually '
            'uninterrupted; e) ample free parking.',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Act. com. hipermercados'
            }},
            targets={mixed_integrated_trade_businesses: DENOMINATOR},
        )
        street_vendors = OBSColumn(
            name='Street vendors and flea markets',
            description='Street trade and retail trade activities that are '
            'characterized by not having a permanent establishment. The number '
            'of activities of street trade and markets are underestimated, '
            'because in this type of economic activity registration in the '
            'IAE scoped and payment of "provincial quota" instead of '
            '"municipal tax" is possible.',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Act. com. almacenes populares'
            }},
            targets={mixed_integrated_trade_businesses: DENOMINATOR},
        )
        malls = OBSColumn(
            name='Malls',
            description='The Spanish Association of Shopping Centers defines '
            'the Mall as a "set of independent shops, planned and developed by '
            'one or more entities, whose size, business mix, common services '
            'and complementary activities are related to its environment. '
            'It has unified management. " According to the criteria of the '
            'Association, consideration Shopping Mall does not require a '
            'minimum surface for sales. The typology of centers established '
            'is based on gross leasable area (GLA) and ranges from the "urban '
            'shopping mall" (up 4,999 m2) to "very large mall" (over 79,999 m2).',
            type='Numeric',
            weight=4,
            aggregate='sum',
            tags=[subsections['commerce_economy'], spain, units['businesses']],
            extra={'source': {
                'name': u'Centros comerciales'
            }},
        )

        columns =  OrderedDict([
            ('telephones', telephones),
            ('motor_vehicles', motor_vehicles),
            ('automobiles', automobiles),
            ('trucks_vans', trucks_vans),
            ('motorcycles', motorcycles),
            ('buses', buses),
            ('industrial_tractors', industrial_tractors),
            ('industrial_construction_businesses', industrial_construction_businesses),
            ('industrial_businesses', industrial_businesses),
            ('energy_water_businesses', energy_water_businesses),
            ('chemical_energy_extraction_businesses', chemical_energy_extraction_businesses),
            ('metal_precision_mechanics_businesses', metal_precision_mechanics_businesses),
            ('manufacturing_businesses', manufacturing_businesses),
            ('construction_businesses', construction_businesses),
            ('wholesale_businesses', wholesale_businesses),
            ('food_drink_tobacco_businesses', food_drink_tobacco_businesses),
            ('textile_clothing_footwear_leather_businesses',
             textile_clothing_footwear_leather_businesses),
            ('pharmaceutical_perfume_household_businesses',
             pharmaceutical_perfume_household_businesses),
            ('consumer_durable_businesses', consumer_durable_businesses),
            ('other_wholesale_interindustrial_businesses',
             other_wholesale_interindustrial_businesses),
            ('other_wholesale_businesses', other_wholesale_businesses),
            ('retail_businesses', retail_businesses),
            ('food_retail_businesses', food_retail_businesses),
            ('trad_food_retail_businesses', trad_food_retail_businesses),
            ('supermarkets', supermarkets),
            ('non_food_retail_businesses', non_food_retail_businesses),
            ('clothing_footwear_businesses', clothing_footwear_businesses),
            ('home_goods_businesses', home_goods_businesses),
            ('other_non_food_retail_businesses', other_non_food_retail_businesses),
            ('mixed_integrated_trade_businesses', mixed_integrated_trade_businesses),
            ('department_stores', department_stores),
            ('superstores', superstores),
            ('street_vendors', street_vendors),
            ('malls', malls),
        ])

        source = input_['source']['lacaixa-source']
        license = input_['license']['lacaixa-license']
        for _, col in columns.iteritems():
            col.tags.append(source)
            col.tags.append(license)
        return columns

class SourceTags(TagsTask):
    def version(self):
        return 2

    def tags(self):
        return [
            OBSTag(id='lacaixa-source',
                   name='CaixaBank Spain Economic Yearbook',
                   type='source',
                   description='The CaixaBank publication of the `Spain Economic Yearbook <http://www.caixabankresearch.com/>`_')
        ]


class LicenseTags(TagsTask):
    def version(self):
        return 1

    def tags(self):
        return [OBSTag(id='lacaixa-license',
                       name='CaixaBank legal disclaimer',
                       type='source',
                       description='More information `here <https://www.caixabank.com/general/avisolegal_en.html>`_.')
               ]


class Anuario(TableTask):

    resolution = Parameter()
    year = Parameter()

    @property
    def infilepath(self):
        base = self.input()['data'].path
        year = str(self.year)[-2:]
        if self.resolution == 'prov':
            fname = 'AE{year}_Provincial_Completo.xls'.format(year=year)
        elif self.resolution == 'muni':
            fname = 'AE{year}_Municipal_Completo.xls'.format(year=year)
        else:
            raise RuntimeError('Unknown resolution "{}"'.format(self.resolution))

        return os.path.join(base, fname)

    def version(self):
        return 4

    def timespan(self):
        return self.year

    def requires(self):
        return {
            'data_columns': AnuarioColumns(),
            'geom_columns': GeomRefColumns(),
            'data': DownloadAnuario(year=self.year),
        }

    def columns(self):
        cols = OrderedDict()
        cols['id_' + self.resolution] = \
                self.input()['geom_columns']['id_' + self.resolution]
        cols.update(self.input()['data_columns'])
        return cols

    def populate(self):
        book = open_workbook(self.infilepath)

        # determine mapping between column names and columns in excel
        columns = self.columns()
        headers = dict()
        colnum2name = OrderedDict()
        sheets = book.sheets()
        for sheetnum, sheet in enumerate(sheets):
            headers.update(dict([
                (cell.value, (sheetnum, cellnum))
                for cellnum, cell in enumerate(sheet.row(0))
            ]))
        for out_colname, coltarget in columns.iteritems():
            col = coltarget._column
            if not col.extra or 'source' not in col.extra or 'name' not in col.extra['source']:
                continue
            sourcename = coltarget._column.extra['source']['name']
            colnum = headers.get(sourcename)
            year = unicode(int(self.year) - 1)
            if not colnum:
                colnum = headers.get(sourcename + u'  ' + year)
            if not colnum:
                colnum = headers.get(sourcename + u' ' + year)
            if not colnum:
                raise Exception('Could not find column "{}" in Excel sheets'.format(
                    sourcename))
            colnum2name[colnum] = out_colname

        # insert data
        session = current_session()
        for i in xrange(1, sheets[0].nrows):
            geom_name = sheets[0].row(i)[0].value.lower()
            geom_ref = sheets[0].row(i)[1].value
            # exclude rows that are for a different resolution
            if 'total c.a.' in geom_name:
                if self.resolution != 'cca':
                    continue
            elif 'total prov.' in geom_name:
                if self.resolution != 'prov':
                    continue
            elif 'nombre municipio' in sheets[0].row(0)[0].value.lower():
                if self.resolution != 'muni' or len(geom_ref) != 5:
                    continue
            else:
                raise RuntimeError('Unrecognized geom ref "{}"'.format(
                    geom_ref))
            values = [u"'" + geom_ref + u"'"] # geo code
            values.extend([
                str(sheets[sheetnum].row(i)[colnum].value)
                for sheetnum, colnum in colnum2name.keys()
            ])
            colnames = ['id_' + self.resolution]
            colnames.extend(colnum2name.values())
            stmt = 'INSERT INTO {output} ({colnames}) ' \
                    'VALUES ({values})'.format(
                        output=self.output().table,
                        colnames=', '.join(colnames),
                        values=', '.join(values),
                    )
            session.execute(stmt)

class AnuarioWrapper(MetaWrapper):

    resolution = Parameter()
    year = Parameter()
    params = {
        'resolution': ['muni', 'prov'],
        'year': ['2013'],
    }

    def tables(self):
        yield Anuario(resolution=self.resolution, year=self.year)
        yield Geometry(resolution=self.resolution)
