
from tasks.meta import OBSColumn, DENOMINATOR, UNIVERSE
from tasks.util import ColumnsTask
from tasks.tags import SectionTags, SubsectionTags, UnitTags

from collections import OrderedDict

class Pessoa13Columns(ColumnsTask):
                    
    def requires(self):
        return {
            'sections': SectionTags(),
            'subsections': SubsectionTags(),
            'units': UnitTags(),
        }

    def version(self):
        return 3

    def columns(self):


        V001 = OBSColumn(
            id='V001',
            name='Pessoas residentes em domicílios particulares e domicílios coletivos',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={},)

        V002 = OBSColumn(
            id='V002',
            name='Pessoas residentes em domicílios particulares permanentes',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V001: DENOMINATOR },)

        V003 = OBSColumn(
            id='V003',
            name='Responsáveis pelos domicílios particulares',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V004 = OBSColumn(
            id='V004',
            name='Cônjuges ou companheiros(as) (de sexo diferente e do mesmo sexo da pessoa responsável) em domicílios particulares ',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V005 = OBSColumn(
            id='V005',
            name='Filhos(as) do responsável e do cônjuge em domicílios particulares',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V006 = OBSColumn(
            id='V006',
            name='Filhos(as) somente do responsável em domicílios particulares',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V007 = OBSColumn(
            id='V007',
            name='Enteados(as) em domicílios particulares',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V008 = OBSColumn(
            id='V008',
            name='Genros ou noras em domicílios particulares',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V009 = OBSColumn(
            id='V009',
            name='Pais, mães, padrastos ou madrastas em domicílios particulares',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V010 = OBSColumn(
            id='V010',
            name='Sogros (as) em domicílios particulares',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V011 = OBSColumn(
            id='V011',
            name='Netos(as) em domicílios particulares',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V012 = OBSColumn(
            id='V012',
            name='Bisnetos(as) em domicílios particulares',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V013 = OBSColumn(
            id='V013',
            name='Irmãos ou irmãs em domicílios particulares',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V014 = OBSColumn(
            id='V014',
            name='Avôs ou avós em domicílios particulares',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V015 = OBSColumn(
            id='V015',
            name='Outros parentes em domicílios particulares',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V016 = OBSColumn(
            id='V016',
            name='Agregados(as) em domicílios particulares',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V017 = OBSColumn(
            id='V017',
            name='Conviventes em domicílios particulares',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V018 = OBSColumn(
            id='V018',
            name='Pensionistas em domicílios particulares',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V019 = OBSColumn(
            id='V019',
            name='Empregados(as) domésticos(as) em domicílios particulares',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V020 = OBSColumn(
            id='V020',
            name='Parentes de empregados(as) domésticos(as) em domicílios particulares',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V002: DENOMINATOR },)

        V021 = OBSColumn(
            id='V021',
            name='Individuais em domicílio coletivo',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Housing']],
                            targets={ V001: DENOMINATOR },)

        V022 = OBSColumn(
            id='V022',
            name='Pessoas com menos de 1 ano de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V023 = OBSColumn(
            id='V023',
            name='Pessoas com menos de 1 mês de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V022: DENOMINATOR },)

        V024 = OBSColumn(
            id='V024',
            name='Pessoas com 1 mês de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V022: DENOMINATOR },)

        V025 = OBSColumn(
            id='V025',
            name='Pessoas com 2 meses de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V022: DENOMINATOR },)

        V026 = OBSColumn(
            id='V026',
            name='Pessoas com 3 meses de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V022: DENOMINATOR },)

        V027 = OBSColumn(
            id='V027',
            name='Pessoas com 4 meses de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V022: DENOMINATOR },)

        V028 = OBSColumn(
            id='V028',
            name='Pessoas com 5 meses de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V022: DENOMINATOR },)

        V029 = OBSColumn(
            id='V029',
            name='Pessoas com 6 meses de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V022: DENOMINATOR },)

        V030 = OBSColumn(
            id='V030',
            name='Pessoas com 7 meses de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V022: DENOMINATOR },)

        V031 = OBSColumn(
            id='V031',
            name='Pessoas com 8 meses de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V022: DENOMINATOR },)

        V032 = OBSColumn(
            id='V032',
            name='Pessoas com 9 meses de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V022: DENOMINATOR },)

        V033 = OBSColumn(
            id='V033',
            name='Pessoas com 10 meses de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V022: DENOMINATOR },)

        V034 = OBSColumn(
            id='V034',
            name='Pessoas com 11 meses de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V022: DENOMINATOR },)

        V035 = OBSColumn(
            id='V035',
            name='Pessoas de 1 ano de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V036 = OBSColumn(
            id='V036',
            name='Pessoas com 2 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V037 = OBSColumn(
            id='V037',
            name='Pessoas com 3 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V038 = OBSColumn(
            id='V038',
            name='Pessoas com 4 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V039 = OBSColumn(
            id='V039',
            name='Pessoas com 5 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V040 = OBSColumn(
            id='V040',
            name='Pessoas com 6 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V041 = OBSColumn(
            id='V041',
            name='Pessoas com 7 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V042 = OBSColumn(
            id='V042',
            name='Pessoas com 8 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V043 = OBSColumn(
            id='V043',
            name='Pessoas com 9 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V044 = OBSColumn(
            id='V044',
            name='Pessoas com 10 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V045 = OBSColumn(
            id='V045',
            name='Pessoas com 11 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V046 = OBSColumn(
            id='V046',
            name='Pessoas com 12 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V047 = OBSColumn(
            id='V047',
            name='Pessoas com 13 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V048 = OBSColumn(
            id='V048',
            name='Pessoas com 14 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V049 = OBSColumn(
            id='V049',
            name='Pessoas com 15 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V050 = OBSColumn(
            id='V050',
            name='Pessoas com 16 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V051 = OBSColumn(
            id='V051',
            name='Pessoas com 17 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V052 = OBSColumn(
            id='V052',
            name='Pessoas com 18 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V053 = OBSColumn(
            id='V053',
            name='Pessoas com 19 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V054 = OBSColumn(
            id='V054',
            name='Pessoas com 20 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V055 = OBSColumn(
            id='V055',
            name='Pessoas com 21 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V056 = OBSColumn(
            id='V056',
            name='Pessoas com 22 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V057 = OBSColumn(
            id='V057',
            name='Pessoas com 23 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V058 = OBSColumn(
            id='V058',
            name='Pessoas com 24 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V059 = OBSColumn(
            id='V059',
            name='Pessoas com 25 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V060 = OBSColumn(
            id='V060',
            name='Pessoas com 26 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V061 = OBSColumn(
            id='V061',
            name='Pessoas com 27 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V062 = OBSColumn(
            id='V062',
            name='Pessoas com 28 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V063 = OBSColumn(
            id='V063',
            name='Pessoas com 29 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V064 = OBSColumn(
            id='V064',
            name='Pessoas com 30 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V065 = OBSColumn(
            id='V065',
            name='Pessoas com 31 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V066 = OBSColumn(
            id='V066',
            name='Pessoas com 32 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V067 = OBSColumn(
            id='V067',
            name='Pessoas com 33 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V068 = OBSColumn(
            id='V068',
            name='Pessoas com 34 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V069 = OBSColumn(
            id='V069',
            name='Pessoas com 35 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V070 = OBSColumn(
            id='V070',
            name='Pessoas com 36 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V071 = OBSColumn(
            id='V071',
            name='Pessoas com 37 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V072 = OBSColumn(
            id='V072',
            name='Pessoas com 38 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V073 = OBSColumn(
            id='V073',
            name='Pessoas com 39 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V074 = OBSColumn(
            id='V074',
            name='Pessoas com 40 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V075 = OBSColumn(
            id='V075',
            name='Pessoas com 41 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V076 = OBSColumn(
            id='V076',
            name='Pessoas com 42 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V077 = OBSColumn(
            id='V077',
            name='Pessoas com 43 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V078 = OBSColumn(
            id='V078',
            name='Pessoas com 44 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V079 = OBSColumn(
            id='V079',
            name='Pessoas com 45 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V080 = OBSColumn(
            id='V080',
            name='Pessoas com 46 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V081 = OBSColumn(
            id='V081',
            name='Pessoas com 47 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V082 = OBSColumn(
            id='V082',
            name='Pessoas com 48 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V083 = OBSColumn(
            id='V083',
            name='Pessoas com 49 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V084 = OBSColumn(
            id='V084',
            name='Pessoas com 50 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V085 = OBSColumn(
            id='V085',
            name='Pessoas com 51 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V086 = OBSColumn(
            id='V086',
            name='Pessoas com 52 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V087 = OBSColumn(
            id='V087',
            name='Pessoas com 53 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V088 = OBSColumn(
            id='V088',
            name='Pessoas com 54 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V089 = OBSColumn(
            id='V089',
            name='Pessoas com 55 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V090 = OBSColumn(
            id='V090',
            name='Pessoas com 56 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V091 = OBSColumn(
            id='V091',
            name='Pessoas com 57 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V092 = OBSColumn(
            id='V092',
            name='Pessoas com 58 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V093 = OBSColumn(
            id='V093',
            name='Pessoas com 59 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V094 = OBSColumn(
            id='V094',
            name='Pessoas com 60 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V095 = OBSColumn(
            id='V095',
            name='Pessoas com 61 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V096 = OBSColumn(
            id='V096',
            name='Pessoas com 62 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V097 = OBSColumn(
            id='V097',
            name='Pessoas com 63 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V098 = OBSColumn(
            id='V098',
            name='Pessoas com 64 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V099 = OBSColumn(
            id='V099',
            name='Pessoas com 65 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V100 = OBSColumn(
            id='V100',
            name='Pessoas com 66 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V101 = OBSColumn(
            id='V101',
            name='Pessoas com 67 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V102 = OBSColumn(
            id='V102',
            name='Pessoas com 68 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V103 = OBSColumn(
            id='V103',
            name='Pessoas com 69 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V104 = OBSColumn(
            id='V104',
            name='Pessoas com 70 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V105 = OBSColumn(
            id='V105',
            name='Pessoas com 71 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V106 = OBSColumn(
            id='V106',
            name='Pessoas com 72 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V107 = OBSColumn(
            id='V107',
            name='Pessoas com 73 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V108 = OBSColumn(
            id='V108',
            name='Pessoas com 74 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V109 = OBSColumn(
            id='V109',
            name='Pessoas com 75 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V110 = OBSColumn(
            id='V110',
            name='Pessoas com 76 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V111 = OBSColumn(
            id='V111',
            name='Pessoas com 77 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V112 = OBSColumn(
            id='V112',
            name='Pessoas com 78 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V113 = OBSColumn(
            id='V113',
            name='Pessoas com 79 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V114 = OBSColumn(
            id='V114',
            name='Pessoas com 80 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V115 = OBSColumn(
            id='V115',
            name='Pessoas com 81 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V116 = OBSColumn(
            id='V116',
            name='Pessoas com 82 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V117 = OBSColumn(
            id='V117',
            name='Pessoas com 83 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V118 = OBSColumn(
            id='V118',
            name='Pessoas com 84 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V119 = OBSColumn(
            id='V119',
            name='Pessoas com 85 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V120 = OBSColumn(
            id='V120',
            name='Pessoas com 86 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V121 = OBSColumn(
            id='V121',
            name='Pessoas com 87 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V122 = OBSColumn(
            id='V122',
            name='Pessoas com 88 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V123 = OBSColumn(
            id='V123',
            name='Pessoas com 89 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V124 = OBSColumn(
            id='V124',
            name='Pessoas com 90 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V125 = OBSColumn(
            id='V125',
            name='Pessoas com 91 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V126 = OBSColumn(
            id='V126',
            name='Pessoas com 92 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V127 = OBSColumn(
            id='V127',
            name='Pessoas com 93 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V128 = OBSColumn(
            id='V128',
            name='Pessoas com 94 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V129 = OBSColumn(
            id='V129',
            name='Pessoas com 95 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V130 = OBSColumn(
            id='V130',
            name='Pessoas com 96 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V131 = OBSColumn(
            id='V131',
            name='Pessoas com 97 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V132 = OBSColumn(
            id='V132',
            name='Pessoas com 98 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V133 = OBSColumn(
            id='V133',
            name='Pessoas com 99 anos de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)

        V134 = OBSColumn(
            id='V134',
            name='Pessoas com 100 anos ou mais de idade',
            type='Numeric',
            weight=3,
            aggregate='sum',
            tags=[self.input()['sections']['br'], self.input()['units']['people'], self.input()['subsections']['Age and Gender']],
                            targets={ V001: DENOMINATOR },)
        return OrderedDict([
           ('V001', V001),
           ('V002', V002),
           ('V003', V003),
           ('V004', V004),
           ('V005', V005),
           ('V006', V006),
           ('V007', V007),
           ('V008', V008),
           ('V009', V009),
           ('V010', V010),
           ('V011', V011),
           ('V012', V012),
           ('V013', V013),
           ('V014', V014),
           ('V015', V015),
           ('V016', V016),
           ('V017', V017),
           ('V018', V018),
           ('V019', V019),
           ('V020', V020),
           ('V021', V021),
           ('V022', V022),
           ('V023', V023),
           ('V024', V024),
           ('V025', V025),
           ('V026', V026),
           ('V027', V027),
           ('V028', V028),
           ('V029', V029),
           ('V030', V030),
           ('V031', V031),
           ('V032', V032),
           ('V033', V033),
           ('V034', V034),
           ('V035', V035),
           ('V036', V036),
           ('V037', V037),
           ('V038', V038),
           ('V039', V039),
           ('V040', V040),
           ('V041', V041),
           ('V042', V042),
           ('V043', V043),
           ('V044', V044),
           ('V045', V045),
           ('V046', V046),
           ('V047', V047),
           ('V048', V048),
           ('V049', V049),
           ('V050', V050),
           ('V051', V051),
           ('V052', V052),
           ('V053', V053),
           ('V054', V054),
           ('V055', V055),
           ('V056', V056),
           ('V057', V057),
           ('V058', V058),
           ('V059', V059),
           ('V060', V060),
           ('V061', V061),
           ('V062', V062),
           ('V063', V063),
           ('V064', V064),
           ('V065', V065),
           ('V066', V066),
           ('V067', V067),
           ('V068', V068),
           ('V069', V069),
           ('V070', V070),
           ('V071', V071),
           ('V072', V072),
           ('V073', V073),
           ('V074', V074),
           ('V075', V075),
           ('V076', V076),
           ('V077', V077),
           ('V078', V078),
           ('V079', V079),
           ('V080', V080),
           ('V081', V081),
           ('V082', V082),
           ('V083', V083),
           ('V084', V084),
           ('V085', V085),
           ('V086', V086),
           ('V087', V087),
           ('V088', V088),
           ('V089', V089),
           ('V090', V090),
           ('V091', V091),
           ('V092', V092),
           ('V093', V093),
           ('V094', V094),
           ('V095', V095),
           ('V096', V096),
           ('V097', V097),
           ('V098', V098),
           ('V099', V099),
           ('V100', V100),
           ('V101', V101),
           ('V102', V102),
           ('V103', V103),
           ('V104', V104),
           ('V105', V105),
           ('V106', V106),
           ('V107', V107),
           ('V108', V108),
           ('V109', V109),
           ('V110', V110),
           ('V111', V111),
           ('V112', V112),
           ('V113', V113),
           ('V114', V114),
           ('V115', V115),
           ('V116', V116),
           ('V117', V117),
           ('V118', V118),
           ('V119', V119),
           ('V120', V120),
           ('V121', V121),
           ('V122', V122),
           ('V123', V123),
           ('V124', V124),
           ('V125', V125),
           ('V126', V126),
           ('V127', V127),
           ('V128', V128),
           ('V129', V129),
           ('V130', V130),
           ('V131', V131),
           ('V132', V132),
           ('V133', V133),
           ('V134', V134),
])
