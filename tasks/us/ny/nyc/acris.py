from tasks.socrata import LoadSocrataCSV

from luigi import Task, Parameter


class Deeds(Task):

    def requires(self):
        yield LoadSocrataCSV(domain='data.cityofnewyork.us', dataset='636b-3b5g') # real parties
        yield LoadSocrataCSV(domain='data.cityofnewyork.us', dataset='8h5j-fqxa') # real legals
        yield LoadSocrataCSV(domain='data.cityofnewyork.us', dataset='bnx9-e6tj') # real master
        yield LoadSocrataCSV(domain='data.cityofnewyork.us', dataset='7isb-wh4c') # control codes
        #yield PLUTO(year=2014)

