
class DownloadData(Task):

    # TGS00036
    dataset = Parameter()
    from_date = Parameter()  # 2007-01-01
    to_date = Parameter()  # 2015-01-01

    URL = 'http://econdb.com/api/series/?dataset={dataset}&format=carto&from_date={from_date}&to_date={to_date}'

    def run(self):
        pass

    def output(self):
        pass
