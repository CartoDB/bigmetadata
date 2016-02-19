from tasks.meta import BMDTag, BMD


class Tags(BMD):
    denominator = BMDTag(id='denominator', name='denominator')
    population = BMDTag(id='population', name='population')

tags = Tags()
