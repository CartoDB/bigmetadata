import sys
import json
import requests

BASEMAP = {
    "type": "http",
    "options": {
        "urlTemplate": "https://{s}.maps.nlp.nokia.com/maptile/2.1/maptile/newest/satellite.day/{z}/{x}/{y}/256/jpg?lg=eng&token=A7tBPacePg9Mj_zghvKt9Q&app_id=KuYppsdXZznpffJsKT24",
        "subdomains": "1234",
        #"urlTemplate": "http://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}.png",
        #"subdomains": ["a", "b", "c"]
    }
}


def viz_to_config(viz_json_url):
    resp = requests.get(viz_json_url)

    assert resp.status_code == 200
    data = resp.json()
    layers = []
    layers.append(BASEMAP)

    for data_layer in data['layers']:
        if data_layer['type'] == 'layergroup':
            for layer in data_layer['options']['layer_definition']['layers']:
                if layer['visible'] is True:
                    layers.append({'type': 'mapnik', 'options': layer['options']})

    return {
        'layers': layers,
        'center': json.loads(data['center']),
        'bounds': data['bounds'],
        'zoom': data['zoom']
    }


def getNamedMap(username, map_config):

    config = {
        "version": "1.3.0",
        "layers": map_config
    }
    r = requests.get('https://'+username+'.cartodb.com/api/v1/map',
                     headers={'content-type':'application/json'},
                     params={'config': json.dumps(config)}
                    )
    return r.json()

#username='andrew'
#vj = 'https://team.cartodb.com/u/andrew/api/v2/viz/21891d8a-faad-11e5-acf8-0e5db1731f59/viz.json'
username = 'observatory'
#viz_json_url = 'https://observatory.cartodb.com/api/v2/viz/5a2f4cc8-e189-11e5-8327-0e5db1731f59/viz.json'
viz_json_url = 'https://observatory.cartodb.com/api/v2/viz/57d9408e-0351-11e6-9c12-0e787de82d45/viz.json'

config = viz_to_config(viz_json_url)
namedmap = getNamedMap(username, config['layers'])

print 'https://{username}.cartodb.com/api/v1/map/static/center/' \
      '{layergroupid}/{zoom}/{center_lon}/{center_lat}/800/500.png'.format(
          username=username,
          layergroupid=namedmap['layergroupid'],
          zoom=config['zoom'],
          center_lon=config['center'][0],
          center_lat=config['center'][1]
      )
#print 'https://' + username+'.cartodb.com/api/v1/map/static/center/'+namedmap['layergroupid']+'/'+str(zoom)+'/'+str(center[0])+'/'+str(center[1])+'/800/500.png'
