import json


with open("E:/Desktop/grid_dy_conv_20200326_utf8.json",'r',encoding='UTF-8') as f:
    geojson_dy = json.load(f)

geojson_dy[0]['name']