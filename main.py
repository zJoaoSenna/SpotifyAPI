import requests
import pandas as pd
import datetime as dt
from datetime import date
import re
import io
from google.cloud import storage

def featurestore_cambio(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    def date_utc_to_date_brazil_east(datestr: str) -> str:
        dtfmt = '%Y-%m-%d %H:%M:%S.%f'
        dtutc = dt.datetime.strptime(datestr, dtfmt)   
        return dtutc.strftime('%d-%m-%Y')


    year = str(date.today().year)

    url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@moeda='EUR'&@dataInicial='01-01-"+year+"'" +"&@dataFinalCotacao='12-31-"+ year+"'"+"&$format=text/csv&$select=paridadeCompra,paridadeVenda,cotacaoCompra,cotacaoVenda,dataHoraCotacao,tipoBoletim"

    payload={}
    headers = {
    'Cookie': 'dtCookie=2FFEF66B3F660488D59A687751248155|cHRheHwx; BIGipServer~was-p_as3~was-p~pool_was-p_default_443=1020268972.47873.0000; JSESSIONID=0000E4TMoAjPS32kEA-6NF0AVla:1cn7m3fq4'
    }

    urlData = requests.get(url).content
    rawData = pd.read_csv(io.StringIO(urlData.decode('utf-8')))
    rawData['dataHoraCotacao'] = rawData['dataHoraCotacao'][:-3].apply(date_utc_to_date_brazil_east)
    rawData['paridadeCompra'] = rawData['paridadeCompra'].str.replace(',','.').astype('float')
    rawData['paridadeVenda'] = rawData['paridadeVenda'].str.replace(',','.').astype('float')
    rawData['cotacaoCompra_euro'] = rawData['cotacaoCompra'].str.replace(',','.').astype('float')
    rawData['cotacaoVenda_euro'] = rawData['cotacaoVenda'].str.replace(',','.').astype('float')
    rawData['cotacaoVenda_dolar'] = rawData['cotacaoVenda_euro']/rawData['paridadeVenda']
    rawData['cotacaoCompra_dolar'] = rawData['cotacaoCompra_euro']/rawData['paridadeCompra']
    rawData.drop(['paridadeCompra', 'paridadeVenda','cotacaoCompra','cotacaoVenda'], axis='columns', inplace=True)
    rawData.drop(rawData.loc[rawData['tipoBoletim']!='Fechamento'].index, inplace=True)

    
    df = rawData.to_csv(sep=";", index=False, encoding="UTF-8",header=True)
    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('bronze-fine-blueprint-333917')
    blob = bucket.blob('historico_cambio_dolar_euro/'+'historico_cambio'+'_'+year+'.csv')
    
    blob.upload_from_string(data=df)
    
    #print(response.text)
    return '200'
     
