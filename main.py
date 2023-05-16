import requests 
import pandas as pd
from google.oauth2 import service_account
import io
import csv
import base64

def do_req(pg, q, slug, lista_df):
    req = requests.get(f"https://www.catho.com.br/vagas/_next/data/kV_SWimkUFCXPK-QrRFx5/{slug}.json?q={q}&slug={slug}&page={pg}")

    reqjson = req.json()
    df = pd.DataFrame(reqjson['pageProps']['jobSearch']['jobSearchResult']['data']['jobs'])

    df = pd.json_normalize(df['job_customized_data']) #normalizado o json/dicionario que começa com {

    #cria novo dataframe
    df_vagas = pd.json_normalize(df['vagas'].explode().tolist()) #normalizando o dicionario que começa com [

    #concatenar
    df = pd.concat([df.drop(columns=['vagas']), df_vagas], axis=1)

    lista_df.append(df)

    # retorno
    if int(reqjson['pageProps']['pageState']['props']['page']) <= int(reqjson['pageProps']['pageState']['props']['totalPages']):
        return True
    else:
        return False
        
def start_main(request):
    if __name__ == '__main__':
        q = "engenheiro de dados pleno"
    metodo = "bigquery"
    q = request.args.get("q")
    metodo = request.args.get("metodo", default="json")

    if q == None or q == "": return {'data': 'Sem retorno'}

    lst_metodos_permitidos = ['json', 'csv', 'bigquery']
    if not metodo in lst_metodos_permitidos: metodo = "json"

    slug = q.replace(" ", "-")

    status = True
    pg = 1
    lista_df = []

    while status:
        status = do_req(pg, q, slug, lista_df)
        if status: pg += 1

    if len(lista_df) == 0: return {"data": "Sem resposta"}

    #empilhar a lista em unico dataframe
    if len(lista_df) > 1:
        df_final = pd.concat(lista_df)
    else:
        df_final = lista_df[0]

    df_final = df_final.reset_index(drop=True)

    #exemplos, drop de linha nula
    df_final.dropna(subset=['id'], inplace=True)

    #exemplos renomeando as colunas
    df_final = df_final.rename(columns=lambda x: x.replace('.', '_'))

    #exemplos de int e float
    df_final['id'] = df_final['id'].astype(int)
    df_final['salario'] = df_final['salario'].astype(float)

    #exemplos remocao de colunas
    df_final.drop(columns=['grupoMidia', 'benef', 'habilidades', 'ppdFiltro', 'salarioACombinar', 'hrenova', 'pja', 'origemAgregador', 'ppdInfo_instAdapt', 'anunciante_confidencial', 'contratante_confidencial'], inplace=True)

    #verificacao do metodo
    if metodo == "json":
        json_data = df_final.to_dict(orient='records')
        return {"data": json_data}
    
    elif metodo == "bigquery":
        chave = service_account.Credentials.from_service_account_file('web-scraping-catho-cd3b91f04ac8.json')
        df_final.to_gbq("raspagem.crawler", project_id='web-scraping-catho', if_exists="replace", credentials=chave) 