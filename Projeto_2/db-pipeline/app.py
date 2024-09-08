import sqlite3
import os
import pandas as pd
from dotenv import load_dotenv
import assets.utils as utils
from assets.utils import logger
import datetime
import numpy as np

load_dotenv()

def data_clean(df, metadados):
    '''
    Função principal para saneamento dos dados
    INPUT: Pandas DataFrame, dicionário de metadados
    OUTPUT: Pandas DataFrame, base tratada
    '''
    df["data_voo"] = pd.to_datetime(df[['year', 'month', 'day']]) 
    df = utils.null_exclude(df, metadados["cols_chaves"])
    df = utils.convert_data_type(df, metadados["tipos_originais"])
    df = utils.select_rename(df, metadados["cols_originais"], metadados["cols_renamed"])
    df = utils.string_std(df, metadados["std_str"])

    df.loc[:,"datetime_partida"] = df.loc[:,"datetime_partida"].str.replace('.0', '')
    df.loc[:,"datetime_chegada"] = df.loc[:,"datetime_chegada"].str.replace('.0', '')

    for col in metadados["corrige_hr"]:
        lst_col = df.loc[:,col].apply(lambda x: utils.corrige_hora(x))
        df[f'{col}_formatted'] = pd.to_datetime(df.loc[:,'data_voo'].astype(str) + " " + lst_col)
    
    logger.info(f'Saneamento concluído; {datetime.datetime.now()}')
    return df

def feat_eng(df):
    '''
    Função Função para criação de novas colunas "tempo_de_voo_esperado", "tempo_voo_hr" , "atraso", "flg_potencial_erro", "flg_atraso", "flg_adiantado", "dia_semana"
    INPUT: Pandas DataFrame, base de dados tradada
    OUTPUT: Pandas DataFrame, base de dados com as novas colunas
    '''

    tmp = df.copy()
    # Cria coluna "tempo_de_voo_esperado" em horas
    tmp["tempo_de_voo_esperado"] = (tmp["datetime_chegada_formatted"] - tmp["datetime_partida_formatted"]) / pd.Timedelta(hours=1)
    tmp["dia_semana"] = tmp["data_voo"].dt.day_of_week
    tmp["horario"] = tmp.loc[:, "datetime_partida_formatted"].dt.hour.apply(
        lambda h: "MADRUGADA" if 0 <= h < 6 else "MANHA" if 6 <= h < 12 else "TARDE" if 12 <= h < 18 else "NOITE"
    )
    # Cria coluna "tempo_voo_hr" em horas
    tmp["tempo_voo_hr"] = tmp["tempo_voo"] / 60
    # Cria coluna "atraso"
    tmp["atraso"] = tmp["tempo_voo_hr"] - tmp["tempo_de_voo_esperado"]
    tmp["flg_status"] = tmp.loc[:, "atraso"].apply(
        lambda atraso: "ATRASO" if atraso > 0.6 else "ON-TIME"
    )
    logger.info(f'Colunas "tempo_de_voo_esperado", "dia_semana", "horario", "tempo_voo_hr", "atraso", "flg_status", criadas com sucesso. ; {datetime.datetime.now()}')
    return tmp

def save_data_sqlite(df):
    '''
    Função para salvar os dados no Database NyflightsDB.db
    INPUT: Pandas DataFrame, base de dados tradada
    OUTPUT: Dados salvos no banco 
    '''
    try:
        conn = sqlite3.connect("data/NyflightsDB.db")
        logger.info(f'Conexão com banco estabelecida ; {datetime.datetime.now()}')
    except:
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()}')
    c = conn.cursor()
    df.to_sql('nyflights', con=conn, if_exists='replace')
    conn.commit()
    logger.info(f'Dados salvos com sucesso; {datetime.datetime.now()}')
    conn.close()

def fetch_sqlite_data(table):
    '''
    Função para conectar ao Database NyflightsDB.db e exibir as cinco primeiras linhas 
    INPUT: Pandas DataFrame, base de dados tradada
    OUTPUT: Mensagem com as cinco primeiras linhas da consulta
    '''
    try:
        conn = sqlite3.connect("data/NyflightsDB.db")
        logger.info(f'Conexão com banco estabelecida ; {datetime.datetime.now()}')
    except:
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()}')
    c = conn.cursor()
    c.execute(f"SELECT * FROM {table} LIMIT 5")
    print(c.fetchall())
    conn.commit()
    conn.close()


if __name__ == "__main__":
    logger.info(f'Inicio da execução ; {datetime.datetime.now()}')
    metadados  = utils.read_metadado(os.getenv('META_PATH'))
    df = pd.read_csv(os.getenv('DATA_PATH'),index_col=0)
    df = data_clean(df, metadados)
    print(df.head())
    utils.null_check(df, metadados["null_tolerance"])
    utils.keys_check(df, metadados["cols_chaves"])
    df = feat_eng(df)
    #save_data_sqlite(df)
    fetch_sqlite_data(metadados["tabela"][0])
    logger.info(f'Fim da execução ; {datetime.datetime.now()}')