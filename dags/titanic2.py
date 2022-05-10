'''
1 - ler a tabela única de indicadores criados na DAG 1 (ler /tmp/tabela_unica.csv)
2 - produzir médias para cada indicador considerando o total.
3 - printa a tabela nos logs
4 - Escrever o resultado em um arquivo csv local no container
'''

# Imports Libs:
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from datetime import datetime
from numpy import dtype
import pandas as pd
import os
from pathlib import Path


default_args = {
    'owner': 'Willian',
    'start_date': datetime(2022, 5, 10)
}


@dag(default_args=default_args, schedule_interval="@once", \
  description="Atividade Airflow", catchup=False, tags=['taskApi'])
def exercicio2():
    """
    """

    @task
    def start():
        print("Começou") 
    
 
    @task
    def mean():
        '''
        '''   
        df = pd.read_csv('./temp/tabela_unica.csv', sep=',')
        list(df.columns.values)
        df_mean = df.mean(numeric_only=True)
        print(df)
        df_mean.to_csv(r'mean.csv', header=True)
  


    @task
    def end():        
        print()

    st = start()
    ed = end()

    m = mean()

    st >> m >> ed

execucao = exercicio2()