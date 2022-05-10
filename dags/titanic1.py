'''
1 - ler os dados e escrever localmente dentro do container numa pasta /tmp
2 - processar os seguintes indicadores
2.a - quantidade de passageiros por sexo e classe (produzir e escrever)
2.b - preco médio da tarifa pago por sexo e classe (produzir e escrever)
2.c - a proporção (entre 0 e 1) de sobreviventes por sexo e classe (produzir e 
escrever)
2.d - a quantidade total de SibSp + Parch (tudo junto) por sexo e classe 
(produzir e escrever)
3 - juntar todos os indicadores criados em um único dataset (produzir e 
escrever) /tmp/tabela_unica.csv
4 - Printa a tabela nos logs
5 - Triggar a DAG 2
'''

# Imports Libs:
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import pandas as pd
import os
from pathlib import Path


def create_df_sex__class_variable(name_variable, df):
  '''
  '''
  df_results = pd.DataFrame(columns=['Sex', 'Class', name_variable])
  for n in range(0, 6):
    if n <= 2:
      df_results = df_results.append({'Sex' : 'Female', \
                                    'Class' : n + 1, \
                                    name_variable : df[n]}, \
                                      ignore_index=True)
    else:
      df_results = df_results.append({'Sex' : 'Male', \
                                    'Class' : n - 2, \
                                    name_variable : df[n]}, \
                                      ignore_index=True)
  return df_results


def create_path():
  '''
  '''
  path_source = 'https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv'
  path = Path('PRE/astronomerairflow/temp')
  name_file = 'titanic.csv'
  path.mkdir(parents=True, exist_ok=True)
  path_destiny =  os.path.join(path, name_file)
  return path_source, path, name_file, path_destiny  


default_args = {
    'owner': 'Willian',
    'start_date': datetime(2022, 5, 9)
}

@dag(default_args=default_args, schedule_interval="@once", \
  description="Atividade Airflow", catchup=False, tags=['taskApi'])
def exercicio():
    """
    """
    path_source, path, name_file, path_destiny = create_path()

    @task
    def start():
      print("Começou") 
    

    #1 -
    @task
    def read_and_write():
      '''
      '''
      df = pd.read_csv(path_source, sep=';')
      print(path_destiny)
      df.to_csv(path / name_file ,  index = None, header=True)
    
    #2a -
    @task
    def nunique():
      '''
      '''
      df = pd.read_csv(path_destiny, sep=',')
      df_nunique = df.groupby(['Sex', 'Pclass'])['PassengerId'].nunique()
      df_results_nunique = create_df_sex__class_variable('CountPassenger',\
        df_nunique)
      df_results_nunique.to_csv(r'./temp/nunique.csv',  \
        index = None, header=True)



    #2b -
    @task
    def mean():
      '''
      '''
      df = pd.read_csv(path_destiny , sep=',')
      df_mean = df.groupby(['Sex', 'Pclass'])['Fare'].mean().round(2)
      df_results_mean = create_df_sex__class_variable('FareMean', df_mean)
      df_results_mean.to_csv(r'./temp/mean.csv',  index = None, \
        header=True)
  

    #2c - 
    @task
    def sum_sibsp_parch():
      '''
      '''
      df = pd.read_csv(path_destiny , sep=',')
      df_sibsp = df.groupby(['Sex', 'Pclass'])['SibSp'].sum()
      df_parch = df.groupby(['Sex', 'Pclass'])['Parch'].sum()
      df_sum = df_sibsp + df_parch
      df_results_sum = create_df_sex__class_variable('SumSibspParch', df_sum)
      df_results_sum.to_csv(r'./temp/sum.csv',  index = None, header=True)
      

    #2d - 
    @task
    def propotion_survived():
      '''
      '''
      df = pd.read_csv(path_destiny , sep=',')
      df_count_status = df.groupby(['Sex', 'Pclass'])['Survived'].count().round(0)
      df_count_survived = df.loc[df['Survived'] == 1].groupby(['Sex', \
        'Pclass', 'Survived'])['Survived'].count()
      df_propotion_Survived = (df_count_survived) / (df_count_status)
      df_propotion_Survived = df_propotion_Survived.astype(float)
      df_propotion_Survived = round(df_propotion_Survived, 2)
      df_propotion_Survived = create_df_sex__class_variable('PropotionSurvived',\
        df_propotion_Survived)
      df_propotion_Survived.to_csv(r'./temp/propotion.csv',\
        index = None, header=True)


    #3 -  
    @task
    def merge_df():
      '''
      '''
      df_nunique = pd.read_csv('./temp/nunique.csv', sep=',')
      df_mean = pd.read_csv('./temp/mean.csv', sep=',')
      df_sum = pd.read_csv('./temp/sum.csv', sep=',')
      df_propotion = pd.read_csv('./temp/propotion.csv', sep=',')
      df_merge = df_nunique.merge(df_mean).merge(df_sum).merge(df_propotion)
      df_results_merge = df_merge.iloc[:,2:]      
      df_results_merge.to_csv(r'./temp/tabela_unica.csv',\
        index = None, header=True)
      print(df_results_merge)


    ed =  TriggerDagRunOperator(
        task_id="trigga_dag_exercicio2",
        trigger_dag_id="exercicio2"
    )

    st = start()

    r = read_and_write()
    n = nunique()
    m = mean()
    s = sum_sibsp_parch()
    p = propotion_survived()
    me = merge_df()

    st >> r  >> [n, m, s, p] >> me >> ed

execucao = exercicio()

