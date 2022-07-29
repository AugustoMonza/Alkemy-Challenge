from operator import index
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text
from decouple import config
import psycopg2
import logging

def connection():
    user_name = config('USER_NAME')
    password = config('PASSWORD')
    server_ = config('PORT')
    database = config('DB_NAME')

    engine = create_engine(f"postgresql://{user_name}:{password}@localhost:{server_}/{database}")
    logging.info('Conexion creada')
    return engine

def normalize():
    now = datetime.now()
    #Lectura de archivos.csv descargados
    month_year = now.strftime('%Y-%B')
    date_file = f"{now.strftime('%d-%m-%Y')}.csv"

    museo_df = pd.read_csv(f"Museos/{month_year}/Museos-{date_file}")
    cine_df = pd.read_csv(f"Cines/{month_year}/Cines-{date_file}")
    biblioteca_df = pd.read_csv(f"Bibliotecas/{month_year}/Bibliotecas-{date_file}")
    logging.info('tablas normalizadas')
    return museo_df, cine_df, biblioteca_df

def name(museo_df, cine_df, biblioteca_df):
    
    #Recupero información
    museo_norm = museo_df.loc[:,['Cod_Loc', 'IdProvincia', 'IdDepartamento', 'categoria', 'provincia', 'localidad', 'nombre', 'direccion', 'CP','telefono', 'Mail', 'Web','fuente']]
    cine_norm = cine_df.loc[:,['Cod_Loc','IdProvincia', 'IdDepartamento', 'Categoría', 'Provincia', 'Localidad', 'Nombre', 'Dirección', 'CP', 'Teléfono', 'Mail', 'Web', 'Fuente']]
    biblioteca_norm = biblioteca_df.loc[:,['Cod_Loc','IdProvincia','IdDepartamento', 'Categoría', 'Provincia', 'Localidad', 'Nombre', 'Domicilio', 'CP', 'Teléfono', 'Mail', 'Web','Fuente']]
    cine_table = cine_df.loc[:,['Provincia','Pantallas','Butacas','espacio_INCAA']]
    museo_norm.rename(columns={'Cod_Loc':'cod_localidad',
                                'IdProvincia': 'id_provincia',
                                'IdDepartamento':'id_departamento',
                                'categoria': 'categoría',
                                'provincia': 'provincia',
                                'direccion': 'domicilio',
                                'CP': 'código postal',
                                'telefono':'número de teléfono',
                                'Mail':'mail',
                                'Web': 'web'},
                        inplace=True)
    cine_norm.rename(columns={'Cod_Loc':'cod_localidad',
                                'IdProvincia': 'id_provincia',
                                'IdDepartamento':'id_departamento',
                                'Categoría':'categoría',
                                'Provincia':'provincia',
                                'Localidad':'localidad',
                                'Nombre':'nombre',
                                'Dirección':'domicilio',
                                'CP':'código postal',
                                'Teléfono':'número de teléfono',
                                'Mail':'mail',
                                'Web':'web',
                                'Fuente':'fuente'
                                },
                        inplace=True)

    biblioteca_norm.rename(columns={'Cod_Loc':'cod_localidad',
                                        'IdProvincia': 'id_provincia',
                                        'IdDepartamento':'id_departamento',
                                        'Categoría':'categoría',
                                        'Provincia':'provincia',
                                        'Localidad':'localidad',
                                        'Nombre':'nombre',
                                        'Domicilio':'domicilio',
                                        'CP': 'código postal',
                                        'Teléfono':'número de teléfono',
                                        'Mail':'mail',
                                        'Web':'web',
                                        'Fuente':'fuente'
                                        },
                            inplace=True)
    return museo_norm, cine_norm, biblioteca_norm, cine_table

def tables(museo_norm, cine_norm, biblioteca_norm, cine_table):
    master_table = pd.concat([museo_norm, cine_norm, biblioteca_norm])
    master_table = master_table.replace('s/d', np.nan)
    
    #Creo las tablas.
    register_by_category = master_table.groupby(['categoría']).size().reset_index(name='Cantidad por categoría')
    register_by_category_and_province = master_table.groupby(['categoría', 'provincia']).size().reset_index(name='Registro por Provincia y Categoría')
    register_by_fount = master_table.groupby(['fuente']).size().reset_index(name='Registro total por fuente')
    master_table = master_table.drop('fuente', axis=1)
    cine_info =cine_table.groupby(['Provincia']).agg({'Pantallas':'sum',
                                                                'Butacas':'sum',
                                                                'espacio_INCAA':'count'
                                                                })   
    return master_table, cine_info, register_by_category, register_by_category_and_province, register_by_fount

#Se envía las tablas a la Base de Datos.
def to_sql(master_table, cine_info, register_by_category, register_by_category_and_province, register_by_fount, engine):
    master_table.to_sql('Tabla Maestra',engine, if_exists='replace', index=False)
    cine_info.to_sql('Cine',engine, if_exists='replace', index=False)
    register_by_category.to_sql('Categoría',engine, if_exists='replace', index=False)
    register_by_category_and_province.to_sql('Categoría y Provincia',engine, if_exists='replace')
    register_by_fount.to_sql('Fuente',engine, if_exists='replace', index=False)
    logging.info('Tablas enviadas a la Base de datos')

def main():
    engine = connection()
    museo_df, cine_df, biblioteca_df = normalize()
    museo_norm, cine_norm, biblioteca_norm, cine_table = name(museo_df, cine_df, biblioteca_df)
    master_table, cine_info, register_by_category, register_by_category_and_province, register_by_fount = tables(museo_norm, cine_norm, biblioteca_norm, cine_table)
    to_sql(master_table, cine_info, register_by_category, register_by_category_and_province, register_by_fount, engine)
    
if __name__ == '__main__':
    main()
    
    