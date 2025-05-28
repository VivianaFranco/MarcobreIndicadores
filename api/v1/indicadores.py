from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from models.ActualPlanta import ActualPlanta
from models.Provisiones import Provisiones
from database import db
from typing import List
import pandas as pd
from datetime import datetime, date
import calendar
import numpy as np
import redis
import pickle
import io
import time
import os
from dotenv import load_dotenv
import time,datetime
import pytz
from typing import Optional

load_dotenv()
router = APIRouter()


#Fechas
def get_current_datetime():
    peru_tz = pytz.timezone("America/Lima")
    current_date = datetime.datetime.now(peru_tz)
    current_day = current_date.weekday()
    current_week = current_date.isocalendar()[1]
    current_month = current_date.month
    current_year = current_date.year

    last_date = current_date - datetime.timedelta(days=7)
    last_day = last_date.weekday()
    last_week = last_date.isocalendar()[1]
    last_month = last_date.month
    last_year = last_date.year

    if current_day == 0:
        Semana = last_week
        Anho = last_year
    else:
        Semana = current_week #Para correr el semanal
        Anho = current_year
    return current_date, Semana, Anho
    

#Funciones
        
async def id_to_string_process(cursor, array):
    async for item in cursor:
        item['_id'] = str(item['_id']) 
        array.append(item)
    return array

def function_return_Streaming(df, result_list):
    def generate():
        buffer = io.StringIO()
        buffer.write('[')
        first = True
        
        chunk_size = 1000
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            if not first:
                buffer.write(',')
            first = False
            chunk_json = chunk.to_json(orient='records')
            buffer.write(chunk_json[1:-1])
            yield buffer.getvalue()
            buffer.truncate(0)
            buffer.seek(0)
        buffer.write(']')
        yield buffer.getvalue()
    
    result_list.append(generate())


REDISHOST = os.getenv("REDISHOST")
REDISPORT = os.getenv("REDISPORT")
REDISUSER = os.getenv("REDISUSER")
REDISPASSWORD = os.getenv("REDISPASSWORD")


#Conectandose al servidor de redis, que entiendo esta en mi conteder de dockers
RedisDockers = redis.Redis(host=REDISHOST, port=REDISPORT,username=REDISUSER,password=REDISPASSWORD, db=0)

@router.post("/asdasda",tags=["Indicadores"])
async def asdasd():
    RedisDockers.set('Process_Status_Data_SAP_Indicadores','completed')
    return ({"Message": "Oki Doki"})
    

#Proceso de Carga a Redis
@router.post("/UpdateDataSAPToRedis", tags=["Indicadores"])
async def Update_Data_SAPIndicadores_To_Redis():
    
    All_Data_IW29 = []
    All_Data_IW37nBase = []
    All_Data_IW37nReporte = []  
    All_Data_IW39 = []  
    
    Process_Status_Data_SAP_Indicadores = RedisDockers.get('Process_Status_Data_SAP_Indicadores')
    print(Process_Status_Data_SAP_Indicadores)
    
    if Process_Status_Data_SAP_Indicadores is None or Process_Status_Data_SAP_Indicadores.decode('utf-8') != 'in progess':
        
        RedisDockers.set('Process_Status_Data_SAP_Indicadores','in progess')
        print("Iniciando Carga de datos de SAP a Redis")
        
        print("Obteniendo los datos de MongoDB")
        CursorIW29 = db.iw29.find({"Anho": "2025"})
        cursorIW37nBase = db.iw37n.find({"Anho": "2025"})
        CursorIW37nReporte = db.iw37nreport.find({"Anho": "2025"})
        CursorIW39 = db.iw39report.find({"Anho": "2025"})
        
        print("Procesando los datos de MongoDB")
        await id_to_string_process(CursorIW29,All_Data_IW29)
        await id_to_string_process(cursorIW37nBase,All_Data_IW37nBase)
        await id_to_string_process(CursorIW37nReporte,All_Data_IW37nReporte)
        await id_to_string_process(CursorIW39,All_Data_IW39)
        
        print("Creando los data frame")
        df_IW29 = pd.DataFrame(All_Data_IW29)
        df_IW37nBase = pd.DataFrame(All_Data_IW37nBase)
        df_IW37nReporte = pd.DataFrame(All_Data_IW37nReporte)
        df_IW39 = pd.DataFrame(All_Data_IW39)        
        
        print("Creando las tablas en Redis")
        RedisDockers.set('df_IW29',pickle.dumps(df_IW29))
        RedisDockers.set('df_IW37nBase',pickle.dumps(df_IW37nBase))
        RedisDockers.set('df_IW37nReporte',pickle.dumps(df_IW37nReporte))
        RedisDockers.set('df_IW39',pickle.dumps(df_IW39))
        RedisDockers.set('Process_Status_Data_SAP_Indicadores','completed')
        print("Finalizando el proceso de carga de Data SAP Indicadores en Redis")
        
        return ({
            "Message": "Oki Doki"
            })


@router.get('/GetDataSAPIw39FromRedis', tags=["Indicadores"])
def Get_Data_IW39_From_Redis():
    
    print("Obteniendo datos de SAP IW39 desde redis")
    df_result = []
    pickled_IW39 =RedisDockers.get('df_IW39')
    df_IW39 = pickle.loads(pickled_IW39)
    function_return_Streaming(df_IW39,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención de IW39 desde Redis")
    return StreamingResponse(generate(), media_type='application/json')


@router.get('/GetDataSAPIw37nBaseFromRedis', tags=["Indicadores"])
def Get_Data_IW37nBase_From_Redis():
    
    print("Obteniendo datos de SAP IW37nBase desde redis")
    df_result = []
    pickled_IW37nBase =RedisDockers.get('df_IW37nBase')
    df_IW37Base = pickle.loads(pickled_IW37nBase)
    function_return_Streaming(df_IW37Base,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención de IW37nBase desde Redis")
    return StreamingResponse(generate(), media_type='application/json')


@router.get('/GetDataSAPIw37nReporteFromRedis', tags=["Indicadores"])
def Get_Data_IW37nReporte_From_Redis():
    
    print("Obteniendo datos de SAP IW37nReporte desde redis")
    df_result = []
    pickled_IW37nReporte =RedisDockers.get('df_IW37nReporte')
    df_IW37Reporte = pickle.loads(pickled_IW37nReporte)
    function_return_Streaming(df_IW37Reporte,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención de IW37nReporte desde Redis")
    return StreamingResponse(generate(), media_type='application/json')


@router.get('/GetDataSAPIw29FromRedis', tags=["Indicadores"])
def Get_Data_IW29_From_Redis():
    
    print("Obteniendo datos de SAP IW29 desde redis")
    df_result = []
    pickled_IW29 =RedisDockers.get('df_IW29')
    df_IW29 = pickle.loads(pickled_IW29)
    function_return_Streaming(df_IW29,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención de IW29 desde Redis")
    return StreamingResponse(generate(), media_type='application/json')




#-----------------------------------------------------------------------

async def Process_IW39 (type: Optional[str]=None):    
    
    All_Data_IW39 = []
    df_result = []
    current_date, Semana, Anho = get_current_datetime()
    
    print("Fecha actual:", current_date)
    print("Zona horaria:", current_date.tzinfo)
    print("Semana: ",Semana,"Anho: ",Anho)
    print("Obteniendo datos de MongoDB IW39")
    
    if type is None:
        print("Obteniendo semanal")
        CursorIW39 = db.iw39report.find({
            "Semana": str(Semana),
            "Anho": str(Anho)
        })
    else:
        print("Obteniendo anual")
        CursorIW39 = db.iw39report.find({
            "Anho": str(Anho) #{"$exists": False}
        })
    
    print("Procesando los datos de MongoDB IW39")
    await id_to_string_process(CursorIW39,All_Data_IW39)
    df_IW39 = pd.DataFrame(All_Data_IW39)

    print(df_IW39.columns)    
    print("Creando el data frame IW39")
    df_IW39["Orden-Semana"] = df_IW39["Orden"].astype(str) + "-" + df_IW39["Semana"].astype(str)
    df_IW39 = df_IW39[[
        "Orden",
        "Status del sistema",
        "Semana",
        "StatUsu",
        "P",
        "Ubicación técnica",
        "CpoClasif",
        "Texto breve",
        "PtoTrbRes",
        "Orden-Semana",
        ]]
    return df_IW39

async def Process_IW37nReporte (type: Optional[str]=None):
    All_Data_IW37nReporte = []
    df_result = []  
    print("Obteniendo datos de MongoDB IW37nReporte")
    print("Tipo: ", type)
    current_date, Semana, Anho = get_current_datetime()
    print("semana",Semana,"Anho",Anho)

    if type is None:
        print("Obteniendo semanal")
        CursorIW37nReporte = db.iw37nreport.find({
            "Semana": str(Semana),
            "Anho": str(Anho)
        })
   
    else:
        print("Obteniendo anual")
        CursorIW37nReporte = db.iw37nreport.find({
            "Anho": str(Anho),   #"Anho": {"$exists": False}
        })
    
    print("Procesando los datos de MongoDB IW37nReporte")
    await id_to_string_process(CursorIW37nReporte,All_Data_IW37nReporte)
    df_IW37nReporte = pd.DataFrame(All_Data_IW37nReporte)
    print(df_IW37nReporte.columns)  
    print("Creando el data frame IW37nReporte")
    print(df_IW37nReporte.columns)
    df_IW37nReporte["Orden-Semana"] = df_IW37nReporte["Orden"].astype(str) + "-" + df_IW37nReporte["Semana"].astype(str)
    df_IW37nReporte["Inic.extr."] = pd.to_datetime(df_IW37nReporte["Inic.extr."].str.replace(".", "/"), format="%d/%m/%Y").dt.date
    df_IW37nReporte['Inic.extr.'] = df_IW37nReporte['Inic.extr.'].apply(lambda x: pd.to_datetime(x, unit='ms') if isinstance(x, (int, float)) else pd.to_datetime(x)).dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    df_IW37nReporte = df_IW37nReporte[[
        "Orden",
        "Inic.extr.",
        "Stat.sist.",
        "Revisión",
        "Trbjo real",
        "StatUsu",
        "Semana",
        "Orden-Semana",
        "Ubic.técn.",
        "PtoTrbRes",
        " Trabajo",
        "Op.",
        "CpoClasif",
        "Texto breve",
        "P"
        ]]
    
    print(df_IW37nReporte.columns)
    return df_IW37nReporte

async def Process_IW37nBase (type: Optional[str]=None):
    All_Data_IW37nBase = []
    df_result = []  
    print("Obteniendo datos de MongoDB IW37nBase")
    current_date, Semana, Anho = get_current_datetime()
    if type is None:
        print("Obteniendo semanal")
        CursorIW37nBase = db.iw37n.find({
            "Semana": str(Semana),
            "Anho": str(Anho)
        })
    else:
        print("Obteniendo anual")
        CursorIW37nBase = db.iw37n.find({
            "Anho": str(Anho),
        })
    
    print("Procesando los datos de MongoDB IW37nBase")
    await id_to_string_process(CursorIW37nBase,All_Data_IW37nBase)
    df_IW37nBase = pd.DataFrame(All_Data_IW37nBase)
        
    print("Creando los data frame IW37nBase")
    df_IW37nBase["Orden-Semana"] = df_IW37nBase["Orden"].astype(str) + "-" + df_IW37nBase["Semana"].astype(str)
    df_IW37nBase["Inic.extr."] = pd.to_datetime(df_IW37nBase["Inic.extr."].str.replace(".", "/"), format="%d/%m/%Y").dt.date
    df_IW37nBase['Inic.extr.'] = df_IW37nBase['Inic.extr.'].apply(lambda x: pd.to_datetime(x, unit='ms') if isinstance(x, (int, float)) else pd.to_datetime(x)).dt.strftime('%Y-%m-%dT%H:%M:%S')
    df_IW37nBase = df_IW37nBase[
        [
        'Orden',
        'Aviso',
        'Texto breve',
        'Op.',
        'Texto breve operación',
        'PtoTbjoOp',
        'Cl.',
        'Ubic.técn.',
        'Denomin.',
        'Autor',
        'Inic.extr.',
        'Fe.entrada',
        'CpoClasif',
        'P',
        'PtoTrbRes',
        'PstoTbjo',
        'StatSistOp',
        'Stat.sist.',
        'Revisión',
        'Trbjo real',
        ' Trabajo',
        'Inic.real',
        'StatUsu',
        'Semana',
        'Orden-Semana']
        ]
    return df_IW37nBase

async def Process_Condiciones ():
    All_Data_Condiciones = []
    print("Obteniendo datos de MongoDB Condiciones")
    CursorIndicadores = db.baseindicadores.find({})
    
    print("Procesando los datos de MongoDB Condiciones")
    await id_to_string_process(CursorIndicadores,All_Data_Condiciones)
    df_Indicadores = pd.DataFrame(All_Data_Condiciones)
    
    return df_Indicadores

async def Process_IW37nBase_2 (type: Optional[str]=None):
    
    if type is None:
        print("Obteniendo semanal")
        df_IW37nBase = await Process_IW37nBase()
        Result_IW37nReporte = await Process_IW37nReporte()
        Result_Condiciones = await Process_Condiciones()
        Result_IW39 = await Process_IW39 ()
    else:
        print("Obteniendo anual")
        df_IW37nBase = await Process_IW37nBase(type="Total")
        Result_IW37nReporte = await Process_IW37nReporte(type="Total")
        Result_Condiciones = await Process_Condiciones()
        Result_IW39 = await Process_IW39 (type="Total")
    
    
    Result_IW39 = Result_IW39[["Status del sistema","Orden-Semana"]]
    df_IW37nBase = pd.merge(df_IW37nBase, Result_IW39, on='Orden-Semana',how='left')
    df_IW37nBase.rename(columns={'Status del sistema':'Status Sistema Reportado'}, inplace=True)
    
    Result_IW37nReporte = Result_IW37nReporte[["Trbjo real","StatUsu","Orden-Semana"]]
    df_IW37nBase = pd.merge(df_IW37nBase, Result_IW37nReporte, on='Orden-Semana',how='left')
    df_IW37nBase.rename(columns={'Trbjo real_x':'Trbjo real'}, inplace=True)
    df_IW37nBase.rename(columns={'StatUsu_x':'StatUsu'}, inplace=True)
    df_IW37nBase.rename(columns={'Trbjo real_y':'Trabajo Real'}, inplace=True)
    df_IW37nBase.rename(columns={'StatUsu_y':'Status Usuario'}, inplace=True)
    
    df_IW37nBase['Status Sistema Reportado'] = df_IW37nBase['Status Sistema Reportado'].str[:9]
    Result_Condiciones.rename(columns={'StatusSistema':'Status Sistema Reportado'}, inplace=True)
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['Status Sistema Reportado','StatusKPI']], on='Status Sistema Reportado',how='left')
    df_IW37nBase["OTCerradas"] =  np.where(df_IW37nBase["StatusKPI"] == "Cerrado", df_IW37nBase["Orden"], 0)
    df_IW37nBase["UT"] = df_IW37nBase["Ubic.técn."].str[:13].str.strip()
    
    #Temporal
    #---------------------------------------------
    df_IW37nBase["UT2"] = df_IW37nBase["Ubic.técn."].str[9:13].str.strip()
    df_IW37nBase["Temp"] = np.where(df_IW37nBase["UT2"] == "SUBS", df_IW37nBase["Ubic.técn."], df_IW37nBase["UT"])
    df_IW37nBase["UT"] = df_IW37nBase["Temp"]
    df_IW37nBase.drop(columns=["Temp","UT2"], inplace=True)
    #---------------------------------------------
    
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['UT','Area', 'SubArea']], on='UT',how='left')
    Result_Condiciones.rename(columns={'Ptotrabajo':'PtoTrbRes'}, inplace=True)
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['PtoTrbRes','Denominacion', 'AreaResponsable', 'Empresa', 'TipoContrato']], on='PtoTrbRes',how='left')
        
    return df_IW37nBase




@router.get("/GetAndProcessIW37nBase_2", tags=["Indicadores"])
async def Process_IW37nBase_2 (type: Optional[str]=None):
    df_result = []  
    
    if type is None:
        print("Obteniendo semanal")
        df_IW37nBase = await Process_IW37nBase()
        Result_IW37nReporte = await Process_IW37nReporte()
        Result_Condiciones = await Process_Condiciones()
        Result_IW39 = await Process_IW39 ()
    else:
        print("Obteniendo anual")
        df_IW37nBase = await Process_IW37nBase(type="Total")
        Result_IW37nReporte = await Process_IW37nReporte(type="Total")
        Result_Condiciones = await Process_Condiciones()
        Result_IW39 = await Process_IW39 (type="Total")
    
    
    Result_IW39 = Result_IW39[["Status del sistema","Orden-Semana"]]
    df_IW37nBase = pd.merge(df_IW37nBase, Result_IW39, on='Orden-Semana',how='left')
    df_IW37nBase.rename(columns={'Status del sistema':'Status Sistema Reportado'}, inplace=True)
    
    Result_IW37nReporte = Result_IW37nReporte[["Trbjo real","StatUsu","Orden-Semana"]]
    df_IW37nBase = pd.merge(df_IW37nBase, Result_IW37nReporte, on='Orden-Semana',how='left')
    df_IW37nBase.rename(columns={'Trbjo real_x':'Trbjo real'}, inplace=True)
    df_IW37nBase.rename(columns={'StatUsu_x':'StatUsu'}, inplace=True)
    df_IW37nBase.rename(columns={'Trbjo real_y':'Trabajo Real'}, inplace=True)
    df_IW37nBase.rename(columns={'StatUsu_y':'Status Usuario'}, inplace=True)
    
    df_IW37nBase['Status Sistema Reportado'] = df_IW37nBase['Status Sistema Reportado'].str[:9]
    Result_Condiciones.rename(columns={'StatusSistema':'Status Sistema Reportado'}, inplace=True)
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['Status Sistema Reportado','StatusKPI']], on='Status Sistema Reportado',how='left')
    df_IW37nBase["OTCerradas"] =  np.where(df_IW37nBase["StatusKPI"] == "Cerrado", df_IW37nBase["Orden"], 0)
    df_IW37nBase["UT"] = df_IW37nBase["Ubic.técn."].str[:13].str.strip()
    
    #Temporal
    #---------------------------------------------
    df_IW37nBase["UT2"] = df_IW37nBase["Ubic.técn."].str[9:13].str.strip()
    df_IW37nBase["Temp"] = np.where(df_IW37nBase["UT2"] == "SUBS", df_IW37nBase["Ubic.técn."], df_IW37nBase["UT"])
    df_IW37nBase["UT"] = df_IW37nBase["Temp"]
    df_IW37nBase.drop(columns=["Temp","UT2"], inplace=True)
    #---------------------------------------------
    
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['UT','Area', 'SubArea']], on='UT',how='left')
    Result_Condiciones.rename(columns={'Ptotrabajo':'PtoTrbRes'}, inplace=True)
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['PtoTrbRes','Denominacion', 'AreaResponsable', 'Empresa', 'TipoContrato']], on='PtoTrbRes',how='left')
    
    function_return_Streaming(df_IW37nBase,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención y procesamiento de IW47 desde MongoDB")
    return StreamingResponse(generate(), media_type='application/json')

async def Process_IW47 (type: Optional[str]=None):
    All_Data_IW47 = []
    print("Obteniendo datos de MongoDB IW47")
    current_year, Semana, Anho = get_current_datetime()
    print("Año actual: ", current_year,"Semana: ",Semana,"Anho: ",Anho)
    if type is None:
        print("Obteniendo semanal")
        CursorIW47 = db.iw47.find({
            "Semana": str(Semana),
            "Anho": str(Anho)
        })
        df_IW37nBase = await Process_IW37nBase() #Modificado era Process_IW37nBase()
        df_IW37nReporte = await Process_IW37nReporte()
        df_Condiciones = await Process_Condiciones()
        df_IW39 = await Process_IW39()
    else:
        print("Obteniendo anual")
        CursorIW47 = db.iw47.find({
            "Anho": str(Anho) #{"$exists": False}
        })
        df_IW37nBase = await Process_IW37nBase(type="Total")
        df_IW37nReporte = await Process_IW37nReporte(type="Total")
        df_Condiciones = await Process_Condiciones()
        df_IW39 = await Process_IW39(type="Total")
    
    print("Procesando los datos de MongoDB IW47")
    await id_to_string_process(CursorIW47,All_Data_IW47)
        
    print("Creando los data frame IW47")
    df_IW47 = pd.DataFrame(All_Data_IW47)
    
    df_IW47 = df_IW47.drop_duplicates()

    df_IW47 = pd.merge(df_IW47,df_IW37nBase[['Orden','Revisión']], on='Orden',how='left')

    df_IW47["RevisionIW47"] = "SEM" + df_IW47["Semana"].astype(str).str.zfill(2) + "-" + str(Anho)[-2:]
    df_IW47["Condicional"] = np.where(df_IW47["Revisión"] != df_IW47["RevisionIW47"], 1, 0)
    df_IW47 = df_IW47[df_IW47["Condicional"] == 1]
    
    #Nuevo
    df_IW47["Orden-Semana"] = df_IW47["Orden"].astype(str) + "-" + df_IW47["Semana"].astype(str)
    
    #df_IW47 = pd.merge(df_IW47,df_IW39[['Orden',"CpoClasif", "P", "PtoTrbRes", "Status del sistema", "StatUsu", "Texto breve", "Ubicación técnica"]], on='Orden',how='left')
    df_IW47 = pd.merge(df_IW47,df_IW39[["Orden-Semana","CpoClasif", "P", "PtoTrbRes", "Status del sistema", "StatUsu", "Texto breve", "Ubicación técnica"]], on='Orden-Semana',how='left')
    
    df_IW47.rename(columns={'P':'Prioridad'}, inplace=True)
    df_IW47.rename(columns={'Status del sistema':'Status Sistema Reportado'}, inplace=True)
    df_IW47["Status Sistema Reportado"] = df_IW47["Status Sistema Reportado"].str[:9].str.strip()
    df_IW47["Status Sistema Reportado"] = df_IW47["Status Sistema Reportado"].str[:9].str.strip()
    df_IW47.drop(columns=["Condicional","Revisión"], inplace=True)
    df_IW47.rename(columns={'RevisionIW47':'Revisión'}, inplace=True)   
    df_IW47 = df_IW47.drop_duplicates()
    #df_IW47 = pd.merge(df_IW47,df_IW37nReporte[['Orden','Inic.extr.']], on='Orden',how='left')
    df_IW47 = pd.merge(df_IW47,df_IW37nReporte[['Orden-Semana','Inic.extr.']], on='Orden-Semana',how='left')
    print("se saco la column orden-semana")
    df_IW47 = df_IW47.drop_duplicates()

    df_IW47["UT"] = df_IW47["Ubicación técnica"].str[:13].str.strip()
    
    df_Condiciones.rename(columns={'StatusSistema':'Status Sistema Reportado'}, inplace=True)
    
    df_IW47 = pd.merge(df_IW47,df_Condiciones[['Status Sistema Reportado','StatusKPI']], on='Status Sistema Reportado',how='left')
    df_Condiciones.rename(columns={'Ptotrabajo':'PtoTrbRes'}, inplace=True)
    df_IW47 = pd.merge(df_IW47,df_Condiciones[['PtoTrbRes','Denominacion', 'AreaResponsable']], on='PtoTrbRes',how='left')
    df_IW47 = pd.merge(df_IW47,df_Condiciones[['UT','Area', 'SubArea']], on='UT',how='left')
    df_IW47 = df_IW47.drop_duplicates()
    
    df_IW47['Temp'] = df_IW47["Ubicación técnica"].fillna('').str.startswith('JP11-MI1').astype(int)
    df_IW47 = df_IW47[df_IW47['Temp'] == 0]
    df_IW47.drop(columns=["_id", "Temp"], inplace=True)
    df_IW47.rename(columns={'Inic.extr.':'InicioExtremo'}, inplace=True)
    print(df_IW47.shape[0])
    df_IW47 = df_IW47.drop_duplicates(subset=['Orden'])
    print(df_IW47.shape[0])
    
    df_IW37nReporte['Trbjo real'] = pd.to_numeric(df_IW37nReporte['Trbjo real'], errors='coerce')
    df_IW37nReporte = df_IW37nReporte.groupby(['Orden-Semana'], as_index=False)['Trbjo real'].sum()
    df_IW47 = pd.merge(df_IW47,df_IW37nReporte[['Orden-Semana','Trbjo real']], on='Orden-Semana',how='left')
    print("Cantidad despues de merge con cantidad de horas")
    print(len(df_IW47))
    return df_IW47

async def Process_IW29 (type: Optional[str] = None):
    
    if type is None:
        All_Data_IW29 = []
        print("Obteniendo datos de MongoDB IW29")
        current_date, Semana, Anho = get_current_datetime()
        print("Semana: ", Semana, "Anho: ", Anho)

        print("Obteniendo semanal")
        CursorIW29 = db.iw29.find({
            "Semana": str(Semana),
            "Anho": str(Anho)
        })
        print("Obteniendo semanal")
    else:
        print("Obteniendo anual")
        CursorIW29 = db.iw29.find({
            "Anho": str(Anho)
        })
    
    print("Procesando los datos de MongoDB IW29")
    await id_to_string_process(CursorIW29,All_Data_IW29)

    #print(len(All_Data_IW29))

    print("Creando los data frame IW29")
    df_IW29 = pd.DataFrame(All_Data_IW29)
    
    # print(df_IW29)
    # print(df_IW29.columns)
    
    dataPrioridad = [
    {"Prioridad": "1", "DescripcionPrioridad": "E:Emergencia"},
    {"Prioridad": "2", "DescripcionPrioridad": "A:Alta"},
    {"Prioridad": "3", "DescripcionPrioridad": "B:Media"},
    {"Prioridad": "4", "DescripcionPrioridad": "C:Baja"},
    {"Prioridad": "5", "DescripcionPrioridad": "D:Muy Baja"},
    ]
    
    dataPrioridadResponsable =[
        {"PrioridadResponsable": "E:Emergencia-Mantto Mecánico-OXI Z. Seca", "Responsables": "Programador Mec. Óxidos"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Mecánico-OXI Z. Húmeda", "Responsables": "Programador Mec. Óxidos"},
        {"PrioridadResponsable": "E:Emergencia-Confiabilidad-OXI Z. Seca", "Responsables": "Programador Mec. Óxidos"},
        {"PrioridadResponsable": "E:Emergencia-Confiabilidad-OXI Z. Húmeda", "Responsables": "Programador Mec. Óxidos"},
        {"PrioridadResponsable": "A:Alta-Mantto Mecánico-OXI Z. Seca", "Responsables": "Programador Mec. Óxidos"},
        {"PrioridadResponsable": "A:Alta-Mantto Mecánico-OXI Z. Húmeda", "Responsables": "Programador Mec. Óxidos"},
        {"PrioridadResponsable": "A:Alta-Confiabilidad-OXI Z. Seca", "Responsables": "Programador Mec. Óxidos"},
        {"PrioridadResponsable": "A:Alta-Confiabilidad-OXI Z. Húmeda", "Responsables": "Programador Mec. Óxidos"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Mecánico-SUL Z. Seca", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Mecánico-SUL Z. Húmeda", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Mecánico-Truck Shop", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "E:Emergencia-Confiabilidad-SUL Z. Seca", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "E:Emergencia-Confiabilidad-SUL Z. Húmeda", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "E:Emergencia-Confiabilidad-Truck Shop", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "A:Alta-Mantto Mecánico-SUL Z. Seca", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "A:Alta-Mantto Mecánico-SUL Z. Húmeda", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "A:Alta-Mantto Mecánico-Truck Shop", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "A:Alta-Confiabilidad-SUL Z. Seca", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "A:Alta-Confiabilidad-SUL Z. Húmeda", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "A:Alta-Confiabilidad-Truck Shop", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "E:Emergencia-Mantto E & I-SUL Z. Seca", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto E & I-SUL Z. Húmeda", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto E & I-OXI Z. Seca", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto E & I-OXI Z. Húmeda", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto E & I-Truck Shop", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto E & I-Potencia", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Potencia-SUL Z. Seca", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Potencia-SUL Z. Húmeda", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Potencia-OXI Z. Seca", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Potencia-OXI Z. Húmeda", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Potencia-Truck Shop", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Potencia-Potencia", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Confiabilidad-Potencia", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto E & I-SUL Z. Seca", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto E & I-SUL Z. Húmeda", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto E & I-OXI Z. Seca", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto E & I-OXI Z. Húmeda", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto E & I-Truck Shop", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto E & I-Potencia", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto Potencia-SUL Z. Seca", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto Potencia-SUL Z. Húmeda", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto Potencia-OXI Z. Seca", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto Potencia-OXI Z. Húmeda", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto Potencia-Truck Shop", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto Potencia-Potencia", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Confiabilidad-Potencia", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "B:Media-Mantto Mecánico-OXI Z. Seca", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "B:Media-Confiabilidad-OXI Z. Seca", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "C:Baja-Mantto Mecánico-OXI Z. Seca", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "C:Baja-Confiabilidad-OXI Z. Seca", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Mecánico-OXI Z. Seca", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "D:Muy Baja-Confiabilidad-OXI Z. Seca", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "B:Media-Mantto Mecánico-OXI Z. Húmeda", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "B:Media-Confiabilidad-OXI Z. Húmeda", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "C:Baja-Mantto Mecánico-OXI Z. Húmeda", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "C:Baja-Confiabilidad-OXI Z. Húmeda", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Mecánico-OXI Z. Húmeda", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "D:Muy Baja-Confiabilidad-OXI Z. Húmeda", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "B:Media-Mantto Mecánico-SUL Z. Seca", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "B:Media-Confiabilidad-SUL Z. Seca", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "C:Baja-Mantto Mecánico-SUL Z. Seca", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "C:Baja-Confiabilidad-SUL Z. Seca", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Mecánico-SUL Z. Seca", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "D:Muy Baja-Confiabilidad-SUL Z. Seca", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "B:Media-Mantto Mecánico-SUL Z. Húmeda", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "B:Media-Confiabilidad-SUL Z. Húmeda", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "C:Baja-Mantto Mecánico-SUL Z. Húmeda", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "C:Baja-Confiabilidad-SUL Z. Húmeda", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Mecánico-SUL Z. Húmeda", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "D:Muy Baja-Confiabilidad-SUL Z. Húmeda", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "B:Media-Mantto E & I-SUL Z. Seca", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "B:Media-Mantto E & I-SUL Z. Húmeda", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "B:Media-Mantto E & I-OXI Z. Seca", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "B:Media-Mantto E & I-OXI Z. Húmeda", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "B:Media-Mantto E & I-Truck Shop", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "C:Baja-Mantto E & I-SUL Z. Seca", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "C:Baja-Mantto E & I-SUL Z. Húmeda", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "C:Baja-Mantto E & I-OXI Z. Seca", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "C:Baja-Mantto E & I-OXI Z. Húmeda", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "C:Baja-Mantto E & I-Truck Shop", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto E & I-SUL Z. Seca", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto E & I-SUL Z. Húmeda", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto E & I-OXI Z. Seca", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto E & I-OXI Z. Húmeda", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto E & I-Truck Shop", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Confiabilidad-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "E:Emergencia-Confiabilidad-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Mecánico-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Mecánico-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "A:Alta-Confiabilidad-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "A:Alta-Confiabilidad-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "A:Alta-Mantto Mecánico-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "A:Alta-Mantto Mecánico-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Confiabilidad-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Confiabilidad-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Confiabilidad-Potencia", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto Mecánico-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto Mecánico-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto Mecánico-Potencia", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Confiabilidad-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Confiabilidad-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Confiabilidad-Potencia", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto Mecánico-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto Mecánico-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto Mecánico-Potencia", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Confiabilidad-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Confiabilidad-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Confiabilidad-Potencia", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Mecánico-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Mecánico-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Mecánico-Potencia", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "E:Emergencia-Mantto E & I-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "E:Emergencia-Mantto E & I-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Potencia-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Potencia-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "A:Alta-Mantto E & I-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "A:Alta-Mantto E & I-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "A:Alta-Mantto Potencia-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "A:Alta-Mantto Potencia-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto E & I-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto E & I-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto E & I-Potencia", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto Potencia-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto Potencia-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto Potencia-Potencia", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto E & I-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto E & I-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto E & I-Potencia", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto Potencia-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto Potencia-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto Potencia-Potencia", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto E & I-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto E & I-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto E & I-Potencia", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Potencia-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Potencia-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Potencia-Potencia", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
    ]

    dataStatusSistemaAvisos = [
        {"Stat.sist.": "MDIF", "DescripcionStatus": "4. Avisos Abiertos"},
        {"Stat.sist.": "MEAB", "DescripcionStatus": "4. Avisos Abiertos"},
        {"Stat.sist.": "MECE", "DescripcionStatus": "5. Avisos Rechazados"},
        {"Stat.sist.": "MECE MIMP ORAS", "DescripcionStatus": "1. Avisos Ejecutados (OT cerrada)"},
        {"Stat.sist.": "MECE ORAS", "DescripcionStatus": "1. Avisos Ejecutados (OT cerrada)"},
        {"Stat.sist.": "MECE ORAS PTBO", "DescripcionStatus": "1. Avisos Ejecutados (OT cerrada)"},
        {"Stat.sist.": "MECE PTBO", "DescripcionStatus": "5. Avisos Rechazados"},
        {"Stat.sist.": "METR", "DescripcionStatus": "3. Avisos Aprobados (Liberado)"},
        {"Stat.sist.": "METR MIMP ORAS", "DescripcionStatus": "2. Avisos Tratados (Con OT)"},
        {"Stat.sist.": "METR ORAS PREI", "DescripcionStatus": "2. Avisos Tratados (Con OT)"},
        {"Stat.sist.": "METR ORAS", "DescripcionStatus": "2. Avisos Tratados (Con OT)"},
    ]
    
    dataResponsablePlaneamiento = [
        {"PosicionPlaneamiento": "Programador Mec. Óxidos", "Responsables": "Nelly Morales/Cliver Cari"},
        {"PosicionPlaneamiento": "Programador Mec. Sulfuros", "Responsables": "Dayana Miranda/Tatiana Hualla"},
        {"PosicionPlaneamiento": "Programador E&I", "Responsables": "Erick Ramos/Joel Valentin"},
        {"PosicionPlaneamiento": "Planificador de Óxidos", "Responsables": "Jean Espinoza/Jorge Rodriguez"},
        {"PosicionPlaneamiento": "Planificador de Sulfuros", "Responsables": "Johan Callomamani/Renato Alarcon"},
        {"PosicionPlaneamiento": "Planificador E&I", "Responsables": "Jhon Quispe/Pedro Milachay"},
        {"PosicionPlaneamiento": "Planificador Mec. Puerto/Área 4000/Potencia", "Responsables": "Jorge Ramirez/Sadin Valdivia"},
        {"PosicionPlaneamiento": "Planificador E&I Puerto/Área 4000/Potencia", "Responsables": "Jorge Ramirez/Sadin Valdivia"},
    ]
    
    dataResponsableEjecucionAvisos = [
    {"Area": "ENERGÍA", "AreaResponsable": "Mantto E & I", "ResponsableEjecucion": "Andy Cabrera"},
    {"Area": "ENERGÍA", "AreaResponsable": "Confiabilidad", "ResponsableEjecucion": "Carlos Bardales/Juan Huete"},
    {"Area": "ENERGÍA", "AreaResponsable": "Mantto Potencia", "ResponsableEjecucion": "Andy Cabrera"},
    {"Area": "INFRAESTRUCTURA", "AreaResponsable": "Mantto E & I", "ResponsableEjecucion": "Jorge F / Juan Salazar"},
    {"Area": "INFRAESTRUCTURA", "AreaResponsable": "Mantto Mecánico", "ResponsableEjecucion": "Herbert Yuto"},
    {"Area": "ÓXIDOS", "AreaResponsable": "Mantto E & I", "ResponsableEjecucion": "Jorge F / Juan Salazar"},
    {"Area": "ÓXIDOS", "AreaResponsable": "Mantto Mecánico", "ResponsableEjecucion": "Eduardo Bernahola"},
    {"Area": "ÓXIDOS", "AreaResponsable": "Confiabilidad", "ResponsableEjecucion": "Carlos Bardales/Juan Huete"},
    {"Area": "PUERTO", "AreaResponsable": "Mantto E & I", "ResponsableEjecucion": "Jorge F / Juan Salazar"},
    {"Area": "PUERTO", "AreaResponsable": "Mantto Mecánico", "ResponsableEjecucion": "Wiles Chavez/ Herbert Yuto"},
    {"Area": "SULFUROS", "AreaResponsable": "Mantto Mecánico", "ResponsableEjecucion": "Martin Castro/ Jimmy Solano"},
    {"Area": "SULFUROS", "AreaResponsable": "Mantto E & I", "ResponsableEjecucion": "Jorge F / Juan Salazar"},
    {"Area": "SULFUROS", "AreaResponsable": "Mantto Potencia", "ResponsableEjecucion": "Andy Cabrera"},
    {"Area": "SULFUROS", "AreaResponsable": "Confiabilidad", "ResponsableEjecucion": "Carlos Bardales/Juan Huete"},
    {"Area": "ENERGÍA", "AreaResponsable": "Termofusión", "ResponsableEjecucion": "Wiles Chavez"},
    {"Area": "INFRAESTRUCTURA", "AreaResponsable": "Termofusión", "ResponsableEjecucion": "Wiles Chavez"},
    {"Area": "ÓXIDOS", "AreaResponsable": "Termofusión", "ResponsableEjecucion": "Wiles Chavez"},
    {"Area": "PUERTO", "AreaResponsable": "Termofusión", "ResponsableEjecucion": "Wiles Chavez"},
    {"Area": "SULFUROS", "AreaResponsable": "Termofusión", "ResponsableEjecucion": "Wiles Chavez"},
    ]
    
    df_Prioridad = pd.DataFrame(dataPrioridad)
    df_dataPrioridadResponsable = pd.DataFrame(dataPrioridadResponsable)
    df_dataStatusSistemaAvisos = pd.DataFrame(dataStatusSistemaAvisos)
    df_dataResponsablePlaneamiento = pd.DataFrame(dataResponsablePlaneamiento)
    df_dataResponsableEjecucionAvisos = pd.DataFrame(dataResponsableEjecucionAvisos)
    df_Condiciones = await Process_Condiciones()


    df_IW29["Creado el"] = pd.to_datetime(df_IW29["Creado el"].str.replace(".", "/"), format="%d/%m/%Y").dt.date
    df_IW29['Creado el'] = df_IW29['Creado el'].apply(lambda x: pd.to_datetime(x, unit='ms') if isinstance(x, (int, float)) else pd.to_datetime(x)).dt.strftime('%Y-%m-%dT%H:%M:%S')
    df_IW29 = df_IW29[df_IW29["Cl."] != "Z3"]
    df_IW29.rename(columns={'P':'Prioridad'}, inplace=True)
    df_IW29 = pd.merge(df_IW29,df_Prioridad, on='Prioridad',how='left')
    df_IW29["UT"] = df_IW29["Ubicac.técnica"].str[:13].str.strip()
    df_IW29 = pd.merge(df_IW29,df_Condiciones[['UT','Area', 'SubArea']], on='UT',how='left')
    
    df_Condiciones.rename(columns={'Ptotrabajo':'PtoTrbRes'}, inplace=True)
    df_IW29 = pd.merge(df_IW29,df_Condiciones[['PtoTrbRes', 'AreaResponsable']], on='PtoTrbRes',how='left')
    df_IW29["PrioridadResponsable"] = df_IW29["DescripcionPrioridad"].astype(str) + "-" + df_IW29["AreaResponsable"].astype(str) + "-" + df_IW29["SubArea"].astype(str)
    df_IW29 = pd.merge(df_IW29,df_dataPrioridadResponsable[['PrioridadResponsable','Responsables']], on='PrioridadResponsable',how='left')
    df_IW29 = pd.merge(df_IW29,df_dataStatusSistemaAvisos[['Stat.sist.','DescripcionStatus']], on='Stat.sist.',how='left')
    df_IW29.rename(columns={'Responsables':'PosicionPlaneamiento'}, inplace=True)
    df_IW29 = pd.merge(df_IW29,df_dataResponsablePlaneamiento[['PosicionPlaneamiento','Responsables']], on='PosicionPlaneamiento',how='left')
    df_IW29 = pd.merge(df_IW29,df_dataResponsableEjecucionAvisos[['Area','AreaResponsable','ResponsableEjecucion']], on=['Area', 'AreaResponsable'],how='left')
    df_IW29 = df_IW29.drop_duplicates()
    df_IW29 = df_IW29.drop(columns=["Fecha"])
    df_IW29.rename(columns={'Creado el':'Fecha'}, inplace=True)
    
    return df_IW29

#Proceso de Tratamiento de Datos
@router.get("/GetAndProcessIW39", tags=["Indicadores"])
async def Get_Process_IW39 ():
    df_result = []  
        
    df_IW39 = await Process_IW39()
    
    function_return_Streaming(df_IW39,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención y procesamiento de IW39 desde MongoDB")
    return StreamingResponse(generate(), media_type='application/json')

@router.get("/GetAndProcessIW37nReporte", tags=["Indicadores"])
async def Get_Process_IW37nReporte ():
    df_result = []  
    
    df_IW37nReporte = await Process_IW37nReporte()

    print(df_IW37nReporte.columns)
    
    function_return_Streaming(df_IW37nReporte,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención y procesamiento de IW37nReporte desde MongoDB")
    return StreamingResponse(generate(), media_type='application/json')

@router.get("/GetAndProcessIW37nBase", tags=["Indicadores"])
async def Get_Process_IW37nReporte ():
    df_result = []  
    
    df_IW37nBase = await Process_IW37nBase_2()
    
    function_return_Streaming(df_IW37nBase,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención y procesamiento de IW37nBase desde MongoDB")
    return StreamingResponse(generate(), media_type='application/json')

@router.get("/GetAndProcessIW47", tags=["Indicadores"])
async def Get_Process_IW47 ():
    df_result = []  
    
    df_IW47 = await Process_IW47()
    
    function_return_Streaming(df_IW47,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención y procesamiento de IW47 desde MongoDB")
    return StreamingResponse(generate(), media_type='application/json')

@router.get("/GetAndProcessIW29", tags=["Indicadores"])
async def Get_Process_IW29 ():
    df_result = []  
    
    df_IW29 = await Process_IW29()

    function_return_Streaming(df_IW29,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención y procesamiento de IW29 desde MongoDB")
    return StreamingResponse(generate(), media_type='application/json')


@router.post("/UpdateDataIndicadoresToRedis", tags=["Indicadores"])
async def Update_Data_Indicadores_To_Redis ():

    print("iniciando update to redis indicadores")
    df_Backlog = await Process_Baklog()
    df_HHDisponibles = await Process_HHDisponibles()
    df_Criticidad = await Process_CriticidadEquipos()
    df_IW29 = await Process_IW29()
    # df_IW39 = await Process_IW39("Total")
    # df_IW37nReporte = await Process_IW37nReporte("Total")
    # df_IW37nBase = await Process_IW37nBase_2("Total")
    # df_IW47 = await Process_IW47("Total")
    df_IW39 = await Process_IW39()
    df_IW37nReporte = await Process_IW37nReporte()
    df_IW37nBase = await Process_IW37nBase_3()
    df_IW47 = await Process_IW47()
    #Proceso en Redis
    
    Process_Status_Data_Indicadores = RedisDockers.get('Process_Status_Data_Indicadores')
    print(Process_Status_Data_Indicadores)
    
    if Process_Status_Data_Indicadores is None or Process_Status_Data_Indicadores.decode('utf-8') != 'in progess':
        
        RedisDockers.set('Process_Status_Data_Indicadores','in progess')
        print("Iniciando Carga de datos de indicadores a Redis")
        
        RedisDockers.set('df_IW29',pickle.dumps(df_IW29))
        df = pickle.loads(RedisDockers.get('df_IW29'))
        print("VALIDAR Filas en df_IW29:", df.shape[0])
        RedisDockers.set('df_IW39',pickle.dumps(df_IW39))
        RedisDockers.set('df_IW37nReporte',pickle.dumps(df_IW37nReporte))
        RedisDockers.set('df_IW37nBase',pickle.dumps(df_IW37nBase))
        RedisDockers.set('df_IW47',pickle.dumps(df_IW47))
        RedisDockers.set('df_Backlog',pickle.dumps(df_Backlog))
        RedisDockers.set('df_HHDisponibles',pickle.dumps(df_HHDisponibles))
        RedisDockers.set('df_Criticidad',pickle.dumps(df_Criticidad))
        print("Proceso de Carga de datos de indicadores a Redis finalizado")
        RedisDockers.set('Process_Status_Data_Indicadores','completed')
        
        return ({
            "Message": "Oki Doki"
            })
    
@router.get('/GetDataIndicadoresFromRedis', tags=["Indicadores"])
async def Get_Data_Indicadores_From_Redis():
    
    print("Obteniendo datos de indicadores de redis")
    
    pickled_IW29 = RedisDockers.get('df_IW29')
    pickled_IW39 = RedisDockers.get('df_IW39')
    pickled_IW37nReporte = RedisDockers.get('df_IW37nReporte')
    pickled_IW37nBase = RedisDockers.get('df_IW37nBase')
    pickled_IW47 = RedisDockers.get('df_IW47')
    pickled_Backlog = RedisDockers.get('df_Backlog')
    pickled_HHDisponibles = RedisDockers.get('df_HHDisponibles')
    pickle_Criticidad = RedisDockers.get('df_Criticidad')

    df_IW29 = pickle.loads(pickled_IW29)
    df_IW39 = pickle.loads(pickled_IW39)
    df_IW37nReporte = pickle.loads(pickled_IW37nReporte)
    df_IW37nBase = pickle.loads(pickled_IW37nBase)
    df_IW47 = pickle.loads(pickled_IW47)
    df_Backlog = pickle.loads(pickled_Backlog)
    df_HHDisponibles = pickle.loads(pickled_HHDisponibles)
    df_Criticidad = pickle.loads(pickle_Criticidad)
    
    df_IW29 = df_IW29.where(pd.notnull(df_IW29), '')
    df_IW39 = df_IW39.where(pd.notnull(df_IW39), '')
    df_IW37nReporte = df_IW37nReporte.where(pd.notnull(df_IW37nReporte), '')
    df_IW37nBase = df_IW37nBase.where(pd.notnull(df_IW37nBase), '')
    df_IW47 = df_IW47.where(pd.notnull(df_IW47), '')
    df_Backlog = df_Backlog.where(pd.notnull(df_Backlog), '')
    df_HHDisponibles = df_HHDisponibles.where(pd.notnull(df_HHDisponibles), '')
    df_Criticidad = df_Criticidad.where(pd.notnull(df_Criticidad), '') 
    
    data_IW29 = df_IW29.to_dict(orient='records')
    data_IW39 = df_IW39.to_dict(orient='records')
    data_IW37nReporte = df_IW37nReporte.to_dict(orient='records')
    data_IW37nBase = df_IW37nBase.to_dict(orient='records')
    data_IW47 = df_IW47.to_dict(orient='records')
    data_Backlog = df_Backlog.to_dict(orient='records')
    data_HHDisponibles = df_HHDisponibles.to_dict(orient='records')
    data_Criticidad = df_Criticidad.to_dict(orient='records')
    
    
    print("Finalizando el proceso de Obteniendo datos de Indicadores desde redis")
    
    return {
        "data_IW29": data_IW29,
        "data_IW39": data_IW39,
        "data_IW37nReporte": data_IW37nReporte,
        "data_IW37nBase": data_IW37nBase,
        "data_IW47": data_IW47,
        "data_Backlog": data_Backlog,
        "data_HHDisponibles":  data_HHDisponibles,
        "data_Criticidad": data_Criticidad
    }

@router.get("/Pruebafechas", tags=["Indicadores"])
async def Prueba_fechas ():
    
    #current_date, Semana, Anho, current_week, last_week = get_current_datetime()
    df_IW47 = await Process_IW39("Total")
    return "Oki Doki"
    # return{
    #     "current_date":current_date,
    #     "current_week":current_week,
    #     "last_week":last_week,
    #     "Semana":Semana,
    #     "Anho":Anho
    # }

@router.post("/borrarredis", tags=["Indicadores"])
async def BorrarRedis ():
    print("Borrando redis")
    RedisDockers.delete('df_IW29')
    RedisDockers.delete('df_IW39')
    RedisDockers.delete('df_IW37nReporte')
    RedisDockers.delete('df_IW37nBase')
    RedisDockers.delete('df_IW47')
    print("Redis borrado")
    return {
        "Message": "Oki Doki"
    }


@router.post("/pruebaiw29", tags=["Indicadores"])
async def Prueba ():
    print
    All_Data_IW29 = []
    CursorIW29 = db.iw29.find({
            "Anho": {"$exists": False}
        })
    await id_to_string_process(CursorIW29,All_Data_IW29)
    print(All_Data_IW29)

    return {
            "Message": "Oki Doki"
        }

@router.post("/resetproceesStatus", tags=["Indicadores"])
async def resetproceesStatus ():
    RedisDockers.set('Process_Status_Data_Indicadores','completed')
    return {
            "Message": "Oki Doki"
    }

@router.get("/Process2", tags=["Indicadores"])
async def Process_Condiciones_2 ():
    df_result = []  
    All_Data_Condiciones2 = []
    print("Obteniendo datos de MongoDB Condiciones")
    CursorIndicadores_2 = db.Condicion2.find({})
    
    print("Procesando los datos de MongoDB Condiciones")
    await id_to_string_process(CursorIndicadores_2,All_Data_Condiciones2)
    df_Indicadores2 = pd.DataFrame(All_Data_Condiciones2)
    df_Indicadores2["Concatenacion"]= df_Indicadores2["UbicacionTecnica"].str.cat(df_Indicadores2["Ptotrabajo"], sep="-")
    df_Indicadores2["Concatenacion"] = df_Indicadores2["Concatenacion"].fillna("-")
    df_Indicadores2["Concat"]= df_Indicadores2["UT"].str.cat(df_Indicadores2["Ptotrabajo"], sep="-")
    df_Indicadores2["Concat"] = df_Indicadores2["Concat"].fillna("-")
    df_Indicadores2 = df_Indicadores2.drop_duplicates()
    print(df_Indicadores2.columns)
    df_prueba =df_Indicadores2[["Ptotrabajo"]]
    print(df_prueba)
    
    # function_return_Streaming(df_Indicadores2,df_result)
    # def generate():

    #     for chunk in df_result:
    #         yield from chunk
    # print("Finalizando el proceso de obtención y procesamiento de IW47 desde MongoDB")
    # return StreamingResponse(generate(), media_type='application/json')
    return df_Indicadores2

@router.get("/GetAndProcessIW37nBase_3", tags=["Indicadores"])
async def Process_IW37nBase_3(ype: Optional[str]=None):


    df_result = []  
    df_IW37nBase = await Process_IW37nBase()
    Result_IW37nReporte = await Process_IW37nReporte()
    Result_Condiciones = await Process_Condiciones_2()
    Result_IW39 = await Process_IW39 ()

    Result_IW39 = Result_IW39[["Status del sistema","Orden-Semana"]]
    df_IW37nBase = pd.merge(df_IW37nBase, Result_IW39, on='Orden-Semana',how='left')
    df_IW37nBase.rename(columns={'Status del sistema':'Status Sistema Reportado'}, inplace=True)

    Result_IW37nReporte = Result_IW37nReporte[["Trbjo real","StatUsu","Orden-Semana"]]
    df_IW37nBase = pd.merge(df_IW37nBase, Result_IW37nReporte, on='Orden-Semana',how='left')
    df_IW37nBase.rename(columns={'Trbjo real_x':'Trbjo real'}, inplace=True)
    df_IW37nBase.rename(columns={'StatUsu_x':'StatUsu'}, inplace=True)
    df_IW37nBase.rename(columns={'Trbjo real_y':'Trabajo Real'}, inplace=True)
    df_IW37nBase.rename(columns={'StatUsu_y':'Status Usuario'}, inplace=True)

    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['PtoTrbRes','Denominacion', 'AreaResponsable', 'Empresa', 'TipoContrato']], on='PtoTrbRes',how='left')
    print(df_IW37nBase)

    df_IW37nBase['Status Sistema Reportado'] = df_IW37nBase['Status Sistema Reportado'].str[:9]
    Result_Condiciones.rename(columns={'StatusSistema':'Status Sistema Reportado'}, inplace=True)
    #Result_Condiciones.rename(columns={'Ptotrabajo':'PtoTrbRes'}, inplace=True)
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['Status Sistema Reportado','StatusKPI']], on='Status Sistema Reportado',how='left')
    #df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['PtoTrbRes','Denominacion', 'AreaResponsable', 'Empresa', 'TipoContrato']], on='PtoTrbRes',how='left')
    df_IW37nBase["OTCerradas"] =  np.where(df_IW37nBase["StatusKPI"] == "Cerrado", df_IW37nBase["Orden"], 0)
    
    #Eliminando Mina
    df_IW37nBase['Seccion'] = df_IW37nBase['Ubic.técn.'].apply(lambda x: x.split('-')[1][:3])
    df_IW37nBase = df_IW37nBase[df_IW37nBase['Seccion'] != 'MI1']
    df_IW37nBase = df_IW37nBase.drop(columns=['Seccion'])

    df_IW37nBase["UT"] = df_IW37nBase["Ubic.técn."].str[:13].str.strip()
   
    #Columna concatenacion larga
    df_IW37nBase["Concatenacion"] = df_IW37nBase["Ubic.técn."].str.cat(df_IW37nBase["PtoTrbRes"], sep="-")
    #Columna concatenacion nulos
    df_IW37nBase["Concat"] = df_IW37nBase["UT"].str.cat(df_IW37nBase["PtoTrbRes"], sep="-")
    #merge normal
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['Concatenacion','Area', 'SubArea']], on='Concatenacion',how='left')
    df_IW37nBase['Vacío'] = df_IW37nBase.apply(lambda row: 'Sí' if pd.isnull(row['Area']) and pd.isnull(row['SubArea']) else 'No', axis=1)
    print("Primer merge numeros de filas")
    print(len(df_IW37nBase))
    #df_IW37nBase = df_IW37nBase.drop_duplicates()
    print("Primer merge numeros de filas, sin duplicados")
    print(len(df_IW37nBase))
    
    #merge con condicional

    df_vacios = df_IW37nBase[df_IW37nBase['Area'].isnull() | df_IW37nBase['SubArea'].isnull()]
    print("df vacios cantidad de filas")
    print(len(df_vacios))

    #Quito duplicados de concat
    result_unico = Result_Condiciones.drop_duplicates(subset='Concat', keep='first')

    df_actualizar = pd.merge(df_vacios,result_unico[['Concat', 'Area', 'SubArea']],on='Concat', how='left',suffixes=('', '_nuevo'))

    print("df vacios con merge")
    print(len(df_vacios))
    df_actualizar['Area'] = df_actualizar.apply(lambda row: row['Area_nuevo'] if pd.isnull(row['Area']) else row['Area'], axis=1)
    df_actualizar['SubArea'] = df_actualizar.apply(lambda row: row['SubArea_nuevo'] if pd.isnull(row['SubArea']) else row['SubArea'], axis=1)
    df_actualizar = df_actualizar.drop(columns=['Area_nuevo', 'SubArea_nuevo'])

    #df_actualizar = df_actualizar.drop_duplicates()
    print("df actualizar con merge y sin duplicados")
    print(len(df_actualizar))

    #Elimo en la principal los Area y subarea vacios
    df_IW37nBase = df_IW37nBase.dropna(subset=['Area', 'SubArea'])

    #Anido ambos data frames
    df_IW37nBase = pd.concat([df_IW37nBase, df_actualizar])
    df_IW37nBase = df_IW37nBase.drop_duplicates()

    #Result_Condiciones.rename(columns={'Ptotrabajo':'PtoTrbRes'}, inplace=True)
    #df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['PtoTrbRes','Denominacion', 'AreaResponsable', 'Empresa', 'TipoContrato']], on='PtoTrbRes',how='left')
    
    print("VALIDARRRRRRRRRRRRRRRRR")
    print(len(df_IW37nBase))
    print(df_IW37nBase.columns)

    # function_return_Streaming(df_IW37nBase,df_result)
    # def generate():
    #     for chunk in df_result:
    #         yield from chunk
    # print("Finalizando el proceso de obtención y procesamiento de IW47 desde MongoDB")
    # return StreamingResponse(generate(), media_type='application/json')
    return df_IW37nBase

@router.get("/GetAndProcessHHDisponibles", tags=["Indicadores"])
async def Get_Process_HHDisponibles ():
    df_result = []  
    
    df_HHDisponibles = await Process_HHDisponibles()

    print(df_HHDisponibles.columns)
    
    function_return_Streaming(df_HHDisponibles,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención y procesamiento de HHdisponibles desde MongoDB")
    return StreamingResponse(generate(), media_type='application/json')

async def Process_HHDisponibles ():
    All_Data_HHDisponibles = []
    print("Obteniendo datos de MongoDB Backlog")
    CursorIndicadores = db.Backlog.find({})
    
    print("Procesando los datos de MongoDB Backlog")
    await id_to_string_process(CursorIndicadores,All_Data_HHDisponibles)
    df_HHDisponibles = pd.DataFrame(All_Data_HHDisponibles)
    
    return df_HHDisponibles

@router.get("/backlog", tags=["Indicadores"])
async def Process_Baklog ():
    df_result = []	

    Result_IW37nReporte = await Process_IW37nReporte()
    Result_Condiciones = await Process_Condiciones_2()
    #Result_IW37nBase = await Process_IW37nBase_3()
    Result_Criticidad = await Process_CriticidadEquipos()
    #Result_IW29 = await Process_IW29()

    print("ESTE ES MI IW37NREPORTE")
    print(Result_IW37nReporte.columns.tolist())
    print(len(Result_IW37nReporte))
    #UT	
    Result_IW37nReporte['UT'] = Result_IW37nReporte['Ubic.técn.'].str[:13]
    #Categoria
    categoria_extract = Result_IW37nReporte['UT'].str[5:8]
    Result_IW37nReporte['Categoria'] = np.where(categoria_extract == 'MI1', 'Mina', 'Planta')
    #Elimino Mina
    Result_IW37nReporte = Result_IW37nReporte[Result_IW37nReporte['Categoria'] != 'Mina']
    print("Cantidad despues de eliminar mina")
    print(len(Result_IW37nReporte))

    #Columna Status del sistema
    Result_IW37nReporte['Status del sistema'] = Result_IW37nReporte['Stat.sist.'].str[:9]
    Result_IW37nReporte = pd.merge(Result_IW37nReporte,Result_Condiciones[['StatusSistema','StatusKPI']],left_on='Status del sistema',right_on='StatusSistema',how='left')
    print("Primer merge con Result_Condiciones")
    print(len(Result_IW37nReporte))

    #Mantenemos las OT Pendientes de cierre
    Result_IW37nReporte = Result_IW37nReporte[Result_IW37nReporte['StatusKPI'] == 'Pendiente de cierre']
    print("Cantidad despues de mantener las OT Pendientes de cierre")
    print(len(Result_IW37nReporte))

    #Columna Puesto de trabajo
    Result_IW37nReporte = pd.merge(Result_IW37nReporte,Result_Condiciones[['PtoTrbRes','Denominacion']],left_on='PtoTrbRes',right_on='PtoTrbRes',how='left')

    #Area y SubArea
    #Columna concatenacion larga
    Result_IW37nReporte["Concatenacion"] = Result_IW37nReporte["Ubic.técn."].str.cat(Result_IW37nReporte["PtoTrbRes"], sep="-")
    #Columna concatenacion nulos
    Result_IW37nReporte["Concat"] = Result_IW37nReporte["UT"].str.cat(Result_IW37nReporte["PtoTrbRes"], sep="-")
    
    #merge normal
    Result_IW37nReporte = pd.merge(Result_IW37nReporte, Result_Condiciones[['Concatenacion','Area', 'SubArea']], on='Concatenacion',how='left')

    print("Primer merge numeros de filas")
    print(len(Result_IW37nReporte))
    
    #merge con condicional
    df_vacios = Result_IW37nReporte[Result_IW37nReporte['Area'].isnull() | Result_IW37nReporte['SubArea'].isnull()]
    print("df vacios cantidad de filas")
    print(len(df_vacios))

    #Quito duplicados de concat - Artificio para elegir el primero
    result_unico = Result_Condiciones.drop_duplicates(subset='Concat', keep='first')

    df_actualizar = pd.merge(df_vacios,result_unico[['Concat', 'Area', 'SubArea']],on='Concat', how='left',suffixes=('', '_nuevo'))

    print("df vacios con merge")
    print(len(df_vacios))
    df_actualizar['Area'] = df_actualizar.apply(lambda row: row['Area_nuevo'] if pd.isnull(row['Area']) else row['Area'], axis=1)
    df_actualizar['SubArea'] = df_actualizar.apply(lambda row: row['SubArea_nuevo'] if pd.isnull(row['SubArea']) else row['SubArea'], axis=1)
    df_actualizar = df_actualizar.drop(columns=['Area_nuevo', 'SubArea_nuevo'])

    #df_actualizar = df_actualizar.drop_duplicates()
    print("df actualizar con merge y sin duplicados")
    print(len(df_actualizar))

    #Elimo en la principal los Area y subarea vacios
    Result_IW37nReporte = Result_IW37nReporte.dropna(subset=['Area', 'SubArea'])

    #Anido ambos data frames
    Result_IW37nReporte = pd.concat([Result_IW37nReporte, df_actualizar])
    Result_IW37nReporte = Result_IW37nReporte.drop_duplicates()
    print("Cantidad despues de anidar")
    print(len(Result_IW37nReporte))
    #--------
    
    #Columna TrabajoHoras
    Result_IW37nReporte['TrabajoHoras'] = pd.to_numeric(Result_IW37nReporte[' Trabajo'], errors='coerce').fillna(0) #.astype(int)

    #Categoria Backlog
    statusu_extract = Result_IW37nReporte['StatUsu'].str[:4]
    conditions = [
        (statusu_extract == 'EJEC'),
        (statusu_extract == 'ESPL'),
        (statusu_extract == 'PLAN'),
        (statusu_extract == 'PLOK'),
        (statusu_extract == 'PROG'),
        (statusu_extract == 'REPR'),
    ]
    choices = ['-', 'Planificación', 'Planificación', 'Programación', 'Programación','Programación']
    Result_IW37nReporte['CategoriaBacklog'] = np.select(conditions, choices, default='-')

    #Quitando filas con categoria backlog = -
    Result_IW37nReporte = Result_IW37nReporte[Result_IW37nReporte['CategoriaBacklog'] != '-']
    print("Cantidad desppues de eliminar ----")
    print(len(Result_IW37nReporte))

    #Columna Categoria Puesto de trabajo / Empresa
    Result_IW37nReporte  = pd.merge(Result_IW37nReporte,Result_Condiciones[['PtoTrbRes','Empresa']],left_on='PtoTrbRes',right_on='PtoTrbRes',how='left')

    #Columna Area Responsable
    Result_IW37nReporte = pd.merge(Result_IW37nReporte,Result_Condiciones[['PtoTrbRes','AreaResponsable']],left_on='PtoTrbRes',right_on='PtoTrbRes',how='left')
    print("Cantidad despues de sacar el area responsable")
    print(len(Result_IW37nReporte))

    #Agrupamos OT por semana y op
    Result_IW37nReporte['Trabajo_sum'] = Result_IW37nReporte.groupby(['Orden', 'Semana', 'Op.'])['TrabajoHoras'].transform('sum')

    #Sacamos el TAG de la OT
    #Result_IW37nReporte =pd.merge(Result_IW37nReporte,Result_IW37nBase[['Orden','CpoClasif']], on='Orden',how='left')
    #Result_IW37nReporte['TAG'] = pd.merge(Result_IW37nReporte,Result_IW37nBase[['Orden','CpoClasif']],left_on='Orden',right_on='Orden',how='left')['CpoClasif']
    Result_IW37nReporte.rename(columns={'CpoClasif':'TAG'}, inplace=True)
    print(Result_IW37nReporte.columns)
    print("-------------ddddd----------")
    print(len(Result_IW37nReporte))
    print(Result_IW37nReporte.columns)

    #CRITICIDAD
    print("Criticidad")

    #Eliminamos duplicados de criticidad
    print(len(Result_Criticidad))
    Result_Criticidad = Result_Criticidad.drop_duplicates(subset='TAG') # Hay duplicados en criticidad
    print(len(Result_Criticidad))

    #Sacamos la Criticidad en base al TAG y UT - Separo el df cuando no hay TAG
    df_TAG = Result_IW37nReporte[Result_IW37nReporte['TAG'].notna()].copy()
    print("Cantidad con TAG",len(df_TAG))
    df_UT = Result_IW37nReporte[(Result_IW37nReporte['TAG'].isna()) | (Result_IW37nReporte['TAG'] == '')].copy()
    print("Cantidad sin TAG",len(df_UT))

    #Sacamos criticidad en base al TAG
    df_TAG = pd.merge(df_TAG,Result_Criticidad[['TAG','Criticidad']],on='TAG',how='left')
    print("Cantidad con merge TAG",len(df_TAG))

    #Sacamos criticidad en base al UT
    df_UT = pd.merge(df_UT,Result_Criticidad[['UT','Criticidad']],left_on='Ubic.técn.',right_on='UT',how='left')
    print("Cantidad con merge UT",len(df_UT))

    #Unimos los dataframes
    Result_IW37nReporte = pd.concat([df_TAG,df_UT])

    #Result_IW37nReporte = pd.merge(Result_IW37nReporte,Result_Criticidad[['TAG','Criticidad']],left_on='TAG',right_on='TAG',how='left')
    print("df total final",len(Result_IW37nReporte))

    # function_return_Streaming(Result_IW37nReporte,df_result)
    # def generate():
    #     for chunk in df_result:
    #         yield from chunk
    # print("Finalizando el proceso de obtención y procesamiento de IW47 desde MongoDB")
    # return StreamingResponse(generate(), media_type='application/json')

    return Result_IW37nReporte

@router.get("/GetAndProcessCriticidadEquipos", tags=["Indicadores"])
async def Get_Process_Criticidad ():
    df_result = []  
    
    df_CriticidadEquipos = await Process_CriticidadEquipos()
    
    function_return_Streaming(df_CriticidadEquipos,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención y procesamiento de Criticidad desde MongoDB")
    return StreamingResponse(generate(), media_type='application/json')

async def Process_CriticidadEquipos ():

    All_Data_CriticidadEquipos = []
    print("Obteniendo datos de MongoDB Criticidad Equipos")
    CursorIndicadores = db.CriticidadEquipos.find({})
    
    print("Procesando los datos de MongoDB Criticidad Equipos")
    await id_to_string_process(CursorIndicadores,All_Data_CriticidadEquipos)
    df_CriticidadEquipos = pd.DataFrame(All_Data_CriticidadEquipos)
    
    return df_CriticidadEquipos



###################################################################################
#PLAN MENSUAL
###################################################################################

@router.get("/GetAndProcessPlanMensual", tags=["Indicadores"])
async def Get_Process_PlanMensual ():
    print("plan mensual")
    df_result = []  
    
    df_PlanMensual = await Process_PlanMensual()
    print("proceso 1")
    function_return_Streaming(df_PlanMensual,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención y procesamiento de PlanMensual desde MongoDB")
    return StreamingResponse(generate(), media_type='application/json')

# @router.get("/GetAndProcessiwbase", tags=["Indicadores"])
# async def Get_Process_iwbase ():
#     All_Data_IW37nBaseMensual = []
#     df_result = []  
#     current_date, Semana, Anho = get_current_datetime()
#     if type is None:
#         print("Obteniendo Mensual")
#         CursorIW37nBaseMensual = db.iw37nbaseMes.find({
#             "Semana": "18",
#             "Anho": str(Anho)
#         })
#     else:
#         print("Obteniendo anual")
#         CursorIW37nBaseMensual = db.iw37nbaseMes.find({
#             "Anho": str(Anho),
#         })
#     print("Procesando los datos de MongoDB IW37nBase")
#     await id_to_string_process(CursorIW37nBaseMensual,All_Data_IW37nBaseMensual)
#     df_IW37nBase = pd.DataFrame(All_Data_IW37nBaseMensual)
    
#     print("Procesando los datos de MongoDB IW37nBase")

#     function_return_Streaming(df_IW37nBase,df_result)
#     def generate():
#         for chunk in df_result:
#             yield from chunk
#     print("Finalizando el proceso de obtención y procesamiento de PlanMensual desde MongoDB")
#     return StreamingResponse(generate(), media_type='application/json')

async def Process_PlanMensual (type: Optional[str]=None):

    All_Data_IW37nBaseMensual = []
    All_Data_IW37nReport = []
    df_result = []  
    print("Obteniendo datos de MongoDB IW37nBase")
    current_date, Semana, Anho = get_current_datetime()
    if type is None:
        print("Obteniendo Mensual")
        CursorIW37nBaseMensual = db.iw37nbaseMes.find({
            "Semana": "18", #Semana del fin de mes
            "Anho": str(Anho)
        })
    else:
        print("Obteniendo anual")
        CursorIW37nBaseMensual = db.iw37nbaseMes.find({
            "Anho": str(Anho),
        })
    
    print("Procesando los datos de MongoDB IW37nBase")
    await id_to_string_process(CursorIW37nBaseMensual,All_Data_IW37nBaseMensual)
    df_IW37nBase = pd.DataFrame(All_Data_IW37nBaseMensual)
    print("Monto inicial")
    print(len(df_IW37nBase))

    # df_IW37nBase = await Process_IW37nBase()
    Result_IW37nReporte = await Process_IW37nReporte()
    Result_Condiciones = await Process_Condiciones_2()
    Result_IW39 = await Process_IW39 ()
    
    print("Siguiente paso N°01")
    Result_IW39 = Result_IW39[["Status del sistema","Orden-Semana"]]
    df_IW37nBase["Orden-Semana"] = df_IW37nBase["Orden"].astype(str) + "-" + df_IW37nBase["Semana"].astype(str)
    print(len(df_IW37nBase))

    print("Siguiente paso N°02")
    df_IW37nBase = pd.merge(df_IW37nBase, Result_IW39, on='Orden-Semana',how='left')
    print(len(df_IW37nBase))

    print("Siguiente paso N°03")
    df_IW37nBase.rename(columns={'Status del sistema':'Status Sistema Reportado'}, inplace=True)
    # Result_IW37nReporte = Result_IW37nReporte[["Trbjo real","StatUsu","Orden-Semana"]]
    print(len(df_IW37nBase))
    print("Quitando duplicados de la base")
    df_IW37nBase = df_IW37nBase.drop_duplicates(subset=['Orden'], keep='first')
    print(len(df_IW37nBase))
    print("Se realiza el merge....")
    df_IW37nBase = pd.merge(df_IW37nBase, Result_IW37nReporte, on='Orden-Semana',how='left')
    print("Despues del merge con i w37nrpeto")
    print(len(df_IW37nBase))
    print(df_IW37nBase.columns)
    #df_IW37nBase = df_IW37nBase.drop_duplicates(subset=['Orden_x'], keep='first')
    print("despues del eliminar duplicados")
    print(len(df_IW37nBase))

    print(df_IW37nBase.columns)

    print("Siguiente paso N°04")
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['PtoTrbRes','Denominacion', 'AreaResponsable', 'Empresa', 'TipoContrato']], on='PtoTrbRes',how='left')
    df_IW37nBase = df_IW37nBase.drop_duplicates()
    print(len(df_IW37nBase))
    # print(df_IW37nBase)
    print("Siguiente paso 05")
    df_IW37nBase['Status Sistema Reportado'] = df_IW37nBase['Status Sistema Reportado'].str[:9]
    print(len(df_IW37nBase))
    Result_Condiciones.rename(columns={'StatusSistema':'Status Sistema Reportado'}, inplace=True)
 
    #Result_Condiciones.rename(columns={'Ptotrabajo':'PtoTrbRes'}, inplace=True)

    print("Siguiente paso 06")
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['Status Sistema Reportado','StatusKPI']], on='Status Sistema Reportado',how='left')
    #df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['PtoTrbRes','Denominacion', 'AreaResponsable', 'Empresa', 'TipoContrato']], on='PtoTrbRes',how='left')
    print(len(df_IW37nBase))

    print("Siguiente paso 07")
    df_IW37nBase.rename(columns={'Orden_x':'Orden'}, inplace=True)
    df_IW37nBase["OTCerradas"] =  np.where(df_IW37nBase["StatusKPI"] == "Cerrado", df_IW37nBase["Orden"], 0) #StatusKPI si el valor es 0 es por que la OT esta abierta
    print(len(df_IW37nBase))

    print("Siguiente paso 08")
    df_IW37nBase = df_IW37nBase.drop_duplicates()
    print(len(df_IW37nBase))
    
    print("Siguiente paso 09 - Completa con - las ubicaciones tecnicas vacias")
    df_IW37nBase['Ubic.técn.'] = df_IW37nBase['Ubic.técn.'].fillna('-')
    
    #REVISAR
    # df_IW37nBase_vacio = df_IW37nBase[df_IW37nBase['Ubic.técn.'] == ''] No salia Oxxidos xq elimina los q no tenian ubi tecnica.
    # print(df_IW37nBase_vacio)

    #df_IW37nBase = df_IW37nBase[df_IW37nBase['Ubic.técn.'] != '']
    print("Eliminamos las filas de Mina")
    df_IW37nBase['Seccion'] = df_IW37nBase['Ubic.técn.'].apply(lambda x: x.split('-')[1][:3])
    df_IW37nBase = df_IW37nBase[df_IW37nBase['Seccion'] != 'MI1']
    df_IW37nBase = df_IW37nBase.drop(columns=['Seccion'])
    print("Cantidad despues de eliminar Mina:")
    print(len(df_IW37nBase))
    df_IW37nBase["UT"] = df_IW37nBase["Ubic.técn."].str[:13].str.strip()
   
    #Columna concatenacion larga
    df_IW37nBase["Concatenacion"] = df_IW37nBase["Ubic.técn."].str.cat(df_IW37nBase["PtoTrbRes"], sep="-")
    #Columna concatenacion nulos
    df_IW37nBase["Concat"] = df_IW37nBase["UT"].str.cat(df_IW37nBase["PtoTrbRes"], sep="-")
    #merge normal
    print("Siguiente paso 10")
    #Sacamos el área y subarea de la concatenación larga
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['Concatenacion','Area', 'SubArea']], on='Concatenacion',how='left')
    print("Quitamos duplicados despues del merge que busca el area y subarea de la concatenación larga")
    df_IW37nBase = df_IW37nBase.drop_duplicates()
    print(len(df_IW37nBase))
    #Identificamos el área y subarea que quedaron vacios por que no encontrò un match
    df_IW37nBase['Vacío'] = df_IW37nBase.apply(lambda row: 'Sí' if pd.isnull(row['Area']) and pd.isnull(row['SubArea']) else 'No', axis=1)
 
    #merge con condicional
    print("Empezamos el proceso de merge con condicional para concatenacion corta")
    df_vacios = df_IW37nBase[df_IW37nBase['Area'].isnull() | df_IW37nBase['SubArea'].isnull()] #Nuevo df con filas de area y subarea vacias
    print("df vacios cantidad de filas con area y subarea vacias")
    print(len(df_vacios))
    print("Siguiente paso 11")

    #Quito duplicados de concat
    result_unico = Result_Condiciones.drop_duplicates(subset='Concat', keep='first')
    print("Siguiente paso 12")
    #Realizo el merge con la concatenacion corta
    df_actualizar = pd.merge(df_vacios,result_unico[['Concat', 'Area', 'SubArea']],on='Concat', how='left',suffixes=('', '_nuevo'))
    cantidad_areas_vacias = df_actualizar['Area_nuevo'].isna().sum()
    print("Cantidad de filas con Area vacía despues del merge con la concatenacion corta:", cantidad_areas_vacias)

    #Coloca el nuevo valor de la concatenacion en las columnas area y subarea
    df_actualizar['Area'] = df_actualizar.apply(lambda row: row['Area_nuevo'] if pd.isnull(row['Area']) else row['Area'], axis=1)
    df_actualizar['SubArea'] = df_actualizar.apply(lambda row: row['SubArea_nuevo'] if pd.isnull(row['SubArea']) else row['SubArea'], axis=1)
    df_actualizar = df_actualizar.drop(columns=['Area_nuevo', 'SubArea_nuevo'])

    #df_actualizar = df_actualizar.drop_duplicates()
    print("df actualizar con los area y subarea actualizados deberia ser igual a df vacios cantidad de filas con area y subarea vacias")
    print(len(df_actualizar))

    #Elimo en la principal los Area y subarea vacios
    df_IW37nBase = df_IW37nBase.dropna(subset=['Area', 'SubArea'])
    print("Siguiente paso 13")

    #Anido ambos data frames
    df_IW37nBase = pd.concat([df_IW37nBase, df_actualizar])
    print("Cantidad final del dataframe, anidando el segundo dataframe:", len(df_IW37nBase))
    df_IW37nBase = df_IW37nBase.drop_duplicates()

    #Result_Condiciones.rename(columns={'Ptotrabajo':'PtoTrbRes'}, inplace=True)
    #df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['PtoTrbRes','Denominacion', 'AreaResponsable', 'Empresa', 'TipoContrato']], on='PtoTrbRes',how='left')
    
    print("VALIDARRRRRRRRRRRRRRRRR")
    print(len(df_IW37nBase))
    df_IW37nBase.rename(columns={"Semana_x":"Semana"}, inplace=True)
    df_IW37nBase.rename(columns={'Trbjo real':'Trabajo Real'}, inplace=True)
    df_IW37nBase.rename(columns={'StatUsu':'Status Usuario'}, inplace=True)
    df_IW37nBase.rename(columns={'Revisión_y':'Revisión'}, inplace=True)
    print(df_IW37nBase.columns)

    # function_return_Streaming(df_IW37nBase,df_result)
    # def generate():
    #     for chunk in df_result:
    #         yield from chunk
    # print("Finalizando el proceso de obtención y procesamiento de IW47 desde MongoDB")
    # return StreamingResponse(generate(), media_type='application/json')
    return df_IW37nBase
