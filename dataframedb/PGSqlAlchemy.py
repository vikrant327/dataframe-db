# SqlAlchemy - Pandas database module
from sqlalchemy import types, create_engine
from sqlalchemy.exc import InvalidRequestError 
import pyodbc
import sys
from sqlalchemy.types import Boolean, Date, DateTime, Float, Integer, Text, Time, Interval


DEFAULT_PYTHON_SQL_TYPE_MAPPING = {
    'bool': Boolean,
    'str': Text,
    'object':Text,
    'int': Integer,
    'float': Float,
    'datetime': DateTime,
    'date': Date,
    'time': Time,
    'timedelta': Interval
}


def dataTypesMap(dTypes):
    #df_postgres={'object':'VARCHAR(length=255','int64':'Integer()','float64':'Float()','bool':'Boolean()','datetime64':'DateTime()'}
    df_postgres={'object':types.VARCHAR(length=255),'int64':types.Integer(),'float64':types.Float(),'bool':types.Boolean(),'datetime64':types.DateTime()}
    table_dtype = {}
    for index,value in dTypes.items():
        table_dtype[index] = df_postgres[str(value)]
    return table_dtype
    

class PGSqlAlchemy:
    def __init__(self,config):
        self.dbserverType = config['serverType']
        self.dbserver = config['server']
        self.tablename = config["tablename"]
        self.port = config["port"]  if config["port"] else 5432
        self.dbname = config["dbname"]
        self.schema = config["schema"]
        self.username = config["username"]
        self.password = config["password"]
        self.dataframe = config["dataframe"]
        self.if_exist = config["mode"]

    def appendToExistingTable(self):
        db_string = None
        if self.dbserverType == "SqlServer":
            db_string = "mssql+pyodbc://{}:{}@{}:{}/{}?driver=ODBC+Driver+17+for+SQL+Server".format(self.username,self.password,self.dbserver,self.port,self.dbname)
        elif self.dbserverType == "pg":
            db_string = "postgres://{}:{}@{}:{}/{}".format(self.username,self.password,self.dbserver,self.port,self.dbname)
            
        if db_string is None:
            return "Database Type not providex, please include 'serverType' = 'SqlServer' or 'pg' in config"
        try :
            #db_string = "mssql+pyodbc://{}:{}@{}:{}/{}?driver=ODBC+Driver+17+for+SQL+Server".format(self.username,self.password,self.dbserver,self.port,self.dbname)
            db_engine = create_engine(db_string,echo=False)
            self.dataframe.to_sql(self.tablename,db_engine,if_exists='append',index=True,chunksize=500)
            return "{} Records Added Successfully to Table {}".format(self.dataframe.shape[0],self.tablename)
        except:
            return "Error Append Records to Database"

    def createNewTable(self):
        table_dtype = dataTypesMap(self.dataframe.dtypes)
        db_string = None
        if self.dbserverType == "SqlServer":
            db_string = "mssql+pyodbc://{}:{}@{}:{}/{}?driver=ODBC+Driver+17+for+SQL+Server".format(self.username,self.password,self.dbserver,self.port,self.dbname)
        elif self.dbserverType == "pg":
            db_string = "postgres://{}:{}@{}:{}/{}".format(self.username,self.password,self.dbserver,self.port,self.dbname)
            
        if db_string is None:
            return "Database Type not providex, please include 'serverType' = 'SqlServer' or 'pg' in config"
        
        try :
            db_engine = create_engine(db_string)
            self.dataframe.to_sql(self.tablename,db_engine,if_exists='replace',index=True,chunksize=500,dtype=table_dtype)
            return "Table Name {} with {} Created Successfully, {}".format(self.tablename,self.dataframe.shape[0],table_dtype)
        except OSError as err:
            return "Error Creating Database {0}".format(table_dtype)

    def syncDatabase(self):
        if self.if_exist == "replace":
            return self.createNewTable()
        else:
            return self.appendToExistingTable()
            
            
            



        
