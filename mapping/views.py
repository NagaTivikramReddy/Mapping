from django.shortcuts import render
from django.views.generic import TemplateView, ListView
import pandas as pd
from datetime import datetime
import numpy as np
from pathlib import Path
import pyodbc

def result(request):
    if request.method == 'POST':
        file = request.FILES['myfile']
        server = request.POST.get('server')
        database = request.POST.get('database')
        username = request.POST.get('username')
        password = request.POST.get('password')
        print(file)
        print(type(file))
        table = pd.read_excel(file)

        sheet_name = "DimCustomer"
        
        def stored_proc_dim():
        
            table_name = (table.loc[(1),('MetaDataValue')])
            table_type = table.loc[(0),('MetaDataValue')]
            
            primary_key = table.loc[table['PK'] == 1.0, ('ColumnName')]
            sarrogate_key = table.loc[table['SK'] == 1.0, ('ColumnName')]

            table['ColumnName'].fillna('', inplace=True)
            q = table.loc[table['DWHC'] != 1.0, ('ColumnName',)]

            columns = []
            for column in q['ColumnName']:
                if column != "":
                    column = "[" + column +"]"+","
                    columns.append(column)

            columns_ = "\n".join( i for i in columns)
            
            sourcedotcols = [] 
            for ele in columns:
                sourcedotcols.append("Source."+ele)

            sourcecolumns = "\n".join(i for i in sourcedotcols)

            targetdotcols = [] 
            for ele in columns:
                targetdotcols.append("Target."+ele)

            targetcolumns = "\n".join(i for i in targetdotcols)

            sourcetarget = []
            for i in range(len(columns)):
                sourcetarget.append(targetdotcols[i].rstrip(",")+"="+sourcedotcols[i])

            sourcetargetequal = "\n".join(i for i in sourcetarget)

            values_ = "'',\n'',\n'',\n'',\n'',\n'',\n0,\n'',\n'',\n'',\n'',\n'',\n'',\n'',\n0,\n'',\n'',\n0,\n0,\n'',\n'',\n'',\n0,\n'',\n'',\n'',\n'',\n'',\n'',\n'',\n'',\n'',\n'',\n'',\n'',\n'',\n0,\n0,\n'',\n'',\n0,\n0,\n0,\n0,\n0,\n0,\n'',\n'',\n''"
            
            # reading source query column from table where 'DWHC != 1'
            table['DWHC'].fillna('', inplace=True)
            table['ColumnName'].fillna('', inplace=True)
            table['DataType'].fillna('', inplace=True)
            qry = table.loc[table['DWHC'] != 1.0, ('ColumnName', 'DataType', 'Source Query')]
            qry_list = list()
            for n, p, q in zip(qry['ColumnName'], qry['DataType'], qry['Source Query']):
                if p.startswith(('int','bit','bigint','tinyint','smallint','float','decimal','numeric','money','real')):
                    temp_lst = q + " AS ["+ n +"]"+ ","
                    qry_list.append(temp_lst)
                elif p.startswith(('nvarchar','Nvarchar','varchar','char','nchar','text')):   
                    temp_lst = q + " AS [" + n +"]"+ "," 
                    qry_list.append(temp_lst)
                elif p.startswith(('datetime','date')):   
                    temp_lst = q + " AS ["+ n +"]"+ "," 
                    qry_list.append(temp_lst)
            query = qry_list

            query_= '\n'.join([str(elem) for elem in query])

            alias_qry = query_.rstrip(',')


            qry_listt = list()
            for p, q in zip(qry['DataType'], qry['Source Query']):
                if not p:
                    temp_lstt = q
                    qry_listt.append(temp_lstt)
            queryy = qry_listt
            alias_qryy = '\n'.join([str(elem) for elem in queryy])



            alias_query = alias_qry + '\n' + alias_qryy
            
            stored_proc_script = ("CREATE  PROCEDURE [dbo].[sp_dimClinicsActual_Mapping_16_3_2022_2]" + "\n" +"\n"

            "@RowCount INT = 0 OUTPUT" + "\n" +"\n"

            "AS"+ "\n" +"\n"

            "-- Connector : TCM NA"+ "\n"
            "-- Purpose   : update target table [rebex DW].[dbo].[dimClinics]"+ "\n" +"\n"

            "-- Load Type = MERGE"+ "\n"
            "-- SCD Type  = 1"+ "\n" +"\n"

            "BEGIN"+ "\n" +"\n"

            "SET NOCOUNT ON;"+ "\n" +"\n"

            "-- SELECT RECORDS"+ "\n" +"\n"
                    
            "SELECT "+"\n" +alias_qry+"\n" +"\n"
            "INTO #"+table_type+table_name + "\n"+"\n"
            ""+alias_qryy +"\n"+ ";" +"\n"+"\n"
            "SELECT @RowCount = @@ROWCOUNT;" +"\n"+"\n"
        
            "-- SCD1 LOAD"  +"\n"
            "MERGE INTO [rebex DW].[dbo].["+table_type+table_name+"] AS Target" +"\n"
            "USING #"+table_type+table_name+" AS Source" +"\n"
            "ON Target.["+primary_key[:1]+"] = Source.["+primary_key[:1]+"]" +"\n"
            "AND Target.[rebex_Source] = Source.[rebex_Source]" +"\n"+"\n"

            "WHEN NOT MATCHED BY TARGET THEN" +"\n"+"\n"

            "INSERT (" +"\n"
            ""+columns_.rstrip(",") +"\n"+ ")"+ "\n"
            "VALUES (" + "\n"+sourcecolumns.rstrip(",") +"\n"+")" + "\n"+"\n"
            "WHEN MATCHED THEN"+"\n"+"\n"
            "UPDATE SET"+"\n"+ sourcetargetequal.rstrip(",")+";"+"\n"+"\n"
            "DROP TABLE #"+table_type+table_name+"\n"+"\n"
            "-- INSERT UNKNOWN MEMBER RECORD"+"\n"+"\n"

            "DELETE FROM [rebex DW].[dbo].["+table_type+table_name+"]"+"\n"
            "WHERE [ClinicID] = 0"+"\n"+"\n"
                    
            "SET IDENTITY_INSERT [rebex DW].[dbo].[dimClinics] ON"+"\n"+"\n"
                    
            "INSERT INTO [rebex DW].[dbo].["+table_type+table_name+"] ("+"\n"
            "[ClinicID],"+"\n"+columns_.rstrip(",")+"\n"
            ")"+"\n"
            "VALUES"+"("+"\n"+"0,"+"\n"+values_+"\n"+")" + "\n"+"\n"
            "SET IDENTITY_INSERT [rebex DW].[dbo].["+table_type+table_name+"] OFF ;" + "\n"+"\n"+"\n"+"END") 
            
          
            return stored_proc_script

        sp = stored_proc_dim()

        with open('C:/Users/VIKRAM/OneDrive/Documents/Sonata work files/Work folder/Django_created_sps/Stored_Procedure_16_3_2022.sql','w') as file:
            for i in sp:
                file.write(i)
        # C:\Users\VIKRAM\OneDrive\Documents\Sonata work files\Work folder\Django_created_sps

        with open('C:/Users/VIKRAM/OneDrive/Documents/Sonata work files/Work folder/Django_created_sps/Stored_Procedure_16_3_2022.sql') as file:
            stored_proc = file.read()

        # server = ".\sqlexpress"
        # database = "master"

        # connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';Trusted_Connection=yes;DATABASE={'+database+'};')

        connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';UID='+username+';PWD='+ password + ';DATABASE='+database)

        connection.autocommit = True
        cursor = connection.cursor()

        with open('C:/Users/VIKRAM/OneDrive/Documents/Sonata work files/Work folder/Django_created_sps/Stored_Procedure_16_3_2022.sql') as readfile:
            cursor.execute(readfile.read())

        return render(request,"result.html",{'status':True,"stored_proc":stored_proc})

    else:
        return render(request,"result.html")


def upload(request):
    return render(request,"upload.html")


