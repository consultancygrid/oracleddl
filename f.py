import cx_Oracle
import os,sys
import subprocess
import glob

# path to local oracle client installation
lib_dir = r"C:\tools\oracle_client\instantclient_21_3"

try:
    cx_Oracle.init_oracle_client(lib_dir=lib_dir)
except Exception as err:
    print("Error connecting: cx_Oracle.init_oracle_client()")
    print(err);
    sys.exit(1);
    
    
try:
 def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
  if defaultType == cx_Oracle.CLOB:
   return cursor.var(cx_Oracle.LONG_STRING, arraysize = cursor.arraysize)
  elif defaultType == cx_Oracle.BLOB:
   return cursor.var(cx_Oracle.LONG_BINARY, arraysize = cursor.arraysize)


 conn = cx_Oracle.connect('USERNAME/PASSWORD@TNS_ALIAS')
 conn.outputtypehandler = OutputTypeHandler
 print (conn.version)
 cursor = conn.cursor()
 cursor.execute(""" begin DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'SQLTERMINATOR', true); end;""")
 cursor.execute(""" begin DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'PRETTY', true); end;""")
 cursor.execute(""" begin DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'SEGMENT_ATTRIBUTES', TRUE); end;""")
 cursor.execute(""" begin DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'STORAGE', false); end;""")
 cur = conn.cursor()
 sql="""
select 'TABLE' as object_type , owner as owner ,table_name as object_name,dbms_metadata.get_ddl('TABLE', table_name, owner) as col, 'sql' as ext from all_Tables where owner =:bind union all 
select 'SEQUENCE' as object_type , SEQUENCE_OWNER as owner,SEQUENCE_name as object_name,dbms_metadata.get_ddl('SEQUENCE', SEQUENCE_name, SEQUENCE_OWNER) as col , 'sql' as ext  from all_SEQUENCEs where SEQUENCE_OWNER =:bind union all 
select 'PACKAGE_BODY' as object_type , OWNER as owner,object_name as object_name ,dbms_metadata.get_ddl('PACKAGE_BODY', object_name, OWNER) as col, 'sql' as ext  from ALL_PROCEDURES where object_type='PACKAGE' and  owner =:bind union all 
select 'PACKAGE_SPEC' as object_type , OWNER as owner,object_name as object_name ,dbms_metadata.get_ddl('PACKAGE_SPEC', object_name, OWNER) as col, 'sql' as ext from ALL_PROCEDURES where object_type='PACKAGE' and  owner =:bind union all 
select 'PROCEDURE' as object_type , OWNER as owner ,object_name as object_name,dbms_metadata.get_ddl('PROCEDURE', object_name, OWNER) as col, 'sql' as ext  from ALL_PROCEDURES where object_type='PROCEDURE' and  owner =:bind union all 
select 'FUNCTION' as object_type , OWNER as owner,object_name as object_name ,dbms_metadata.get_ddl('FUNCTION', object_name, OWNER) as col, 'sql' as ext  from ALL_PROCEDURES where object_type='FUNCTION' and  owner =:bind union all 
select 'TYPE' as object_type , OWNER as owner,object_name as object_name ,dbms_metadata.get_ddl('TYPE', object_name, OWNER) as col, 'sql' as ext  from ALL_PROCEDURES where object_type='TYPE' and  owner =:bind union all 
select 'VIEW' as object_type , owner as owner,view_name as object_name ,dbms_metadata.get_ddl('VIEW', view_name, owner) as col, 'sql' as ext  from all_views where owner =:bind union all 
select 'SYNONYM' as object_type,owner as owner,synonym_name as object_name ,dbms_metadata.get_ddl('SYNONYM', synonym_name, owner) as col, 'sql' as ext  from all_synonyms where owner ='PUBLIC' and table_owner=:bind union all 
select 'SYNONYM' as object_type,owner as owner,synonym_name as object_name ,dbms_metadata.get_ddl('SYNONYM', synonym_name, owner) as col, 'sql' as ext  from all_synonyms where owner=:bind union all 
select 'MATERIALIZED_VIEW' as object_type,owner as owner ,mview_name as object_name,dbms_metadata.get_ddl('MATERIALIZED_VIEW', mview_name, owner) as col, 'sql' as ext  from ALL_MVIEWS where owner=:bind union all 
select 'TRIGGER' as object_type,owner as owner,trigger_name as object_name ,dbms_metadata.get_ddl('TRIGGER', trigger_name, owner) as col, 'sql' as ext  from ALL_triggers where owner=:bind union all 
select 'INDEX' as object_type,owner as owner,INDEX_name as object_name ,dbms_metadata.get_ddl('INDEX', INDEX_name, owner) as col , 'sql' as ext  from all_indexes where owner=:bind union all 
select 'TYPE_SPEC' as object_type,owner as owner,TYPE_name as object_name ,dbms_metadata.get_ddl('TYPE_SPEC', TYPE_name, owner) as col, 'sql' as ext   from all_types where owner=:bind union all 
select 'TYPE_BODY' as object_type,owner as owner ,TYPE_name as object_name,dbms_metadata.get_ddl('TYPE_BODY', TYPE_name, owner) as col, 'sql' as ext   from all_types where owner=:bind and methods>0 union all
select 'GRANTS' as object_type,:bind as owner ,table_name as object_name, to_clob(listagg('GRANT  '||privilege||' ON '||table_schema||'.'||table_name||' To '||grantee,';'||chr(10))  within group (order by 1 ))  as col, 'sql' as ext   from all_Tab_privs t  where table_schema=:bind  group by table_schema,table_name
order by 1,2,3
"""
 cur.prepare(sql)
 cur.execute(sql,{'bind' : sys.argv[1].upper()})
 res = cur.fetchall()
# res = cur.fetchmany(100000)
 for r in res:
  print (r[0], sys.argv[1].upper() ,r[2])
  if not os.path.exists(sys.argv[1].upper()):
   os.makedirs(sys.argv[1].upper())
  if not os.path.exists(sys.argv[1].upper() + '/' + r[0]):
   os.makedirs(sys.argv[1].upper() + '/' + r[0])
  with open( sys.argv[1].upper() + '/' + r[0] + '/' + r[2] +'.'+ r[4], 'a') as f:
#        with open( r[1] + '/' + r[0] + '/' + r[1] + '.' + r[2] +'.sql', 'w') as f:
#   print >> f, '', r[3]
            f.write(r[3].strip())
            if r[4].lower() == 'rgr':
                f.write("\n")
  f.close()
 #print res

 #for result in cur:
 #    print result
 cur.close()
 conn.close()
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise
