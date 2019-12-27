from flask import Blueprint, Response
from lib.aws.glue_connector import GlueConnector

catalog = Blueprint('catalog', __name__, url_prefix='/catalog')

@catalog.route("/get_databases")
def get_databases():
    glue = GlueConnector('token_chamgeme_later', 'us-east-1')
    db_list = glue.getDatabases()
    return Response(db_list.__str__(), 200)

@catalog.route("/get_database/<db_name>")
def get_database(db_name):
    glue = GlueConnector('token_chamgeme_later', 'us-east-1')
    db_list = glue.getDatabase(db_name)
    return Response(db_list.__str__(), 200)

@catalog.route("/get_database_tables/<db_name>")
def get_database_tables(db_name):
    glue = GlueConnector('token_chamgeme_later', 'us-east-1')
    tbl_list = glue.getTables(db_name)
    return Response(tbl_list.__str__(), 200)

@catalog.route("/get_database_table/<db_name>/<tbl_name>")
def get_database_table(db_name, tbl_name):
    glue = GlueConnector('token_chamgeme_later', 'us-east-1')
    tbl_data = glue.getTable(dbName=db_name, tblName=tbl_name)
    return Response(tbl_data.__str__(), 200)