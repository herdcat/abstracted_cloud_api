from flask import Blueprint, Response
from lib.aws.athena_connector import AthenaConnector

database = Blueprint('database', __name__, url_prefix='/database')

@database.route("/create_query")
def create_query():
    athena = AthenaConnector('token_chamgeme_later', 'us-east-1')
    query = athena.createQuery()
    return Response(query.__str__(), 200)

@database.route("/retreive_queries")
def retreive_queries():
    athena = AthenaConnector('token_chamgeme_later', 'us-east-1')
    query_list = athena.retreiveQueries()
    return Response(query_list.__str__(), 200)

@database.route("/retreive_query/<query_id>")
def retreive_query(query_id):
    athena = AthenaConnector('token_chamgeme_later', 'us-east-1')
    query = athena.retreiveQuery(query_id)
    return Response(query.__str__(), 200)