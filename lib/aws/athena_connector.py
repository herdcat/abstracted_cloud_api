from boto3 import client

class AthenaConnector ():
    def __init__(self,sec_token, region):
        self.client = client('athena', region)

    def createQuery(self, queryName, queryString, dbName):
        return self.client.create_named_query(Name=queryName,QueryString=queryString, Database=dbName)['NamedQueryId']

    def retreiveQuery(self, queryId, nextToken=''):
        return self.client.get_named_query(NamedQueryId=queryId)['NamedQuery']

    def retreiveQueries(self, workgroup=None, nextToken=None):
        return self.client.list_named_queries()['NamedQueryIds']