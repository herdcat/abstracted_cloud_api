from boto3 import client

class GlueConnector ():
    def __init__(self,sec_token, region):
        self.client = client('glue', region)

    def getDatabases(self, nextToken=''):
        dbList = self.client.get_databases(NextToken=nextToken)['DatabaseList']
        return [x['Name'] for x in dbList]

    def getDatabase(self, dbName):
        return self.client.get_database(Name=dbName)['Database']

    def getTables(self, dbName, nextToken=''):
        tblList = self.client.get_tables(DatabaseName=dbName, NextToken=nextToken)['TableList']
        return [{'Name':x['Name'],'DatabaseName':x['DatabaseName'],'Owner':x['Owner']} for x in tblList]    

    def getTable(self, dbName, tblName):
        return self.client.get_table(Name=tblName, DatabaseName=dbName)['Table']       