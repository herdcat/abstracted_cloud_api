from flask import Flask, Response
from flask_cors import CORS
from blueprints.catalog import catalog
from blueprints.database import database
from blueprints.datalake import datalake

app = Flask(__name__)
CORS(app)
app.register_blueprint(catalog)
app.register_blueprint(database)
app.register_blueprint(datalake)

@app.route("/")
def index():
    return Response('', 200)