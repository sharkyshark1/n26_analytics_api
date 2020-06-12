from pipeline import Pipeline
from flask import Flask, request, jsonify, render_template, send_file, Response
from flask_httpauth import HTTPBasicAuth
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from n26_api import N26Caller
from utils import Utils
from pipeline import Pipeline
from processing import TransactionProcessor
import dotenv
import os
import pandas
# from authorization import bp_auth

# start mongo db service
# brew services start mongodb-community@4.2

dotenv.load_dotenv(".env", verbose=True)

app = Flask(__name__)
auth = HTTPBasicAuth()
CORS(app)

users = {
    'tj': generate_password_hash('testedeinpasswort')
}

# app.register_blueprint(bp_auth)
app.secret_key = 'ljhkgjfhdfhjghkblnm'


@auth.verify_password
def verify_password(username, password):
    if username in users:
        return check_password_hash(users.get(username), password)
    return False


@app.route("/")
@auth.login_required
def hello():
    return "Hello, %s!" % auth.username()


@app.route("/balance", methods=['GET'])
@auth.login_required
def display_balance():
    balance = N26Caller().get_balance()
    return jsonify(balance)


@app.route("/recent_transactions", methods=['GET'])
@auth.login_required
def display_recent_transactions():
    time = Utils().get_current_timestamp() - 3*24*60*60*1000
    transactions = N26Caller().get_recent_transactions(start_time=time)
    return jsonify(transactions)


@app.route("/update_database", methods=['GET'])
@auth.login_required
def update_db():
    Pipeline().update_data()
    return jsonify('MongoDB is up-to-date!')


@app.route("/history", methods=['GET', 'POST'])
@auth.login_required
def get_history():
    df = TransactionProcessor().get_transactions_df()
    csv_string = df.to_csv(sep=';', encoding='UTF-8')
    return Response(
        csv_string,
        mimetype="text/csv",
        headers={"Content-disposition": "attachment; filename=transaction_history.csv"})


@app.route("/analytics", methods=['GET', 'POST'])
@auth.login_required
def get_analytics_pipeline():
    zip_file = Pipeline().run()
    return Response(zip_file,
                    mimetype='application/zip',
                    headers={'Content-Disposition': 'attachment;filename=analytics.zip'})


if __name__ == '__main__':
    app.run(host='0.0.0.0.', debug=True, port=8080)

