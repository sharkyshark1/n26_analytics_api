import dotenv
import os
from flask import Flask, jsonify, Response
from flask_httpauth import HTTPBasicAuth
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from n26_api import N26Caller
from utils import Utils
from pipeline import Pipeline
from processing import TransactionProcessor

# start mongo db service before starting the flask app
# brew services start mongodb-community@4.2
# python3 flask_app.py

dotenv.load_dotenv(".env", verbose=True)

app = Flask(__name__)
auth = HTTPBasicAuth()
CORS(app)

users = {
    os.getenv('APP_USER'): generate_password_hash(os.getenv('APP_PASSWORD'))
}


@auth.verify_password
def verify_password(username, password):
    if username in users:
        return check_password_hash(users.get(username), password)
    return False


@app.route("/")
@auth.login_required
def hello():
    return "Hello, %s!" % auth.username(), 201


@app.errorhandler(404)
def own_404_page(error):
    info = "Route does not exist --- Available routes: /balance, /recent_transactions, /update_db, /history, /analytics"
    return jsonify(info), 404


@app.route("/balance", methods=['GET'])
@auth.login_required
def display_balance():
    balance = N26Caller().get_balance()
    return jsonify(balance), 201


@app.route("/recent_transactions", methods=['GET'])
@auth.login_required
def display_recent_transactions():
    time = Utils().get_current_timestamp() - 3*24*60*60*1000
    transactions = N26Caller().get_recent_transactions(start_time=time)
    return jsonify(transactions), 201


@app.route("/update_db", methods=['GET'])
@auth.login_required
def update_db():
    Pipeline().update_data()
    return jsonify('MongoDB is up-to-date!'), 201


@app.route("/history", methods=['GET', 'POST'])
@auth.login_required
def get_history():
    df = TransactionProcessor().get_transactions_df()
    csv_string = df.to_csv(sep=';', encoding='UTF-8')
    return Response(
        csv_string,
        mimetype="text/csv",
        headers={"Content-disposition": "attachment; filename=transaction_history.csv"}), 201


@app.route("/analytics", methods=['GET', 'POST'])
@auth.login_required
def get_analytics_pipeline():
    zip_file = Pipeline().run()
    return Response(zip_file,
                    mimetype='application/zip',
                    headers={'Content-Disposition': 'attachment;filename=analytics.zip'}), 201


if __name__ == '__main__':
    app.run(host='0.0.0.0.', debug=True, port=8080)

