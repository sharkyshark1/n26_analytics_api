from pipeline import Pipeline
from flask import Flask, request, jsonify
from flask_httpauth import HTTPBasicAuth
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from n26_api import N26Caller
from utils import Utils
# import dotenv
import os
# from authorization import bp_auth

# doten.load_dotenv(".env", verbose=True)

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
def display_transaction():
    balance = N26Caller().get_balance()
    return "Current balance, %s!" % str(balance)


@app.route("/transactions/recent")
@auth.login_required
def display_recent_transactions():
    time = Utils().get_current_timestamp() - 3*24*60*60*1000
    transactions = N26Caller().get_recent_transactions(start_time=time)
    return str(transactions)


@app.route("/test", methods=['POST'])
@auth.login_required
def display_test():
    test_input = request.json.get('test_input')
    print(test_input)
    return test_input


@app.route("/test1", methods=['GET', 'POST'])
@auth.login_required
def display_test1():
    test_input = request.args.get('test_input')
    print(test_input)
    return {'tj': test_input}
# http://0.0.0.0:8080/test?test_input=ilovemyselfbaby


if __name__ == '__main__':
    app.run(host='0.0.0.0.', debug=True, port=8080)

