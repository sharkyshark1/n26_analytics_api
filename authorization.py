from functools import wraps
import random
import json
import sys

from flask import Blueprint, request, jsonify, session, Response, make_response, redirect
from authlib.jose import JWK
from authlib.jose import JWK_ALGORITHMS
from authlib.jose import jwt
import database

ALL_CHARS = 'ABCEDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890'


def my_random_string(k=16):
    return ''.join(random.choices(ALL_CHARS, k=k))


jwk=JWK(algorithms=JWK_ALGORITHMS)
public_key_obj = json.load(open('jwt/public.jwk'))
public_key = jwk.loads(public_key_obj)

bp_auth = Blueprint('auth', __name__)


def user_from_token():
    if 'Authorization' not in  request.headers:
        return None
    auth = request.headers['Authorization']
    if auth.startswith('Bearer '):
        print('Auth with Bearer')
        jwt_token = auth[7:]
        print('TOKEN', jwt_token)
        claims = jwt.decode(jwt_token, public_key)
        print('CLAIMS', claims)
        if 'sub' in claims:
            username = claims['sub']
            if 'new_new_sub' not in claims:
                print('No new_new_sub claim for', username, file=sys.stderr)
                return None
        return username
    else:
        print('Auth without Bearer')
        auth = request.authorization
        if auth and database.verify_user(auth.username, auth.password):
            return auth.username
    print('Return None')
    return None


@bp_auth.route('/auth/status')
def auth_status():
    print('status called')
    token_user = user_from_token()
    if token_user is not None:
        res = jsonify({
            'status': 'logged-in',
            'username': token_user
        })
        return res
    if 'username' in session:
        if 'xsrf-token' not in session:
            xsrf_token = my_random_string()
            session['xsrf-token'] = xsrf_token
        else:
            xsrf_token = session['xsrf-token']
        res = jsonify({
            'status': 'logged-in',
            'username': session['username']
        })
        res.set_cookie('XRSF-TOKEN', xsrf_token)
        return res
    else:
        return jsonify({
            'status': 'logged-out'
        })


@bp_auth.route('/auth/login', methods = ['POST'])
def auth_login():
    print('in login')
    obj = request.json
    print(obj)
    if not obj:
        return Response('Need to post a json', status=400)
    try:
        username = obj['username']
        password = obj['password']
    except KeyError:
        return Response('Need username and password', status=400)
    user_obj = database.verify_user(username, password)
    if not user_obj:
        return Response('Invalid user or password', status=401)
    session['username'] = username
    print(session)
    # generate XSRF token, insert into session, add cookie
    xsrf_token = my_random_string()
    session['xsrf-token'] = xsrf_token
    res = jsonify({
        'status': 'logged-on',
        'username': username
    })
    res.set_cookie('XSRF-TOKEN', xsrf_token)
    print(res)
    return res


@bp_auth.route('/auth/logout', methods = ['POST'])
def auth_logout():
    try:

        del session['username']
    except KeyError as e:
        print(e)
    try:
        del session['xsrf-token']
    except KeyError as e:
        print(e)
    return jsonify({
        'status': 'logged-out'
    })

def protected(f):
    @wraps(f)
    def do_stuff(*args, **kwargs):
        # if jwt, do not check xrsf header
        token_user = user_from_token()
        if token_user is not None:
            return f(*args, **kwargs)
        elif 'username' in session:
            if request.method not in ['GET', 'HEAD'] and 'xsrf-token' in session:
                request_token = request.headers.get('X-XSRF-TOKEN')
                if request_token != session['xsrf-token']:
                    return Response('XSRF Token does not match')
            return f(*args, **kwargs)
        else:
            return Response('Need to be logged in', status=401)
    return do_stuff()


def ensure_xsrf_token(f):
    @wraps(f)
    def do_stuff(*args, **kwargs):
        if 'xsrf-token' not in session:
            session['xsrf-token'] = my_random_string()
        xsrf_token = session['xsrf-token']
        result = f(*args, **kwargs)
        result.set_cookie('XSRF-TOKEN', xsrf_token)
        return result
    return do_stuff

