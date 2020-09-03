from flask.blueprints import Blueprint
from flask import render_template,flash

user = Blueprint('user',__name__)

@user.route('/index')
def index():

    print('34343434')
    return render_template('user/index.html')



