from flask.blueprints import Blueprint
from flask import render_template ,request,flash

home = Blueprint('home',__name__)

@home.route('/')
def index():
    flash('You were successfully logged in')
    print(request.args)
    return render_template('home/index.html')


if __name__ == "__main__":
    pass