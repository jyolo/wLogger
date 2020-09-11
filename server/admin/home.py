from flask.blueprints import Blueprint
from flask import render_template ,request,flash,session

home = Blueprint('home',__name__)

@home.route('/',methods=['GET','POST'])
def index():
    # flash('You were successfully logged in asdasd')
    if(request.method == 'GET'):
        print(request.args.get('var1'))
        return render_template('home/index.html')

    if(request.method == 'POST'):
        var1 = request.form['var1']
        var2 = request.form['var2']
        print(request.args )
        return {
            'var1':var1,
            'var2':var2
        }


if __name__ == "__main__":
    pass