from flask import Flask
from environs import Env
import os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from server.admin.user import user
from server.admin.home import home





app = Flask(__name__)
app.register_blueprint(home,url_prefix='/')
app.register_blueprint(user,url_prefix='/user')

# start this server from the main.py
def start_server():
    env_file = os.path.dirname(os.path.abspath(__file__)) + '/.env'
    env = Env()
    env.read_env(path=env_file)

    if(not os.path.exists(env_file)):
        raise FileNotFoundError('webserver .env file not found')

    app.run(
        host = env.str('HOST'),
        port = env.str('PORT'),
    )



if __name__ == "__main__":
    start_server()
