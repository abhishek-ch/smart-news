from flask import Flask, render_template, request, json
from flask.ext.pymongo import PyMongo
import requests
import processing
import manual_process

# from flask.ext.mongoengine import MongoEngine
from flask_wtf import Form
from wtforms.fields.html5 import DateField

app = Flask(__name__)
# app.config["MONGODB_SETTINGS"] = {'DB': "news"}
# app.config["SECRET_KEY"] = "abhishekabc"
app.secret_key = 'abhishekabc!'
# mongo = PyMongo(app)
manual_fetch = manual_process.FetchValue()
# https://docs.mongodb.org/ecosystem/tutorial/write-a-tumblelog-application-with-flask-mongoengine/
# https://flask-pymongo.readthedocs.org/en/latest/
# http://code.tutsplus.com/tutorials/creating-a-web-app-from-scratch-using-python-flask-and-mysql--cms-22972
# https://docs.mongodb.org/ecosystem/tutorial/write-a-tumblelog-application-with-flask-mongoengine/



@app.route('/showProcess')
def showSignUp():
    return render_template('process.html')


@app.route('/', methods=[ 'GET','POST'])
def entryPage():
    errors = []
    results = {}
    if request.method == "POST":
        # get url that the user has entered
        try:
            datepicker = request.form['datepicker']
            processing.process(datepicker,False)
            #r = requests.get(datepicker)
            #print(r.text)
        except:
            errors.append(
                "Unable to get URL. Please make sure it's valid and try again."
            )
            print("UNAVBLELE")
    return render_template('index.html', errors=errors, results=results)


def rreplace(s, old, new, occurrence):
    li = s.rsplit(old, occurrence)
    return new.join(li)


@app.route('/manual.html', methods=[ 'GET','POST'])
def manualPage():
    errors = []
    results = {}
    title = 'Kukki'
    time = ''
    navIndex = ''
    if request.method == "POST":
        try:
            datepicker = request.form['thedate']
            thefetch = request.form['thefetch']
            if thefetch == 'bakwas':
                time = datepicker
            else:
                if thefetch[len(thefetch)-1] == '/':
                    time = thefetch[0:len(thefetch)-1]
                else:
                    time = thefetch

            try:
                theinput = str(request.form['theinput'])
                theinput = rreplace(theinput,'/','',1)
                theinput = int(theinput)
            except ValueError,e:
                print("Error @ manual dataprocessor str to int conversion ",e)
                theinput = 0

            if request.form.get('submit') == 'swipeleft':
                print "first part ",theinput
                if theinput > 0:
                    theinput -= 1

            elif request.form.get('submit') == 'swiperight':
                theinput += 1

            # calls the mongodb to fetch data from Database
            text_to_verify = manual_fetch.get_next_value(time,theinput)

            # if value return is not null means we can send back the result to be displayed
            if text_to_verify != -1:
                return render_template('manual.html', errors=errors, results=text_to_verify,title='Found it',navIndex=str(theinput),data=text_to_verify['data'],time=time)



        except Exception,e:
            errors.append("Bad Date, try Again!")
            print("Error @ manualPage Post ",e)

    return render_template('manual.html', errors=errors, results=results,title=title,navIndex=navIndex,time=time)



@app.route('/_add_numbers')
def add_numbers():
    a = request.args.get('a', 0, type=int)
    b = request.args.get('b', 0, type=int)


# http://code.tutsplus.com/tutorials/creating-a-web-app-from-scratch-using-python-flask-and-mysql-part-4--cms-23187
# https://github.com/jay3dec/PythonFlaskMySQLApp_Part4 - follow
#https://realpython.com/blog/python/flask-by-example-part-3-text-processing-with-requests-beautifulsoup-nltk/  - Follow for heroku upload

@app.route('/process', methods=['POST','GET'])
def process():
    try:
        if request.method == 'POST':
            _text = request.form['text']
            # _date = request.form['selectedDate']

            return render_template('process.html')
        else:
            _text = request.form['text']
            # _date = request.form['selectedDate']

            return render_template('process.html')

    except Exception as e:
        return json.dumps({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True)
