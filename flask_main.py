from flask import Flask, render_template, url_for, request, make_response, session
import os
# os.environ["AWS_SHARED_CREDENTIALS_FILE"] = r"C:\Flask\s3DataSampler\creds\aws_credentials.csv"
import utils
from s3Synergy import S3Synergy

app = Flask(__name__)
app.config['SECRET_KEY']='089bcc9bf633533dd60b6f19402d1634'


@app.route('/')
def hello_world():
    return render_template('s3_synergy_home.html')

@app.route('/', methods=['GET','POST'])
def get_data():
    args = utils.generate_kwargs(request.form)
    df, scanned_bytes, processed_bytes = S3Synergy().readData(**args)
    #df = pd.read_csv(r"C:\Flask\s3DataSampler\sample\sample_csv.csv")
    path = os.path.dirname(os.path.realpath(__file__))+r'\tmp\test.csv'
    try:
        os.remove(path)
    except Exception:
        pass
    df.to_csv(path,index=False)
    session['op_file']=path
    bytes_data = '''<div class="card-body">
    Bytes Scanned:{scanned_bytes}
    Bytes Processed: {processed_bytes}</div>'''.format(scanned_bytes=scanned_bytes,processed_bytes=processed_bytes)
    strs=df.to_html(index=False)
    strs=strs.replace('<table border="1" class="dataframe">','<table class="table table-bordered table-hover table-sm">').replace('<thead>','<thead class="thead-dark">').replace('<th>','<th scope="col">').replace('<tr style="text-align: right;">','')
    return render_template('s3_synergy_home.html', data=strs, dropdown=utils.download_dropdown(), bytes_data=bytes_data)

@app.route('/download/<dtype>', methods=['GET','POST'])
def download_data(dtype):
    df = pd.read_csv(session['op_file'])
    if(dtype=='csv'):
        resp = make_response(df.to_csv(index=False))
        resp.headers["Content-Disposition"] = "attachment; filename=export.csv"
        resp.headers["Content-Type"] = "text/csv"
    elif(dtype=='json'):
        resp = make_response(df.to_json(orient='table',index=False))
        resp.headers["Content-Disposition"] = "attachment; filename=export.json"
        resp.headers["Content-Type"] = "text/json"
    elif(dtype=='excel'):
        '''To Be Implemented'''
        pass
        # resp = make_response(df.to_excel(pd.ExcelWriter('pandas_multiple.xlsx',engine ='xlsxwriter')))
        # resp.headers["Content-Disposition"] = "attachment; filename=export.xlsx"
        # resp.headers["Content-Type"] = "text/excel"
    return resp


if __name__=='__main__':
    import pandas as pd
    app.run(debug=True)