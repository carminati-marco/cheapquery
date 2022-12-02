from flask import Flask
app = Flask(__name__)
from flask import request
from arrow_v1 import search as arrow_search, APP_CACHE


app_cache = APP_CACHE()

@app.route('/search/', methods=['GET'])
def search():
    data = request.args
    r = arrow_search(data["publisher_id"], data["start_date"],
                     data["end_date"], data.get("publisher_domain_ids"), app_cache=app_cache)
    return {"commissions": r.to_dict(orient='records')}

@app.route('/alive/', methods=['GET', 'POST'])
def alive():
    return "service is up"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)