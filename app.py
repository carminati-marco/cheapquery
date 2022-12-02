from flask import Flask
app = Flask(__name__)
from flask import request
from arrow_v1 import search as arrow_search

@app.route('/search/', methods=['POST'])
def search():
    data = request.get_json()
    r = arrow_search(data["publisher_id"], data["from_transaction_date"],
                     data["to_transaction_date"], data["publisher_domain_ids"])
    return r.to_json()

@app.route('/alive/', methods=['GET', 'POST'])
def alive():
    return "service is up"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)