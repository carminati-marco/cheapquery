
### Setup

After creating a virtualenv, install the packages using
```
pip install -r requirements.txt
```
and launch flask with
```
python app.py
```

### Request

You can obtain the publisher data using the follow curl as example
```
curl --location --request POST 'http://127.0.0.1:5000/search/' \
--header 'Content-Type: application/json' \
--data-raw '{
    "publisher_domain_ids": [
        1705172,
        1552619,
        1701740,
        1701743,
        1571397,
        1599780
    ],
    "publisher_id": 74968,
    "from_transaction_date": 20201203,
    "to_transaction_date": 20221004
}'
```

