
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
curl --location --request GET 'http://127.0.0.1:5000/search?publisher_id=74968&start_date=2022-05-01&end_date=2022-11-30'
```

