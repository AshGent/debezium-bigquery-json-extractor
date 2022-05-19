# debezium-bigquery-json-extractor
A simple tool to help automate the creation of BigQuery SQL statements that extracts Debezium's Mongodb connector 'after' field


*This package is used in conjunction with the tool `Studio 3T`'s schema analyzer CSV export*


## Setup
* Create python virtualenv & install dependencies
```shell
$ pyenv virtualenv 3.8.0 parser
$ pyenv activate parser
$ pip install -r requirements.txt
```

* Update `main.py` with the filename you want to run parser against.
* Run `main.py`

Sample output using `sample_file.csv`
```
JSON_EXTRACT(after, "$._class" AS _class,
JSON_EXTRACT(after, "$._id['$oid']" AS ObjectId,
JSON_EXTRACT_ARRAY(after, "$.aliases" AS aliases,
JSON_EXTRACT(after, "$.createdBy" AS createdBy,
JSON_EXTRACT(after, "$.createdDate['$date']" AS createdDate,
```
