# csvToElasticsearch
move csv files to elasticseach

This is a fork of git@github.com:aarreedd/CSV-to-ElasticSearch.git with some improves.

Improves:

* Add chuck to import big files
* You can force fields to be string
* Fix fail for float values

#Use

1. CREATE example:

$ python csv_to_elastic.py \
    --elastic-address 'localhost:9200' \
    --csv-file input.csv \
    --elastic-index 'index' \
    --must-be-string 'ci,dni'\
     --chuck 10 \
    --datetime-field=dateField \
    --json-struct '{
        "name" : "%name%",
        "ci" : "%ci%",
        "dni" : "%dni%",
        }'
    

CSV:

name,ci,dni
user,001,023


## Flags
```
--must-be-string String with comma separator
  fields that 
--chuck number of rows that will send in a group
