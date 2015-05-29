# elasticsearch-jdbc-river-rebuild-strategy
Rebuild strategy for the elasticsearch-jdbc-river (https://github.com/jprante/elasticsearch-jdbc)

# Usage
This strategy can be used to update Elasticsearch indexes periodically. It simple rebuilds the index at configured intervals. 

The stategy uses an alias to map to the real index. 
The alias will be switched to the new builded index after the rebuild (atomic operation). The old index will be deleted after the switch.

# Configuration

Use a feeder config like this:

    {
        "elasticsearch" : {
            "cluster" : "elasticsearch",
            "host" : "localhost",
            "port" : 9300
        },
        "strategy" : "rebuild",
        "interval" : 60000,
        "jdbc" : {
            "url": "jdbc:oracle:thin:@oracldb:1521:mydb",
            "user": "superman",
            "password": "secret",
            "sql": [  "select 'car' as \"_type\", c.* from cars c" ,
                      "select 'bike' \"_type\",b.* from bikes b" ],
            "treat_binary_as_string": true,
            "alias": "valuelist",
            "index_prefix": "valuelist_",
            "template": "valuelist_index.json"
        }
    }

Important settings (rebuild strategy specific. See for the other the https://github.com/jprante/elasticsearch-jdbc project):

*alias* - The name of the created alias

*index_prefix* - The prefix of the generated index. A timestamp will be added

*template* - Path to a JSON file containing the template to create the index (settings, mappings)

**Example of a template file (valuelist_index.json)**

    {
        "settings": {
            "index" : {
                "number_of_shards" : 1,
                "refresh_interval" : 1,
                "number_of_replicas" : 0
            },
            "analysis": {
                "analyzer": {
                    <removed>
        },
        "mappings": {
            "car": {
                "_source": {
                    "enabled": true
                },
                "_all": {
                    "enabled": false
                },
                "properties": {
                     <removed>
                }
            },
            "bike": {
                "_source": {
                    "enabled": true
                },
                "_all": {
                    "enabled": false
                },
                "properties": {
                      <removed>
                }
            }
        }
    }
