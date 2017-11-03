cassandra-schema
================


bin/agrees
----------
Report on schema agreement.

    Usage: agrees [options]
    
    Options:
      --version       Show version number                                  [boolean]
      -h, --help      Show help                                            [boolean]
      -H, --hostname  Contact hostname               [string] [default: "localhost"]
      -P, --port      Contact port number                   [number] [default: 9042]
      -u, --username  Cassandra username             [string] [default: "cassandra"]
      -p, --password  Cassandra password             [string] [default: "cassandra"]
      -c, --config    RESTBase configuration file                           [string]
      --verbose       Use verbose output                                   [boolean]

bin/mkschema
------------
Robustly, sequentially, execute CQL DDL statements, against a single Cassandra host.

    Usage: mkschema <schema.yaml>
    
    Options:
      --version        Show version number                                 [boolean]
      -h, --help       Show help                                           [boolean]
      -H, --hostname   Contact hostname              [string] [default: "localhost"]
      -P, --port       Contact port number                  [number] [default: 9042]
      -u, --username   Cassandra username            [string] [default: "cassandra"]
      -p, --password   Cassandra password            [string] [default: "cassandra"]
      -c, --config     RESTBase configuration file                          [string]
      --without-ssl    Disable SSL encryption                              [boolean]
      --max-wait-secs  The maximum time to wait for agreement (after a DDL statement
                       execution)                             [number] [default: 10]
    

