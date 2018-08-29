# datomic-playground

Examples of how to use Datomic.

## Tips
https://gist.github.com/sgnl/609557ebacd3378f3b72
* brew services start postgresql
* brew services stop postgresql

Console launcher can be weird to figure out...
https://groups.google.com/forum/#!topic/datomic/ygPBk0_IUyM
bin/console -p 8080 dev datomic:sql://\?jdbc:postgresql://localhost:5432/datomic\?user=datomic\&password=datomic

## Usage

Get Datomic:
* Free: https://my.datomic.com/downloads/free
* Pro: https://my.datomic.com/downloads/pro (Probably need to log in)

Set up Storage:
* https://docs.datomic.com/on-prem/storage.html

Run the transactor by copying the template file and doing something like this with your tweaked template:
* bin/transactor ~/.datomic/sql-transactor-template.properties
* bin/transactor -Xmx4g ~/.datomic/sql-transactor-template.properties

Run the REPL and try it out. 

The Seattle example and data are straight from the download.

datomic-playground.simple-example is a very simple example using refs, schemas, and some basics.

## License

Copyright © 2018 Mark Bastian

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
