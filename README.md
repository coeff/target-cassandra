# target-cassandra

A [Singer.io](https://singer.io) target for [Apache Cassandra](http://cassandra.apache.org/)

## Usage

- If developing, run `python setup.py develop`, otherwise install egg normally.
- Adjust connection parameters in `target_cassandra_config.json`
- Run the target with a valid input piped through stdin: `tap-cassandra -c target_cassandra_config.json < singer_formatted_data.json`

## TODO

- [ ] Support complex types (`object`, `anyOf`)

---

Copyright &copy; 2020 Coefficient
