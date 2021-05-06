# qsv
CLI tool to query CSVs through SQL. Attempts to be fast and provide the full featureset of SQLite

## Installation
After cloning the repository, you can install a binary locally using `cargo install --path .`

## Features
## Usage/Features
### Simple queries
qsv supports syntactically valid SQLite queries run on CSV data:

```qsv query "SELECT * FROM foo.csv AS foo INNER JOIN bar.csv AS bar ON (foo.id = bar.foo_id);"```

```qsv query "WITH ages(age) AS (SELECT age FROM testdata/people.csv) SELECT * FROM testdata/people.csv AS people INNER JOIN ages ON (people.age = ages.age);"```

you can escape spaces in a filename like so (you may have to escape backticks depending on shell):

```qsv query "select * from `testdata/occupations with spaces.csv`"```

### SQLite user defined functions
In order to make some common data analysis tasks simpler, qsv has a number of user defined functions added to SQLite. If there's something you'd like added, please request it as a Github Issue.
* md5(text)
* sqrt(real)
* stddev(real)

### Options
* `--delimiter=` to set a custom delimiter in the CSVs. Only set globally on the query
* `--textonly` force all columns to be inferred as strings/text
* `--trim` trim fields in CSVs in case there is additional whitespace. Will not remove whitespace from the middle of a string
* `--output-header` outputs the header alongside the results of the query. Off by default to make the output more in line with SQL
