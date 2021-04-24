// Used for writing assertions
use std::process::Command;

use assert_cmd::prelude::*;

// Run programs
#[test]
fn it_can_run_the_commandline_for_a_simple_query() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("qsv")?;
    cmd.arg("SELECT 1 = 1");
    cmd.assert().success();
    Ok(())
}
#[test]
fn it_errors_if_no_query_is_passed() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("qsv")?;
    cmd.assert()
        .failure()
        .stderr(predicates::str::contains("the len is 1"));
    Ok(())
}

#[test]
fn it_will_run_a_simple_query_with_subqueries() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("qsv")?;
    cmd.arg("select * from (select * from (select*from ./testdata/occupations.csv))");
    cmd.assert().success();
    Ok(())
}

#[test]
fn it_will_run_a_simple_query_with_spaces_in_filename() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("qsv")?;
    cmd.arg("select * from (select * from (select*from `./testdata/occupations with spaces.csv`))");
    cmd.assert().success();
    Ok(())
}

#[test]
fn it_will_run_a_simple_query_with_ctes() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("qsv")?;
    cmd.arg("with some_cte (age) as (select distinct(age) from testdata/people.csv where age <> 13) select * from testdata/occupations.csv occupation INNER JOIN some_cte on (occupation.minimum_age = some_cte.age)");
    cmd.assert().success();
    Ok(())
}

#[test]
fn it_will_run_a_simple_query_with_unions() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("qsv")?;
    cmd.arg("select age from ./testdata/people.csv union select minimum_age as age from ./testdata/occupations.csv");
    cmd.assert().success();
    Ok(())
}
