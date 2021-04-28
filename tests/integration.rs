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
        .stderr(predicates::str::contains("The following required arguments were not provided"))
        .stderr(predicates::str::contains("<query>"));
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

#[test]
fn it_will_run_a_simple_query_with_sqrt_on_float() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("qsv")?;
    cmd.arg("select sqrt(12374)");
    cmd.assert().success().stdout(predicates::str::contains("111.23848254988019"));
    Ok(())
}

#[test]
fn it_will_run_a_simple_query_with_sqrt_on_integer() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("qsv")?;
    cmd.arg("select sqrt(4)");
    cmd.assert().success().stdout(predicates::str::contains("2"));
    Ok(())
}

#[test]
fn it_will_run_a_simple_query_with_md5() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("qsv")?;
    cmd.arg("select md5('foobar')");
    cmd.assert().success().stdout(predicates::str::contains("3858f62230ac3c915f300c664312c63f"));
    Ok(())
}

#[test]
fn it_will_run_a_simple_query_with_stddev() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("qsv")?;
    cmd.arg("select stddev(number) from testdata/statistical.csv");
    cmd.assert().success().stdout(predicates::str::contains("1.7078251276599"));
    Ok(())
}

#[test]
fn it_will_run_with_an_alternate_delimiter() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("qsv")?;
    cmd.arg("select min(minimum_age) from testdata/slash_as_separator.csv");
    cmd.arg("--delimiter=/");
    cmd.assert().success().stdout(predicates::str::contains("25"));
    Ok(())
}
