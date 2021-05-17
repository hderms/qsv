mod query_subcommand {
    use std::process::Command;

    use assert_cmd::prelude::*;
    fn build_cmd() -> Command {
        let mut cmd = Command::cargo_bin("qsv").unwrap();
        cmd.arg("query");
        cmd
    }
    #[test]
    fn it_can_run_the_commandline_for_a_simple_query() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("SELECT 1 = 1");
        cmd.assert().success();
        Ok(())
    }

    #[test]
    fn it_errors_if_no_query_is_passed() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.assert()
            .failure()
            .stderr(predicates::str::contains(
                "The following required arguments were not provided",
            ))
            .stderr(predicates::str::contains("<query>"));
        Ok(())
    }

    #[test]
    fn it_will_run_a_simple_query_with_subqueries() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select * from (select * from (select*from ./testdata/occupations.csv))");
        cmd.assert().success();
        Ok(())
    }

    #[test]
    fn it_will_run_a_simple_query_with_spaces_in_filename() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut cmd = build_cmd();
        cmd.arg(
            "select * from (select * from (select*from `./testdata/occupations with spaces.csv`))",
        );
        cmd.assert().success();
        Ok(())
    }

    #[test]
    fn it_will_run_a_simple_query_with_ctes() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("with some_cte (age) as (select distinct(age) from testdata/people.csv where age <> 13) select * from testdata/occupations.csv occupation INNER JOIN some_cte on (occupation.minimum_age = some_cte.age)");
        cmd.assert().success();
        Ok(())
    }

    #[test]
    fn it_will_run_a_simple_query_with_unions() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select age from ./testdata/people.csv union select minimum_age as age from ./testdata/occupations.csv");
        cmd.assert().success();
        Ok(())
    }

    #[test]
    fn it_will_run_a_simple_query_with_sqrt_on_float() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select sqrt(12374)");
        cmd.assert()
            .success()
            .stdout(predicates::str::contains("111.23848254988019"));
        Ok(())
    }

    #[test]
    fn it_will_run_a_simple_query_with_sqrt_on_integer() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select sqrt(4)");
        cmd.assert()
            .success()
            .stdout(predicates::str::contains("2"));
        Ok(())
    }

    #[test]
    fn it_will_run_a_simple_query_with_md5() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select md5('foobar')");
        cmd.assert().success().stdout(predicates::str::contains(
            "3858f62230ac3c915f300c664312c63f",
        ));
        Ok(())
    }

    #[test]
    fn it_will_run_a_simple_query_with_stddev() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select stddev(number) from testdata/statistical.csv");
        cmd.assert()
            .success()
            .stdout(predicates::str::contains("1.7078251276599"));
        Ok(())
    }

    #[test]
    fn it_will_run_with_an_alternate_delimiter() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select min(minimum_age) from testdata/slash_as_separator.csv");
        cmd.arg("--delimiter=/");
        cmd.assert()
            .success()
            .stdout(predicates::str::contains("25"));
        Ok(())
    }

    #[test]
    fn it_will_run_with_trim() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select min(minimum_age) from testdata/occupations_with_extraneous_spaces.csv");
        cmd.arg("--trim");
        cmd.assert().success();
        Ok(())
    }

    #[test]
    fn it_will_run_with_textonly() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select md5(column) from testdata/sortable_columns.csv");
        cmd.arg("--textonly");
        cmd.assert().success();
        Ok(())
    }

    #[test]
    fn it_will_run_a_query_from_a_gz_file() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select * from testdata/people.csv.gz");
        cmd.assert().success();
        Ok(())
    }

    #[test]
    fn it_will_run_a_query_and_output_header() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select * from testdata/people.csv");
        cmd.arg("--output-header");
        cmd.assert()
            .success()
            .stdout(predicates::str::contains("name,age"));
        Ok(())
    }

    #[test]
    fn it_will_handle_a_column_of_floats() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select avg(number) from testdata/all_floats.csv");
        cmd.arg("--textonly");
        cmd.assert()
            .success()
            .stdout(predicates::str::contains("2.5100000000000002"));
        Ok(())
    }

    #[test]
    fn it_will_handle_a_column_of_mixed_floats() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select avg(number) from testdata/mixed_floats.csv");
        cmd.arg("--textonly");
        cmd.assert()
            .success()
            .stdout(predicates::str::contains("2.14"));
        Ok(())
    }

    #[test]
    fn it_rejects_multiple_sql_statements_in_query() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select 1 = 1;select 1 = 1;");
        cmd.arg("--textonly");
        cmd.assert().failure().stderr(predicates::str::contains(
            "Expected exactly one SQL statement in query input",
        ));
        Ok(())
    }

    #[test]
    fn it_succeeds_with_a_single_statement_ended_with_semicolon(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select 1 = 1;");
        cmd.arg("--textonly");
        cmd.assert().success();
        Ok(())
    }

    #[test]
    fn it_succeeds_with_a_csv_with_spaces_in_headers() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select \"minimum age\" from testdata/occupations_with_spaces_in_headers.csv");
        cmd.assert().success();
        Ok(())
    }

    #[test]
    fn it_succeeds_without_running_sql_in_file_headers() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select \"minimum age\" from testdata/sql_injection.csv");
        cmd.assert().success();
        Ok(())
    }
}
mod analyze_subcommand {
    use std::process::Command;

    use assert_cmd::prelude::*;
    use predicates::str::contains;

    fn build_cmd() -> Command {
        let mut cmd = Command::cargo_bin("qsv").unwrap();
        cmd.arg("analyze");
        cmd
    }
    #[test]
    fn it_can_run_the_commandline_for_a_simple_query() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("SELECT 1 = 1");
        cmd.assert().success();
        Ok(())
    }

    #[test]
    fn it_errors_if_no_query_is_passed() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.assert()
            .failure()
            .stderr(contains(
                "The following required arguments were not provided",
            ))
            .stderr(contains("<query>"));
        Ok(())
    }

    #[test]
    fn it_will_run_a_simple_query_with_subqueries() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select * from (select * from (select*from ./testdata/occupations.csv))");
        cmd.assert()
            .success()
            .stdout(contains("./testdata/occupations.csv:"))
            .stdout(contains("minimum_age -> integer"))
            .stdout(contains("occupation -> text"));
        Ok(())
    }

    #[test]
    fn it_will_run_a_simple_query_with_spaces_in_filename() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut cmd = build_cmd();
        cmd.arg(
            "select * from (select * from (select*from `./testdata/occupations with spaces.csv`))",
        );
        cmd.assert().success();
        Ok(())
    }

    #[test]
    fn it_will_run_a_simple_query_with_unions() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("select age from ./testdata/people.csv union select minimum_age as age from ./testdata/occupations.csv");
        cmd.assert()
            .success()
            .stdout(contains("./testdata/occupations.csv:"))
            .stdout(contains("minimum_age -> integer"))
            .stdout(contains("occupation -> text"))
            .stdout(contains("./testdata/people.csv:"))
            .stdout(contains("name -> text"))
            .stdout(contains("age -> integer"));

        Ok(())
    }
}

mod stats_subcommand {
    use std::process::Command;

    use assert_cmd::prelude::*;
    use predicates::str::contains;

    fn build_cmd() -> Command {
        let mut cmd = Command::cargo_bin("qsv").unwrap();
        cmd.arg("stats");
        cmd
    }
    #[test]
    fn it_can_run_the_commandline_for_a_simple_query() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.arg("testdata/statistical.csv");
        cmd.assert()
            .success()
            .stdout(contains("Mean: 3.50000"))
            .stdout(contains("Stddev: 1.707"))
            .stdout(contains("Min: 1"))
            .stdout(contains("Max: 6"))
            .stdout(contains("Unique: 6"));
        Ok(())
    }

    #[test]
    fn it_errors_if_no_query_is_passed() -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = build_cmd();
        cmd.assert()
            .failure()
            .stderr(contains(
                "The following required arguments were not provided",
            ))
            .stderr(contains("<filename>"));
        Ok(())
    }
}
