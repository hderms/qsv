use rusqlite::functions::{Aggregate, Context, FunctionFlags};
use rusqlite::{Connection, Result};
use stats::OnlineStats;

pub(crate) fn add_udfs(connection: &Connection) -> Result<()> {
    connection.create_scalar_function(
        "md5",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        move |ctx| calculate_md5(ctx).map_err(|e| rusqlite::Error::UserFunctionError(e.into())),
    )?;
    connection.create_scalar_function(
        "sqrt",
        1,
        FunctionFlags::SQLITE_DETERMINISTIC,
        move |ctx| calculate_sqrt(ctx).map_err(|e| rusqlite::Error::UserFunctionError(e.into())),
    )?;
    connection.create_aggregate_function(
        "stddev",
        1,
        FunctionFlags::SQLITE_DETERMINISTIC,
        Stddev,
    )?;
    Ok(())
}
pub(crate) fn calculate_md5(ctx: &Context) -> Result<String> {
    assert_eq!(ctx.len(), 1, "called with unexpected number of arguments");
    let str = ctx.get_raw(0).as_str()?;
    let hash = md5::compute(str);
    Ok(format!("{:x}", hash))
}

pub(crate) fn calculate_sqrt(ctx: &Context) -> Result<f64> {
    assert_eq!(ctx.len(), 1, "called with unexpected number of arguments");
    let arg = ctx.get_raw(0);
    if let Ok(f64) = arg.as_f64() {
        Ok(f64.sqrt())
    } else {
        let i64 = arg.as_i64()?;
        Ok((i64 as f64).sqrt())
    }
}
pub struct Stddev;

impl Aggregate<OnlineStats, Option<f64>> for Stddev {
    fn init(&self, _: &mut Context<'_>) -> Result<OnlineStats> {
        Ok(OnlineStats::new())
    }

    fn step(&self, ctx: &mut Context<'_>, stdev: &mut OnlineStats) -> Result<()> {
        let next = ctx.get::<f64>(0)?;
        stdev.add(next);
        Ok(())
    }

    fn finalize(&self, _: &mut Context<'_>, numbers: Option<OnlineStats>) -> Result<Option<f64>> {
        let stddev = numbers.map(|n| n.stddev());
        Ok(stddev)
    }
}
