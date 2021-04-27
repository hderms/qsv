use rusqlite::functions::{Context, Aggregate};
use rusqlite::Result;
use statistical::population_standard_deviation;

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

impl Aggregate<Vec<f64>, Option<f64>> for Stddev {
    fn init(&self, _: &mut Context<'_>) -> Result<Vec<f64>> {
        Ok(vec!())
    }

    fn step(&self, ctx: &mut Context<'_>, stdev: &mut Vec<f64>) -> Result<()> {
        let next = ctx.get::<f64>(0)?;
        stdev.push(next);
        Ok(())
    }

    fn finalize(&self, _: &mut Context<'_>, numbers: Option<Vec<f64>>) -> Result<Option<f64>> {
        println!("{:?}", &numbers);
        let stddev = numbers.map(|n| population_standard_deviation(&n, None));
        println!("{:?}", stddev);
        Ok(stddev)
    }
}

