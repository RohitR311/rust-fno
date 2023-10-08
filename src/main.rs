use clap::Parser;
use polars::prelude::PolarsError;
use std::time::Instant;

mod helper;
use crate::helper::fno;

mod cli;
use crate::cli::Args;

fn main() -> Result<(), PolarsError> {
    // time the entire program
    let start = Instant::now();

    // specifying fixed columns
    let columns = [
        "SYMBOL",
        "EXPIRY_DT",
        "STRIKE_PR",
        "OPTION_TYP",
        "CLOSE",
        "TIMESTAMP",
        "CONTRACTS",
    ];
    // getting user input for symbol
    let args = Args::parse();
    let symbol;

    if let Some(sym) = args.symbol {
        symbol = sym;
    } else {
        symbol = "NIFTY".into();
    }

    // reading fno data
    let mut lf = fno::read_data("demo.csv")?;
    // processing fno data
    lf = fno::process_data(lf, &columns, &symbol);
    // splitting fno data
    let fno_lf = fno::split_data(lf);

    // processing fno data for iv calculation
    lf = fno::iv_prep(fno_lf);

    // calculate iv nad fetch the final df
    let df = fno::fetch_iv_df(lf)?;

    dbg!(df);
    dbg!(start.elapsed());

    Ok(())
}
