pub mod fno {
    use blackscholes::{ImpliedVolatility, Inputs, OptionType};
    use polars::{
        lazy::dsl::{as_struct, cols, when, Expr, StrptimeOptions},
        prelude::*,
    };

    pub fn read_data(path: &str) -> Result<LazyFrame, PolarsError> {
        LazyCsvReader::new(path)
            .with_infer_schema_length(Some(0))
            .truncate_ragged_lines(true)
            .has_header(true)
            .with_try_parse_dates(true)
            .finish()
    }

    pub fn process_data(lf: LazyFrame, columns: &[&str], symbol: &str) -> LazyFrame {
        lf.select([cols(columns)])
            .filter(
                col("SYMBOL")
                    .eq(lit(symbol))
                    .and(col("CONTRACTS").neq(lit("0"))),
            )
            .select([col("*").exclude(&["CONTRACTS"])])
            .with_column(cols(["EXPIRY_DT", "TIMESTAMP"]).str().strptime(
                DataType::Date,
                StrptimeOptions {
                    format: Some("%d-%b-%Y".into()),
                    ..Default::default()
                },
                Expr::from(0),
            ))
            .with_columns([
                ((col("EXPIRY_DT") - col("TIMESTAMP")) / lit(24 * 60 * 60 * 1000))
                    .alias("DTE")
                    .cast(DataType::Float32),
                cols(["STRIKE_PR", "CLOSE"]).cast(DataType::Float32),
            ])
    }

    pub fn split_data(lf: LazyFrame) -> (LazyFrame, LazyFrame) {
        (
            lf.clone().filter(col("OPTION_TYP").neq(lit("XX"))),
            lf.filter(col("OPTION_TYP").eq(lit("XX"))),
        )
    }

    pub fn iv_prep(fno_lf: (LazyFrame, LazyFrame)) -> LazyFrame {
        let (opt_lf, fut_lf) = fno_lf;

        JoinBuilder::new(opt_lf)
            .with(fut_lf.select([cols(["SYMBOL", "EXPIRY_DT", "TIMESTAMP", "CLOSE"])]))
            .how(JoinType::Left)
            .on([col("SYMBOL"), col("EXPIRY_DT"), col("TIMESTAMP")])
            .finish()
            .with_columns([
                col("CLOSE_right").cast(DataType::Float32),
                (col("DTE") / lit(365)),
                when(col("OPTION_TYP").eq(lit("CE")))
                    .then(lit(0).alias("OPTION_TYP"))
                    .otherwise(lit(1).alias("OPTION_TYP")),
            ])
            .with_column(col("OPTION_TYP").cast(DataType::Float32))
            .rename(["CLOSE_right"], ["FUT_PR"])
            .filter(col("FUT_PR").is_not_null())
    }

    pub fn iv_calc(opt_typ: f32, s: f32, k: f32, p: Option<f32>, t: f32) -> f64 {
        // do something
        if opt_typ == 0.0 {
            let inputs = Inputs {
                option_type: OptionType::Call,
                s,
                k,
                p,
                r: 0.0,
                q: 0.0,
                t,
                sigma: None,
            };
            return inputs.calc_rational_iv().unwrap_or(0.0);
        }
        let inputs = Inputs {
            option_type: OptionType::Put,
            s,
            k,
            p,
            r: 0.0,
            q: 0.0,
            t,
            sigma: None,
        };
        inputs.calc_rational_iv().unwrap_or(0.0)
    }

    pub fn fetch_iv_df(lf: LazyFrame) -> PolarsResult<DataFrame> {
        lf.with_columns([as_struct(&[
            col("OPTION_TYP"),
            col("FUT_PR"),
            col("STRIKE_PR"),
            col("CLOSE"),
            col("DTE"),
        ])
        .map(
            |s| {
                let data = s.struct_()?;

                let binding = data.field_by_name("OPTION_TYP")?;
                let opt_typs = binding.f32()?;

                let binding = data.field_by_name("FUT_PR")?;
                let fut_prs = binding.f32()?;

                let binding = data.field_by_name("STRIKE_PR")?;
                let strike_prs = binding.f32()?;

                let binding = data.field_by_name("CLOSE")?;
                let opt_prs = binding.f32()?;

                let binding = data.field_by_name("DTE")?;
                let dtes = binding.f32()?;

                let out: Float64Chunked = opt_typs
                    .into_iter()
                    .zip(fut_prs.into_iter())
                    .zip(strike_prs.into_iter())
                    .zip(opt_prs.into_iter())
                    .zip(dtes.into_iter())
                    .map(|((((opt_typ, s), k), p), t)| {
                        if let (Some(opt_typ), Some(s), Some(k), Some(p), Some(t)) =
                            (opt_typ, s, k, p, t)
                        {
                            Some(iv_calc(opt_typ, s, k, Some(p), t))
                        } else {
                            None
                        }
                    })
                    .collect();

                Ok(Some(out.into_series()))
                // Ok(Some(Series::default()))
            },
            GetOutput::from_type(DataType::Float32),
        )
        .alias("IV")])
            .collect()
    }
}