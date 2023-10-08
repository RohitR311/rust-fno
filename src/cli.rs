use clap::Parser;

/// Program to calculate IV 
#[derive(Parser, Debug)]
#[command(author = "F&O", version = "0.0.1", about = "A program to fetch F&O data and display IV.", long_about = None)]
pub struct Args {
    /// Name of the symbol to extract
    pub symbol: Option<String>,
}