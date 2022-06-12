use std::{path::PathBuf, process::exit};

use bytesize::ByteSize;
use clap::{Command, FromArgMatches, Parser, Subcommand};
use cli_table::{Cell, Style, Table};
use rustyline::Editor;
use sediment::{
    database::Database,
    format::GrainId,
    io::{
        self,
        any::{AnyFile, AnyFileManager},
    },
};

#[derive(Parser, Debug)]
struct Args {
    #[clap(subcommand)]
    command: Action,

    database_path: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
enum Action {
    Open,
    #[clap(flatten)]
    SingleCommand(SingleCommand),
}

#[derive(Subcommand, Debug)]
enum SingleCommand {
    Get {
        grain_id: GrainId,
    },
    Push {
        value: Option<String>,
        value_path: Option<PathBuf>,
    },
    Stats,
}

fn main() {
    let args = Args::parse();
    let mut db = if let Some(path) = &args.database_path {
        Database::open_with_manager(path, &AnyFileManager::new_file()).unwrap()
    } else {
        println!("Using memory database with no persistence.");
        Database::open_with_manager("db", &AnyFileManager::new_memory()).unwrap()
    };

    match args.command {
        Action::Open => repl(db),
        Action::SingleCommand(command) => command.execute_on(&mut db).unwrap(),
    }
}

fn repl(mut db: Database<AnyFile>) {
    let mut rl = Editor::<()>::new();

    while let Ok(line) = rl.readline("> ") {
        let command = SingleCommand::augment_subcommands(Command::new("sediment"));
        let mut words = shell_words::split(&line).unwrap();
        words.insert(0, String::from("sediment"));
        match command.try_get_matches_from(words) {
            Ok(matches) => {
                let command = SingleCommand::from_arg_matches(&matches).unwrap();
                command.execute_on(&mut db).unwrap();
            }
            Err(err) => {
                eprintln!("{err}");
            }
        }
    }
}

impl SingleCommand {
    fn execute_on(&self, database: &mut Database<AnyFile>) -> io::Result<()> {
        match self {
            SingleCommand::Get { grain_id } => {
                let data = database.read(*grain_id)?;
                if let Some(data) = data {
                    println!("{}", pretty_hex::pretty_hex(&data.data));
                    Ok(())
                } else {
                    eprintln!("No data stored at that grain id.");
                    exit(1)
                }
            }
            SingleCommand::Push { value, value_path } => {
                if let Some(value) = value {
                    let mut session = database.new_session();
                    let grain = session.write(value.as_bytes())?;
                    let batch = session.commit()?;
                    println!("New grain id: {}", grain);
                    println!("Committed in batch: {}", batch);
                    Ok(())
                } else if let Some(path) = value_path {
                    todo!()
                } else {
                    eprintln!("Either a value or a path must be provided");
                    exit(1)
                }
            }
            SingleCommand::Stats => {
                let stats = database.statistics();
                println!(
                    "Total database size: {} ({} bytes).",
                    ByteSize(stats.file_size).to_string_as(true),
                    stats.file_size
                );
                println!(
                    "Unallocated disk space: {} ({} bytes).",
                    ByteSize(stats.unallocated_bytes).to_string_as(true),
                    stats.unallocated_bytes
                );
                let total_grain_space = stats
                    .grains_by_length
                    .iter()
                    .map(|(bytes, stats)| u64::from(*bytes) * (stats.free + stats.allocated))
                    .sum();
                println!(
                    "Total Grain Capacity: {} ({} bytes)",
                    ByteSize(total_grain_space).to_string_as(true),
                    total_grain_space
                );
                let mut rows = Vec::new();
                for (grain_length, grain_stats) in &stats.grains_by_length {
                    rows.push(vec![
                        grain_length.cell(),
                        grain_stats.free.cell(),
                        grain_stats.allocated.cell(),
                        (grain_stats.free + grain_stats.allocated).cell(),
                    ]);
                }
                let table = rows
                    .table()
                    .title(vec![
                        "Grain Size".cell().bold(true),
                        "Free".cell().bold(true),
                        "Allocated".cell().bold(true),
                        "Total".cell().bold(true),
                    ])
                    .display()
                    .unwrap();
                println!("{table}");
                Ok(())
            }
        }
    }
}