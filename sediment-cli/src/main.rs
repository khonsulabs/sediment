use std::{path::PathBuf, process::exit};

use clap::{Parser, Subcommand};
use reedline::{ColumnarMenu, Completer, DefaultPrompt, Reedline, Signal};
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
    Stats,
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
        Action::Stats => todo!(),
    }
}

fn repl(db: Database<AnyFile>) {
    let completer = Box::new(CommandCompleter);
    let menu = Box::new(ColumnarMenu::default().with_name("test-menu"));
    let prompt = DefaultPrompt::default();

    let mut line_editor = Reedline::create()
        .with_completer(completer)
        .with_menu(reedline::ReedlineMenu::EngineCompleter(menu));
    while let Signal::Success(line) = line_editor.read_line(&prompt).unwrap() {
        println!("{line}")
    }
}

struct CommandCompleter;

impl Completer for CommandCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<reedline::Suggestion> {
        let word_start = line[..pos].rfind(' ').unwrap_or_default();
        let partial_word = dbg!(&line[word_start..pos]);
        vec![]
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
        }
    }
}
