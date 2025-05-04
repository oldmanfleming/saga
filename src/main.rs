use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use clap::{ArgAction, Parser};
use cron::Schedule;
use epub_builder::{EpubBuilder, EpubContent, EpubVersion, TocElement, ZipLibrary};
use feed_rs::{model::Entry, parser};
use html5ever::tree_builder::TreeBuilderOpts;
use html5ever::{ParseOpts, parse_document};
use lettre::message::{SinglePart, header};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, SmtpTransport, Transport};
use log::{error, info, warn};
use markup5ever_rcdom::{RcDom, SerializableHandle};
use rand::{rng, seq::IndexedRandom};
use reqwest::blocking;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Deserialize;
use simple_logger::SimpleLogger;
use std::default::Default;
use std::{fs, str::FromStr, thread, time::Duration};
use tendril::TendrilSink;
use xml5ever::serialize::{SerializeOpts, serialize};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Optionally run in daemon mode
    #[arg(short, long, action = ArgAction::SetTrue)]
    daemon: Option<bool>,
}

/// To think about:
/// - Reading pdf papers
/// - Reading html blogs that don't support RSS
fn main() -> Result<()> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let cli = Cli::parse();

    let config = get_config()?;

    let db = get_db_conn()?;

    match cli.daemon {
        Some(true) => start_daemon(&db, &config),
        _ => process(&db, &config),
    }
}

fn start_daemon(db: &Connection, config: &Config) -> Result<()> {
    info!("Using schedule: {}", config.schedule);

    let schedule = Schedule::from_str(&config.schedule).unwrap();
    info!("Daemon started, waiting for next scheduled run...");
    loop {
        if let Some(next) = schedule.upcoming(Utc).next() {
            let now = Utc::now();
            let duration_until_next = next.signed_duration_since(now);

            if duration_until_next > chrono::Duration::zero() {
                info!("Next run scheduled at: {}", next);
                if let Ok(std_duration) = duration_until_next.to_std() {
                    thread::sleep(std_duration);
                } else {
                    warn!("Calculated duration is negative, running immediately.");
                }
            } else {
                info!("Scheduled time is now or in the past, running immediately.");
            }

            info!("Running scheduled process...");
            if let Err(e) = process(db, config) {
                error!("Error during scheduled process: {}", e);
            }
            info!("Scheduled process finished.");

            thread::sleep(Duration::from_secs(1));
        } else {
            error!("Could not determine next schedule time.");
            thread::sleep(Duration::from_secs(60));
        }
    }
}

fn process(db: &Connection, config: &Config) -> Result<()> {
    let mut entries = vec![];
    let cutoff = Utc::now();
    for feed_conf in &config.rss {
        if let Some(entry) = get_entry(db, feed_conf, cutoff)? {
            info!("Found entry {}", entry.title.clone());
            entries.push(entry);
        }
    }

    let epub_content = generate_epub(entries)?;

    let epub_name = format!("saga_output_{}.epub", Utc::now().format("%Y%m%d_%H%M%S"));

    fs::write(&epub_name, &epub_content)?;
    info!("EPUB file saved as: {}", epub_name);

    send_email(config, &epub_name, epub_content)?;

    // update last_processed time and insert entries in transaction

    Ok(())
}

// cut off time is used to guard against race condition of an entry
// being published during processing and being considered missed
fn get_entry(
    db: &Connection,
    feed_conf: &FeedConfig,
    cutoff: DateTime<Utc>,
) -> Result<Option<DisplayEntry>> {
    info!("Processing rss feed: {}", feed_conf.url);

    info!("Fetching entries");

    let entries = get_entries(&feed_conf.url)?;

    info!("Finding entry");

    // find new entries that have not been processed yet
    let new_entries: Vec<DisplayEntry> = entries
        .into_iter()
        .filter(|x| x.published < cutoff && !is_entry_already_processed(db, &x.id).unwrap())
        .collect();

    if new_entries.is_empty() {
        warn!("Feed is empty");
        return Ok(None);
    }

    let entry = pick_entry(db, feed_conf, new_entries)?;

    Ok(Some(entry))
}

fn pick_entry(
    db: &Connection,
    feed_conf: &FeedConfig,
    new_entries: Vec<DisplayEntry>,
) -> Result<DisplayEntry> {
    match get_feed_last_processed(db, &feed_conf.url)? {
        Some(last_processed) => {
            // find unprocessed new entries published after the last processed time
            let mut unprocessed_entries: Vec<&DisplayEntry> = new_entries
                .iter()
                .filter(|x| x.published > last_processed)
                .collect();

            // if there is nothing new and random is set
            // take a random old one that has not been processed
            if unprocessed_entries.is_empty() && feed_conf.random {
                info!("Picking a random entry");
                return new_entries
                    .choose(&mut rng())
                    .cloned()
                    .ok_or(anyhow!("failed to pick random entry"));
            }

            info!("Picking oldest of the new entries");
            // take the oldest after the cutoff
            unprocessed_entries.sort_by(|a, b| a.published.cmp(&b.published));
            unprocessed_entries
                .first()
                .cloned()
                .cloned()
                .ok_or(anyhow!("failed to pick first entry"))
        }
        None => {
            // If there's no last processed time, take the newest entry
            info!("Picking the latest entry");
            new_entries
                .first()
                .cloned()
                .ok_or(anyhow!("failed to pick first entry"))
        }
    }
}

#[derive(Deserialize, Debug)]
struct Config {
    email: EmailConfig,
    schedule: String,
    rss: Vec<FeedConfig>,
}

#[derive(Deserialize, Debug)]
struct FeedConfig {
    url: String,
    random: bool,
}

#[derive(Deserialize, Debug)]
struct EmailConfig {
    to: String,
    from: String,
    relay: String,
    username: String,
    password: String,
}

fn get_config() -> Result<Config> {
    let mut config_path = std::env::current_dir()?;
    config_path.push("config.yml");

    let config_str = std::fs::read_to_string(&config_path)?;
    let config: Config = serde_yml::from_str(&config_str)?;

    info!("Using config at path {:?}", config_path);

    Ok(config)
}

#[derive(Debug, Clone)]
struct DisplayEntry {
    id: String,
    feed_title: String,
    title: String,
    authors: Vec<String>,
    published: DateTime<Utc>,
    content: String,
}

fn get_entries(url: &String) -> Result<Vec<DisplayEntry>> {
    let resp = blocking::get(url)?.text()?;
    let feed = parser::parse(resp.as_bytes())?;
    let mut display_enrties: Vec<DisplayEntry> = vec![];
    for entry in feed.entries {
        let feed_title = feed
            .title
            .as_ref()
            .map_or(String::from("Unknown Feed"), |x| x.content.clone());
        let id = entry.id.clone();
        let title = entry
            .title
            .as_ref()
            .map_or(String::from("Unknown Title"), |x| x.content.clone());
        let authors = entry.authors.iter().map(|a| a.name.clone()).collect();
        let published = entry.published.unwrap_or(DateTime::<Utc>::MIN_UTC);
        let content = parse_xhtml(entry)?;
        info!("Contet: {}", content);
        display_enrties.push(DisplayEntry {
            id,
            feed_title,
            title,
            authors,
            published,
            content,
        });
    }

    Ok(display_enrties)
}

// TODO: Maybe support content being a src link if we see it happening
fn parse_xhtml(entry: Entry) -> Result<String> {
    let content = entry
        .content
        .ok_or(anyhow!("No content found"))?
        .body
        .ok_or(anyhow!("No content body found"))?;

    let parse_opts = ParseOpts {
        tree_builder: TreeBuilderOpts {
            drop_doctype: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let dom = parse_document(RcDom::default(), parse_opts)
        .from_utf8()
        .read_from(&mut content.as_bytes())?;

    // Prepare for serialization
    let mut buffer = Vec::new();

    // Serialize with XML compliant options
    let ser_opts = SerializeOpts {
        // traversal_scope: TraversalScope::IncludeNode,
        ..Default::default()
    };

    // Convert DOM to XHTML
    let document: SerializableHandle = dom.document.clone().into();
    serialize(&mut buffer, &document, ser_opts)?;

    Ok(String::from_utf8(buffer)?)
}

fn generate_epub(entries: Vec<DisplayEntry>) -> Result<Vec<u8>> {
    let mut output = Vec::<u8>::new();
    let mut builder = EpubBuilder::new(ZipLibrary::new()?)?;
    let title = format!("Saga - 1");
    builder
        .epub_version(EpubVersion::V30)
        .metadata("author", "Saga")?
        .metadata("title", title)?;

    let entry = entries.first().unwrap();
    builder.add_content(
        EpubContent::new("chapter_1.xhtml", entry.content.as_bytes())
            .title("Chapter 1")
            .child(TocElement::new("chapter_1.xhtml#1", "1.1")),
    )?;
    builder.inline_toc();
    builder.generate(&mut output)?;
    Ok(output)
}

fn send_email(config: &Config, epub_name: &String, epub_content: Vec<u8>) -> Result<()> {
    info!("Sending to email: {}", config.email.to);

    let email = Message::builder()
        .from(config.email.from.parse()?)
        .to(config.email.to.parse()?)
        .singlepart(
            SinglePart::builder()
                .header(header::ContentType::parse("application/epub+zip").unwrap())
                .header(header::ContentDisposition::attachment(epub_name))
                .body(epub_content),
        )?;
    let creds = Credentials::new(config.email.username.clone(), config.email.password.clone());
    let mailer = SmtpTransport::relay(&config.email.relay)?
        .credentials(creds)
        .build();

    match mailer.send(&email) {
        Ok(_) => info!("Email sent successfully!"),
        Err(e) => error!("Could not send email: {:?}", e),
    };

    Ok(())
}

fn get_db_conn() -> Result<Connection> {
    let mut db_path = std::env::current_dir()?;
    db_path.push("database.db3");
    let conn = Connection::open(&db_path)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS feeds (
            url TEXT PRIMARY KEY,
            last_processed INTEGER
        )",
        [],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS entries (
            id TEXT PRIMARY KEY
        )",
        [],
    )?;
    info!("Openned connection at path: {:?}", db_path);
    Ok(conn)
}

fn get_feed_last_processed(conn: &Connection, url: &String) -> Result<Option<DateTime<Utc>>> {
    let last_processed = match conn
        .query_row(
            "SELECT last_processed FROM feeds WHERE url = ?1",
            params![url],
            |row| row.get(0),
        )
        .optional()?
    {
        Some(last_processed) => Some(
            DateTime::from_timestamp_millis(last_processed)
                .ok_or(anyhow!("couldn't parse last_processed"))?,
        ),
        None => None,
    };
    Ok(last_processed)
}

fn is_entry_already_processed(conn: &Connection, id: &String) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT count(*) FROM entries WHERE id = ?1",
        params![id],
        |row| row.get(0).map(|x: i64| x > 0),
    )
}
