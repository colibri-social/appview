use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use regex::Regex;
use serde_json::Value;

const OUT_OF_BAND_PARAMS: &[&str] = &["auth"];

const UPSTREAM_NSID_PREFIX: &str = "com.atproto.";

const NON_XRPC_PATHS: &[&str] = &["/", "/.well-known/did.json"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Optionality {
    Required,
    Optional,
    Unknown,
}

#[derive(Debug)]
struct Route {
    verb: String,
    nsid: String,
    params: BTreeMap<String, Optionality>,
    source: String,
}

#[derive(Debug, Default)]
struct Report {
    failures: Vec<String>,
    warnings: Vec<String>,
    routes_without_lexicon: Vec<String>,
    lexicons_without_route: Vec<String>,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn optionality_of(ty: &str) -> Optionality {
    let ty = ty.trim();
    if ty.starts_with("Option<") || ty.starts_with("Vec<") {
        Optionality::Optional
    } else {
        Optionality::Required
    }
}

fn parse_routes() -> Vec<Route> {
    let attr = Regex::new(
        r#"(?s)#\[(get|post|put|delete|patch)\(\s*"([^"]*)"\s*(?:,\s*data\s*=\s*"<[^"]*>"\s*)?,?\s*\)\]"#,
    )
    .expect("route attribute regex compiles");
    let sig = Regex::new(r"(?s)fn\s+\w+\s*\((.*?)\)\s*->").expect("signature regex compiles");

    let mut files = Vec::new();
    rust_sources(&repo_root().join("src"), &mut files);
    files.sort();

    let mut routes = Vec::new();

    for file in files {
        let Ok(text) = fs::read_to_string(&file) else {
            continue;
        };
        let rel = file
            .strip_prefix(repo_root())
            .unwrap_or(&file)
            .display()
            .to_string();

        for m in attr.captures_iter(&text) {
            let verb = m[1].to_string();
            let raw_path = m[2].to_string();
            let (path, query) = match raw_path.split_once('?') {
                Some((p, q)) => (p.to_string(), q.to_string()),
                None => (raw_path.clone(), String::new()),
            };

            if NON_XRPC_PATHS.contains(&path.as_str()) {
                continue;
            }
            let Some(nsid) = path.strip_prefix("/xrpc/") else {
                continue;
            };

            let names: Vec<String> = query
                .split('&')
                .filter_map(|seg| {
                    seg.trim()
                        .strip_prefix('<')
                        .and_then(|s| s.strip_suffix('>'))
                        .map(str::to_string)
                })
                .filter(|n| !OUT_OF_BAND_PARAMS.contains(&n.as_str()))
                .collect();

            let tail = &text[m.get(0).map(|g| g.end()).unwrap_or(0)..];
            let arg_types: BTreeMap<String, String> = sig
                .captures(tail)
                .map(|c| {
                    c[1].split(',')
                        .filter_map(|arg| {
                            let (name, ty) = arg.split_once(':')?;
                            let name = name.trim().trim_start_matches("r#");
                            Some((name.to_string(), ty.trim().to_string()))
                        })
                        .collect()
                })
                .unwrap_or_default();

            let params = names
                .into_iter()
                .map(|name| {
                    let optionality = arg_types
                        .get(&name)
                        .map(|ty| optionality_of(ty))
                        .unwrap_or(Optionality::Unknown);
                    (name, optionality)
                })
                .collect();

            routes.push(Route {
                verb,
                nsid: nsid.to_string(),
                params,
                source: rel.clone(),
            });
        }
    }

    routes
}

fn load_lexicons() -> BTreeMap<String, Value> {
    let dir = repo_root().join("lexicons");
    let mut docs = BTreeMap::new();

    let entries = fs::read_dir(&dir).unwrap_or_else(|e| {
        panic!(
            "cannot read {}: {e}. Run ./scripts/sync-lexicons.sh",
            dir.display()
        )
    });

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_none_or(|e| e != "json") {
            continue;
        }
        if path.file_name().is_some_and(|n| n == "manifest.json") {
            continue;
        }
        let text = fs::read_to_string(&path).expect("lexicon readable");
        let doc: Value = serde_json::from_str(&text)
            .unwrap_or_else(|e| panic!("{} is not valid JSON: {e}", path.display()));
        if let Some(id) = doc.get("id").and_then(Value::as_str) {
            docs.insert(id.to_string(), doc);
        }
    }

    docs
}

fn main_def(doc: &Value) -> Option<&Value> {
    let def = doc.get("defs")?.get("main")?;
    let kind = def.get("type")?.as_str()?;
    matches!(kind, "query" | "procedure" | "subscription").then_some(def)
}

fn lexicon_params(def: &Value) -> BTreeMap<String, bool> {
    let Some(params) = def.get("parameters") else {
        return BTreeMap::new();
    };
    let required: BTreeSet<&str> = params
        .get("required")
        .and_then(Value::as_array)
        .map(|r| r.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();

    params
        .get("properties")
        .and_then(Value::as_object)
        .map(|props| {
            props
                .keys()
                .map(|k| (k.clone(), required.contains(k.as_str())))
                .collect()
        })
        .unwrap_or_default()
}

fn expected_verb(kind: &str) -> &'static str {
    match kind {
        "procedure" => "post",
        _ => "get",
    }
}

#[derive(Debug, Default, serde::Deserialize)]
struct Exceptions {
    #[serde(default)]
    exception: Vec<Exception>,
}

#[derive(Debug, serde::Deserialize)]
struct Exception {
    nsid: String,
    param: String,
    reason: String,
    expires: String,
}

fn load_exceptions() -> Exceptions {
    let path = repo_root().join("lexicons/exceptions.toml");
    let Ok(text) = fs::read_to_string(&path) else {
        return Exceptions::default();
    };
    toml::from_str(&text).unwrap_or_else(|e| panic!("{} is malformed: {e}", path.display()))
}

fn today() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock is after the epoch")
        .as_secs() as i64;
    let days = secs / 86_400;

    let (mut y, mut d) = (1970, days);
    loop {
        let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        let len = if leap { 366 } else { 365 };
        if d < len {
            break;
        }
        d -= len;
        y += 1;
    }

    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let months = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut m = 0;
    while d >= months[m] {
        d -= months[m];
        m += 1;
    }

    format!("{y:04}-{:02}-{:02}", m + 1, d + 1)
}

fn build_report() -> Report {
    let routes = parse_routes();
    let lexicons = load_lexicons();
    let exceptions = load_exceptions();
    let mut report = Report::default();

    assert!(
        !routes.is_empty(),
        "parsed no routes out of src/. The attribute regex has probably drifted"
    );
    assert!(
        !lexicons.is_empty(),
        "no lexicons in lexicons/. Run ./scripts/sync-lexicons.sh"
    );

    let excused = |nsid: &str, param: &str| {
        exceptions
            .exception
            .iter()
            .any(|e| e.nsid == nsid && e.param == param)
    };

    let routed: BTreeSet<&str> = routes.iter().map(|r| r.nsid.as_str()).collect();

    for route in &routes {
        if route.nsid.starts_with(UPSTREAM_NSID_PREFIX) {
            continue;
        }

        let Some(doc) = lexicons.get(&route.nsid) else {
            report.routes_without_lexicon.push(route.nsid.clone());
            continue;
        };
        let Some(def) = main_def(doc) else {
            report.routes_without_lexicon.push(route.nsid.clone());
            continue;
        };

        let kind = def.get("type").and_then(Value::as_str).unwrap_or("query");
        if route.verb != expected_verb(kind) {
            report.failures.push(format!(
                "{}: lexicon says {kind} (expects {}) but the route is #[{}] ({})",
                route.nsid,
                expected_verb(kind),
                route.verb,
                route.source
            ));
        }

        let declared = lexicon_params(def);

        for (name, optionality) in &route.params {
            match (declared.get(name), optionality) {
                (None, Optionality::Required) => {
                    if !excused(&route.nsid, name) {
                        report.failures.push(format!(
                            "{}: route requires `{name}` but no lexicon declares it, so every existing client 404s ({})",
                            route.nsid, route.source
                        ));
                    }
                }
                (None, Optionality::Optional) => report.warnings.push(format!(
                    "{}: route accepts optional `{name}`, not yet in the lexicon",
                    route.nsid
                )),
                (None, Optionality::Unknown) => report.warnings.push(format!(
                    "{}: could not parse the type of `{name}` from the handler signature ({})",
                    route.nsid, route.source
                )),
                (Some(false), Optionality::Required) if !excused(&route.nsid, name) => {
                    report.failures.push(format!(
                        "{}: lexicon marks `{name}` optional but the route requires it, so clients omitting it 404 ({})",
                        route.nsid, route.source
                    ));
                }
                _ => {}
            }
        }

        for (name, required) in &declared {
            if route.params.contains_key(name) {
                continue;
            }
            if *required && !excused(&route.nsid, name) {
                report.failures.push(format!(
                    "{}: lexicon requires `{name}` but the route does not accept it ({})",
                    route.nsid, route.source
                ));
            } else if !*required {
                report.warnings.push(format!(
                    "{}: lexicon declares optional `{name}`, the route ignores it",
                    route.nsid
                ));
            }
        }
    }

    for (nsid, doc) in &lexicons {
        if main_def(doc).is_some() && !routed.contains(nsid.as_str()) {
            report.lexicons_without_route.push(nsid.clone());
        }
    }

    report
}

fn print_list(title: &str, items: &[String]) {
    if items.is_empty() {
        return;
    }
    println!("\n{title} ({}):", items.len());
    for item in items {
        println!("  - {item}");
    }
}

#[test]
fn routes_and_lexicons_do_not_contradict_each_other() {
    let report = build_report();

    print_list(
        "Routes with no lexicon yet (AppView leading, not a failure)",
        &report.routes_without_lexicon,
    );
    print_list(
        "Lexicon methods with no route (not a failure)",
        &report.lexicons_without_route,
    );
    print_list("Warnings", &report.warnings);

    assert!(
        report.failures.is_empty(),
        "\n{} lexicon mismatch(es):\n{}\n",
        report.failures.len(),
        report
            .failures
            .iter()
            .map(|f| format!("  - {f}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn exceptions_have_not_expired() {
    let exceptions = load_exceptions();
    let today = today();

    let expired: Vec<String> = exceptions
        .exception
        .iter()
        .filter(|e| e.expires < today)
        .map(|e| {
            format!(
                "{} / {} expired on {} ({})",
                e.nsid, e.param, e.expires, e.reason
            )
        })
        .collect();

    assert!(
        expired.is_empty(),
        "\nExpired exceptions. Resolve them or extend the date:\n{}\n",
        expired
            .iter()
            .map(|e| format!("  - {e}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn every_route_parses_its_handler_signature() {
    let unknown: Vec<String> = parse_routes()
        .iter()
        .flat_map(|r| {
            r.params
                .iter()
                .filter(|(_, o)| **o == Optionality::Unknown)
                .map(|(name, _)| format!("{}::{name} ({})", r.nsid, r.source))
                .collect::<Vec<_>>()
        })
        .collect();

    assert!(
        unknown.is_empty(),
        "\nHandler signature parsing missed {} param(s). This does not fail the \
         lexicon check (they degrade to warnings), but the regex should be fixed:\n{}\n",
        unknown.len(),
        unknown
            .iter()
            .map(|u| format!("  - {u}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
