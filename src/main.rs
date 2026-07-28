fn main() {
    if let Err(err) = delta_stream::run() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}
