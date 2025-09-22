pub mod timer;
pub use metrics;
pub use tracing::{self, debug, error, info, level_filters::LevelFilter, trace, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer, Registry};

static DEFAULT_FOLDER: &str = "./logs/";
static DEFAULT_EXTENSION: &str = ".log";
static DEFAULT_DIRECTIVE: &str = "info";

#[derive(Clone, Default)]
pub struct LoggerBuilder {
    name: String,

    disable_console: bool,
    disable_console_time: bool,
    disable_console_target: bool,
    console_default_directive: Option<String>,

    enable_file: bool,
    disable_file_target: bool,
    file_default_directive: Option<String>,

    folder: Option<String>,
}

impl LoggerBuilder {
    pub fn new<T: Into<String>>(name: T) -> Self {
        Self {
            name: name.into(),

            ..Default::default()
        }
    }

    pub fn enable_file(mut self) -> Self {
        self.enable_file = true;
        self
    }

    pub fn disable_file_target(mut self) -> Self {
        self.disable_file_target = true;
        self
    }

    pub fn folder<T: Into<String>>(mut self, folder: T) -> Self {
        self.folder = Some(folder.into());
        self
    }

    pub fn file_default_directive<T: Into<String>>(mut self, directive: T) -> Self {
        self.file_default_directive = Some(directive.into());
        self
    }

    pub fn disable_console(mut self) -> Self {
        self.disable_console = true;
        self
    }

    pub fn disable_console_target(mut self) -> Self {
        self.disable_console_target = true;
        self
    }

    pub fn disable_console_time(mut self) -> Self {
        self.disable_console_time = true;
        self
    }

    pub fn console_default_directive<T: Into<String>>(mut self, directive: T) -> Self {
        self.console_default_directive = Some(directive.into());
        self
    }

    pub fn init(self) {
        let registry = tracing_subscriber::registry();

        let mut layers = Vec::new();

        if self.enable_file {
            let directive = self
                .file_default_directive
                .unwrap_or_else(|| DEFAULT_DIRECTIVE.to_owned());

            let file_appender = tracing_appender::rolling::hourly(
                self.folder.unwrap_or(DEFAULT_FOLDER.to_string()),
                format!("{}{}", self.name, DEFAULT_EXTENSION),
            );

            let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

            let filter = EnvFilter::builder()
                .with_default_directive(directive.parse().unwrap())
                .with_env_var("RUST_FILE_LOG")
                .from_env_lossy();

            let file_layer = fmt::layer::<Registry>()
                .with_ansi(false)
                .with_writer(non_blocking)
                .with_target(!self.disable_file_target)
                .with_filter(filter)
                .boxed();

            layers.push(file_layer);
            Box::leak(Box::new(_guard));
        }

        if !self.disable_console {
            let directive = self
                .console_default_directive
                .unwrap_or_else(|| DEFAULT_DIRECTIVE.to_owned());

            let filter = EnvFilter::builder()
                .with_default_directive(directive.parse().unwrap())
                .from_env_lossy();

            if !self.disable_console_time {
                let console_layer = fmt::layer::<Registry>()
                    .with_target(!self.disable_console_target)
                    .with_filter(filter)
                    .boxed();

                layers.push(console_layer);
            } else {
                let console_layer = fmt::layer::<Registry>()
                    .without_time()
                    .with_target(!self.disable_console_target)
                    .with_filter(filter)
                    .boxed();

                layers.push(console_layer);
            }
        }

        registry.with(layers).init();
    }
}

pub fn builder(name: &str) -> LoggerBuilder {
    LoggerBuilder::new(name)
}

pub fn console() {
    LoggerBuilder::default().init()
}

pub fn console_no_target() {
    LoggerBuilder::default().disable_console_target().init()
}
