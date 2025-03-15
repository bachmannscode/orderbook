use anyhow::Result;
use config::Config;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Sender, channel};
use tokio::sync::watch;
use tokio::time::{Duration, interval, sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::{Bytes, Message};

#[derive(Deserialize, Debug)]
struct ExchangeInfo<'a> {
    #[serde(borrow)]
    symbols: Vec<Symbol<'a>>,
}

#[derive(Deserialize, Debug)]
struct Symbol<'a> {
    pair: &'a str,
    filters: Vec<Filter<'a>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Filter<'a> {
    filter_type: &'a str,
    tick_size: Option<&'a str>,
    step_size: Option<&'a str>,
}

#[derive(Deserialize)]
pub struct AppConfig {
    pub global: GlobalConfig,
    pub exchange: ExchangeConfig,
}

#[derive(Deserialize)]
pub struct GlobalConfig {
    pub symbol: String,
    pub print_period: u64,
}

#[derive(Deserialize)]
pub struct ExchangeConfig {
    pub rest_base_url: String,
    pub ws_base_url: String,
}

pub fn load_config() -> AppConfig {
    let settings = Config::builder()
        .add_source(config::File::with_name("config.toml"))
        .build()
        .expect("Failed to load config.");
    settings.try_deserialize().expect("invalid config format")
}

struct AppState<T>
where
    T: ExchangeConnector,
{
    connector: T,
    config: GlobalConfig,
    order_book: Mutex<OrderBook>,
}

impl<T> AppState<T>
where
    T: ExchangeConnector,
{
    pub fn new(connector: T, config: GlobalConfig, order_book: OrderBook) -> Self {
        Self {
            connector,
            config,
            order_book: order_book.into(),
        }
    }
}

#[derive(Deserialize, Debug)]
// (price, quantity)
struct OrderResponse<'a>(&'a str, &'a str);

impl OrderResponse<'_> {
    fn into_order(self, tick_size: u8, step_size: u8) -> Order {
        Order {
            price: to_fixed_point(self.0, tick_size),
            quantity: to_fixed_point(self.1, step_size),
        }
    }
}

#[derive(Debug)]
struct Order {
    price: u64,
    quantity: u64,
}

#[derive(Deserialize, Debug)]
struct SnapshotResponse<'a> {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    #[serde(borrow)]
    bids: Vec<OrderResponse<'a>>,
    #[serde(borrow)]
    asks: Vec<OrderResponse<'a>>,
}

impl SnapshotResponse<'_> {
    fn into_snapshot(self, tick_size: u8, step_size: u8) -> Snapshot {
        Snapshot {
            last_update_id: self.last_update_id,
            bids: self
                .bids
                .into_iter()
                .map(|o| o.into_order(tick_size, step_size))
                .collect(),
            asks: self
                .asks
                .into_iter()
                .map(|o| o.into_order(tick_size, step_size))
                .collect(),
        }
    }
}

#[derive(Debug)]
struct Snapshot {
    last_update_id: u64,
    bids: Vec<Order>,
    asks: Vec<Order>,
}

#[derive(Deserialize)]
struct DepthUpdateResponse<'a> {
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "pu")]
    prev_update_id: u64,
    #[serde(rename = "b", borrow)]
    bids: Vec<OrderResponse<'a>>,
    #[serde(rename = "a", borrow)]
    asks: Vec<OrderResponse<'a>>,
}

impl DepthUpdateResponse<'_> {
    fn into_update(self, tick_size: u8, step_size: u8) -> DepthUpdate {
        DepthUpdate {
            first_update_id: self.first_update_id,
            final_update_id: self.final_update_id,
            prev_update_id: self.prev_update_id,
            bids: self
                .bids
                .into_iter()
                .map(|o| o.into_order(tick_size, step_size))
                .collect(),
            asks: self
                .asks
                .into_iter()
                .map(|o| o.into_order(tick_size, step_size))
                .collect(),
        }
    }
}

#[derive(Debug)]
struct DepthUpdate {
    first_update_id: u64,
    final_update_id: u64,
    prev_update_id: u64,
    bids: Vec<Order>,
    asks: Vec<Order>,
}

pub struct OrderBook {
    // Contains price as key and quantity as value.
    bids: BTreeMap<u64, u64>,
    asks: BTreeMap<u64, u64>,
    last_update_id: u64,
}

impl OrderBook {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0,
        }
    }

    fn update_bid(&mut self, bid: &Order) {
        let _ = match bid.quantity {
            0 => self.bids.remove(&bid.price),
            _ => self.bids.insert(bid.price, bid.quantity),
        };
    }

    fn update_ask(&mut self, ask: &Order) {
        let _ = match ask.quantity {
            0 => self.asks.remove(&ask.price),
            _ => self.asks.insert(ask.price, ask.quantity),
        };
    }
}

#[derive(PartialEq, Debug)]
enum Status {
    Online,
    Offline,
}

trait ExchangeConnector {
    async fn fetch_precision_info(
        exchange_config: &ExchangeConfig,
        global_config: &GlobalConfig,
    ) -> Result<(u8, u8)>;
    fn price_precision(&self) -> u8;
    fn quantity_precision(&self) -> u8;
    async fn get_snapshot(&self, config: &GlobalConfig) -> Result<Snapshot>;
    async fn stream_order_book_updates(
        &self,
        sender: Sender<DepthUpdate>,
        config: &GlobalConfig,
    ) -> Result<()>;
    fn set_status(&self, status: Status);
}

pub struct BinanceConnector {
    config: ExchangeConfig,
    price_precision: u8,
    quantity_precision: u8,
    status_sender: watch::Sender<Status>,
}

impl BinanceConnector {
    async fn new(
        exchange_config: ExchangeConfig,
        global_config: &GlobalConfig,
        status_sender: watch::Sender<Status>,
    ) -> Self {
        let (price_precision, quantity_precision) = loop {
            match BinanceConnector::fetch_precision_info(&exchange_config, global_config).await {
                // Retry fetching info in case of network issues.
                Err(_) => {
                    sleep(Duration::from_secs(5)).await;
                }
                Ok(info) => break info,
            }
        };
        Self {
            config: exchange_config,
            price_precision,
            quantity_precision,
            status_sender,
        }
    }
}

impl ExchangeConnector for BinanceConnector {
    async fn fetch_precision_info(
        exchange_config: &ExchangeConfig,
        global_config: &GlobalConfig,
    ) -> Result<(u8, u8)> {
        let raw_info = reqwest::get(format!("{}/exchangeInfo", exchange_config.rest_base_url))
            .await?
            .bytes()
            .await
            .unwrap();
        let info = serde_json::from_slice::<ExchangeInfo>(&raw_info).unwrap();
        let mut tick_size = None;
        let mut step_size = None;
        info.symbols
            .into_iter()
            .find(|s| s.pair == global_config.symbol)
            .unwrap()
            .filters
            .into_iter()
            .for_each(|f| match f.filter_type {
                "PRICE_FILTER" => tick_size = f.tick_size,
                "LOT_SIZE" => step_size = f.step_size,
                _ => {}
            });
        Ok((
            count_significant_decimal_digits(tick_size.unwrap()),
            count_significant_decimal_digits(step_size.unwrap()),
        ))
    }

    fn price_precision(&self) -> u8 {
        self.price_precision
    }

    fn quantity_precision(&self) -> u8 {
        self.quantity_precision
    }

    async fn get_snapshot(&self, config: &GlobalConfig) -> Result<Snapshot> {
        let snapshot_bytes = reqwest::get(format!(
            "{}/depth?symbol={}&limit=1000",
            self.config.rest_base_url, config.symbol
        ))
        .await?
        .bytes()
        .await
        .unwrap();
        Ok(serde_json::from_slice::<SnapshotResponse>(&snapshot_bytes)
            .unwrap()
            .into_snapshot(self.price_precision, self.quantity_precision))
    }

    async fn stream_order_book_updates(
        &self,
        sender: Sender<DepthUpdate>,
        config: &GlobalConfig,
    ) -> Result<()> {
        let ws_url = format!(
            "{}/{}@depth",
            self.config.ws_base_url,
            config.symbol.to_ascii_lowercase()
        );
        let (ws_stream, _) = connect_async(ws_url).await?;
        let (mut outgoing, mut incoming) = ws_stream.split();
        let (price_precision, quantity_precision) = (self.price_precision, self.quantity_precision);
        let mut ping_interval = interval(Duration::from_secs(5));

        tokio::spawn(async move {
            loop {
                ping_interval.tick().await;
                if outgoing.send(Message::Ping(Bytes::new())).await.is_err() {
                    break;
                }
            }
        });

        tokio::spawn(async move {
            loop {
                // detect connection issues within 10 seconds
                match timeout(Duration::from_secs(10), incoming.next()).await {
                    Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Pong(_)))) => {
                        continue;
                    }
                    Ok(Some(Ok(m))) => {
                        let raw_msg = m.into_data();
                        let resp = serde_json::from_slice::<DepthUpdateResponse>(&raw_msg);
                        let Ok(resp) = resp else {
                            // ignore isolated timestamp that frequently arrives
                            serde_json::from_slice::<u64>(&raw_msg).unwrap();
                            continue;
                        };
                        sender
                            .send(resp.into_update(price_precision, quantity_precision))
                            .await
                            .unwrap();
                    }
                    _ => break,
                }
            }
        });

        Ok(())
    }

    fn set_status(&self, status: Status) {
        self.status_sender.send(status).unwrap();
    }
}

async fn process_order_book<T>(state: Arc<AppState<T>>) -> Result<()>
where
    T: ExchangeConnector,
{
    let (sender, mut receiver) = channel(1000);
    // In case connecting to websocket fails, error out.
    state
        .connector
        .stream_order_book_updates(sender, &state.config)
        .await?;
    loop {
        // In case the snapshot fails due to network issues, error out to
        // avoid a buildup of outdated streaming updates.
        let snapshot = state.connector.get_snapshot(&state.config).await?;
        {
            let mut order_book = state.order_book.lock().await;
            order_book.last_update_id = snapshot.last_update_id;
            snapshot
                .bids
                .into_iter()
                .for_each(|bid| order_book.update_bid(&bid));
            snapshot
                .asks
                .into_iter()
                .for_each(|ask| order_book.update_ask(&ask));
        }
        state.connector.set_status(Status::Online);
        loop {
            // In case the sender drops due to network issues, error out
            // to avoid missing updates.
            let update = receiver
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("dropped sender"))?;
            let mut order_book = state.order_book.lock().await;
            if update.final_update_id < order_book.last_update_id {
                continue;
            }
            if !(update.first_update_id <= order_book.last_update_id
                && order_book.last_update_id <= update.final_update_id)
                && update.prev_update_id != order_book.last_update_id
            {
                *order_book = OrderBook::new();
                break;
            }
            update
                .bids
                .into_iter()
                .for_each(|bid| order_book.update_bid(&bid));
            update
                .asks
                .into_iter()
                .for_each(|ask| order_book.update_ask(&ask));
            order_book.last_update_id = update.final_update_id;
        }
    }
}

async fn handle_order_book<T>(state: Arc<AppState<T>>)
where
    T: ExchangeConnector,
{
    while process_order_book(state.clone()).await.is_err() {
        *state.order_book.lock().await = OrderBook::new();
        state.connector.set_status(Status::Offline);
        sleep(Duration::from_secs(5)).await;
    }
}

// periodically print the TOB without heap allocation
async fn periodic_print<T>(state: Arc<AppState<T>>, status: watch::Receiver<Status>)
where
    T: ExchangeConnector,
{
    let mut print_interval = interval(Duration::from_secs(state.config.print_period));
    // consume initial tick to let order book populate first
    print_interval.tick().await;
    let price_divisor = 10u64.pow(state.connector.price_precision().into());
    let quantity_divisor = 10u64.pow(state.connector.quantity_precision().into());
    let mut highest_bids = [(0, 0); 5];
    let mut lowest_asks = [(0, 0); 5];
    loop {
        print_interval.tick().await;
        if *status.borrow() == Status::Offline {
            println!("Top-of-book out of sync.");
            continue;
        }
        let order_book = state.order_book.lock().await;
        let bids = order_book.bids.iter().rev();
        let asks = order_book.asks.iter();
        if bids.clone().nth(4).is_none() || asks.clone().nth(4).is_none() {
            continue;
        }
        bids.take(5)
            .enumerate()
            .for_each(|(idx, (&price, &quantity))| highest_bids[idx] = (price, quantity));
        asks.take(5)
            .enumerate()
            .for_each(|(idx, (&price, &quantity))| lowest_asks[idx] = (price, quantity));
        let max_bid_quantity_width = highest_bids
            .iter()
            .map(|(_, fixed_point_q)| (fixed_point_q))
            .max()
            // This counts the digits before the decimal point.
            // use `.max(1)` to ensure we dont take log(0)
            .map(|fixed_point_q| (fixed_point_q / quantity_divisor).max(1).ilog10() + 1)
            .unwrap();
        let max_bid_price_width = (highest_bids[0].0 / price_divisor).max(1).ilog10() + 1;
        let max_ask_quantity_width = lowest_asks
            .iter()
            .map(|(_, fixed_point_q)| (fixed_point_q))
            .max()
            .map(|fixed_point_q| (fixed_point_q / quantity_divisor).max(1).ilog10() + 1)
            .unwrap();
        let max_ask_price_width = (lowest_asks[4].0 / price_divisor).max(1).ilog10() + 1;
        println!(
            "Top-of-book at {}:",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
        (1..=5).zip(highest_bids).zip(lowest_asks).for_each(
            |((idx, (bid_price, bid_quantity)), (ask_price, ask_quantity))| {
                print!(
                    "{idx}          {:>bid_q_width$}",
                    bid_quantity / quantity_divisor,
                    bid_q_width = max_bid_quantity_width as usize
                );
                if state.connector.quantity_precision() != 0 {
                    print!(
                        ".{:0quantity_width$}",
                        bid_quantity % quantity_divisor,
                        quantity_width = state.connector.quantity_precision() as usize
                    );
                }
                print!(
                    " -     {:>bid_p_width$}",
                    bid_price / price_divisor,
                    bid_p_width = max_bid_price_width as usize,
                );
                if state.connector.price_precision() != 0 {
                    print!(
                        ".{:0price_width$}",
                        bid_price % price_divisor,
                        price_width = state.connector.price_precision() as usize,
                    );
                }
                print!(
                    " | {:>ask_p_width$}",
                    ask_price / price_divisor,
                    ask_p_width = max_ask_price_width as usize,
                );
                if state.connector.price_precision() != 0 {
                    print!(
                        ".{:0price_width$}",
                        ask_price % price_divisor,
                        price_width = state.connector.price_precision() as usize,
                    );
                }
                print!(
                    "     - {:>ask_q_width$}",
                    ask_quantity / quantity_divisor,
                    ask_q_width = max_ask_quantity_width as usize
                );
                if state.connector.quantity_precision() != 0 {
                    print!(
                        ".{:0quantity_width$}",
                        ask_quantity % quantity_divisor,
                        quantity_width = state.connector.quantity_precision() as usize
                    );
                }
                println!();
            },
        );
    }
}

pub async fn run() {
    let config = load_config();
    let (status_sender, status_receiver) = watch::channel::<Status>(Status::Online);
    let connector = BinanceConnector::new(config.exchange, &config.global, status_sender).await;
    let state = Arc::new(AppState::new(connector, config.global, OrderBook::new()));
    tokio::spawn(handle_order_book(state.clone()));
    tokio::spawn(periodic_print(state.clone(), status_receiver));
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    }
}

// convert a shared string slice reference to a u64 without heap allocation
fn to_fixed_point(input: &str, decimal_places: u8) -> u64 {
    // u64::MAX has 20 digits
    let mut buffer = [b'0'; 20];
    let mut len = 0;
    if let Some((pre_comma, post_comma)) = input.split_once('.') {
        for &b in pre_comma.as_bytes() {
            buffer[len] = b;
            len += 1;
        }
        for &b in post_comma.as_bytes().iter().take(decimal_places as usize) {
            buffer[len] = b;
            len += 1;
        }
        let number_str = std::str::from_utf8(&buffer[..len]).unwrap();
        number_str.parse().unwrap()
    } else if decimal_places == 0 {
        input.parse().unwrap()
    } else {
        for &b in input.as_bytes() {
            buffer[len] = b;
            len += 1;
        }
        let number_str = std::str::from_utf8(&buffer[..(len + decimal_places as usize)]).unwrap();
        number_str.parse().unwrap()
    }
}

fn count_significant_decimal_digits(num: &str) -> u8 {
    num.split_once('.')
        // Notice that the unicode encoding of all possible characters ('.' or 0-9)
        // is 1 byte long so we can count bytes instead of characters with `rfind`.
        .and_then(|(_, rhs)| rhs.rfind(|c| c != '0'))
        // in case num is "1.0" or "1" default to zero
        .map_or_else(|| 0, |s| s + 1) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_fixed_point() {
        assert_eq!(to_fixed_point("123", 2), 12300);
        assert_eq!(to_fixed_point("123", 0), 123);
        assert_eq!(to_fixed_point("123.45000", 3), 123450);
        assert_eq!(to_fixed_point("123.00000", 0), 123);
        assert_eq!(to_fixed_point("18446744073709551615", 0), u64::MAX);
        assert_eq!(to_fixed_point("18446744073709551615.0", 0), u64::MAX);
        assert_eq!(to_fixed_point("184467440737095516.15", 2), u64::MAX);
        assert_eq!(to_fixed_point("0", 0), 0);
        assert_eq!(to_fixed_point("0", 5), 0);
        assert_eq!(to_fixed_point("1", 5), 100000);
    }

    #[test]
    fn test_count_significant_decimal_digits() {
        assert_eq!(count_significant_decimal_digits("0.00067"), 5);
        assert_eq!(count_significant_decimal_digits("1.0"), 0);
        assert_eq!(count_significant_decimal_digits("1"), 0);
        assert_eq!(count_significant_decimal_digits("0.9"), 1);
        assert_eq!(count_significant_decimal_digits("4.02"), 2);
    }
}
