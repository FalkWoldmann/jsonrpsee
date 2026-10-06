//! Server traits use native `async fn` and the futures returned by them must be `Send`,
//! also when they are called with borrowed arguments from generic code.

use std::borrow::Cow;
use std::sync::Arc;

use jsonrpsee::core::{RpcResult, SubscriptionResult, to_json_raw_value};
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::server::ServerBuilder;
use jsonrpsee::types::{ErrorObject, ErrorObjectOwned};
use jsonrpsee::{Extensions, PendingSubscriptionSink, rpc_params};

pub trait Config {
	type Hash: Send + Sync + 'static;
}

impl Config for () {
	type Hash = u64;
}

pub struct MyError;

impl From<MyError> for ErrorObjectOwned {
	fn from(_: MyError) -> Self {
		ErrorObject::owned(1, "my_error", None::<()>)
	}
}

#[rpc(server, namespace = "foo", server_bounds(Conf::Hash: jsonrpsee::core::Serialize + jsonrpsee::core::DeserializeOwned + Clone))]
pub trait Rpc<Conf: Config> {
	#[method(name = "hash")]
	async fn hash(&self, hash: Conf::Hash) -> RpcResult<Conf::Hash>;

	#[method(name = "borrowed")]
	async fn borrowed(&self, a: &str, b: Cow<'_, str>) -> RpcResult<String>;

	#[method(name = "ext", with_extensions)]
	async fn ext(&self, a: &str) -> RpcResult<bool>;

	#[method(name = "customErr")]
	async fn custom_err(&self) -> Result<(), MyError>;

	#[method(name = "sync")]
	fn sync(&self, a: &str) -> RpcResult<String>;

	#[method(name = "blocking", blocking)]
	fn blocking(&self) -> RpcResult<u8>;

	#[method(name = "default", with_extensions)]
	async fn default(&self, a: u8, b: String) -> RpcResult<String> {
		let ext_found = ext.get::<u32>().is_some();
		std::future::ready(()).await;
		Ok(format!("{} {b} {ext_found}", a + 1))
	}

	#[subscription(name = "sub", unsubscribe = "unsub", item = Conf::Hash, with_extensions)]
	async fn sub(&self, hash: Conf::Hash) -> SubscriptionResult;

	#[subscription(name = "subDefault", unsubscribe = "unsubDefault", item = u8)]
	async fn sub_default(&self, val: u8) -> SubscriptionResult {
		let sink = subscription_sink.accept().await?;
		sink.send(to_json_raw_value(&val)?).await?;
		Ok(())
	}

	#[subscription(name = "subSync", unsubscribe = "unsubSync", item = u8)]
	fn sub_sync(&self);
}

pub struct RpcServerImpl;

impl RpcServer<()> for RpcServerImpl {
	async fn hash(&self, hash: u64) -> RpcResult<u64> {
		Ok(hash)
	}

	async fn borrowed(&self, a: &str, b: Cow<'_, str>) -> RpcResult<String> {
		std::future::ready(()).await;
		Ok(format!("{a} {b}"))
	}

	async fn ext(&self, ext: &Extensions, a: &str) -> RpcResult<bool> {
		std::future::ready(()).await;
		Ok(ext.get::<u32>().is_some() && !a.is_empty())
	}

	async fn custom_err(&self) -> Result<(), MyError> {
		Err(MyError)
	}

	fn sync(&self, a: &str) -> RpcResult<String> {
		Ok(a.to_owned())
	}

	fn blocking(&self) -> RpcResult<u8> {
		Ok(1)
	}

	async fn sub(&self, pending: PendingSubscriptionSink, _ext: &Extensions, hash: u64) -> SubscriptionResult {
		let sink = pending.accept().await?;
		sink.send(to_json_raw_value(&hash)?).await?;
		Ok(())
	}

	fn sub_sync(&self, _pending: PendingSubscriptionSink) {}
}

/// Calls the server methods from generic code in a spawned task.
fn spawn_calls<Conf, T>(server: Arc<T>, hash: Conf::Hash) -> tokio::task::JoinHandle<()>
where
	Conf: Config + 'static,
	Conf::Hash: jsonrpsee::core::Serialize + jsonrpsee::core::DeserializeOwned + Clone,
	T: RpcServer<Conf>,
{
	tokio::spawn(async move {
		let ext = Extensions::new();
		let s = String::from("a");

		assert!(server.hash(hash).await.is_ok());
		assert_eq!(server.borrowed(&s, Cow::Borrowed(&s)).await.unwrap(), "a a");
		assert!(!server.ext(&ext, &s).await.unwrap());
		assert!(server.custom_err().await.is_err());
		assert_eq!(server.default(&ext, 1, s.clone()).await.unwrap(), "2 a false");
	})
}

#[tokio::main]
async fn main() {
	spawn_calls::<(), _>(Arc::new(RpcServerImpl), 1).await.unwrap();

	let module = RpcServerImpl.into_rpc();

	assert_eq!(module.call::<_, u64>("foo_hash", rpc_params![7]).await.unwrap(), 7);
	assert_eq!(module.call::<_, String>("foo_borrowed", rpc_params!["a", "b"]).await.unwrap(), "a b");
	assert!(!module.call::<_, bool>("foo_ext", rpc_params!["a"]).await.unwrap());
	assert!(module.call::<_, ()>("foo_customErr", rpc_params![]).await.is_err());
	assert_eq!(module.call::<_, String>("foo_sync", rpc_params!["a"]).await.unwrap(), "a");
	assert_eq!(module.call::<_, u8>("foo_blocking", rpc_params![]).await.unwrap(), 1);
	assert_eq!(module.call::<_, String>("foo_default", rpc_params![1, "b"]).await.unwrap(), "2 b false");

	let mut sub = module.subscribe_unbounded("foo_sub", rpc_params![3]).await.unwrap();
	assert_eq!(sub.next::<u64>().await.unwrap().unwrap().0, 3);

	let mut sub = module.subscribe_unbounded("foo_subDefault", rpc_params![5]).await.unwrap();
	assert_eq!(sub.next::<u8>().await.unwrap().unwrap().0, 5);

	let server = ServerBuilder::default().build("127.0.0.1:0").await.unwrap();
	let handle = server.start(module);
	handle.stop().unwrap();
	handle.stopped().await;
}
