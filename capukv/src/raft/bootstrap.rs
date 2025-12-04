use tonic::{Request, Response, Result};

pub(crate) struct Bootstrapper {
    pub(crate) id: Vec<u8>,
    /// A uuid (bytes)
    pub(crate) heard_from: tokio::sync::mpsc::Sender<Vec<u8>>,
}

#[tonic::async_trait]
impl proto::init_service_server::InitService for Bootstrapper {
    async fn greet(&self, req: Request<proto::GreetRequest>) -> Result<Response<proto::GreetResponse>> {
        let (_, _, req) = req.into_parts();
        self.heard_from.send(req.id).await.expect("Error sending on bootstrapping channel");
        Ok(proto::GreetResponse { id: self.id.clone() }.into())
    }
}
