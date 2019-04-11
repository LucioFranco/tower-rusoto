use bytes::Buf;
use futures::{Async, Future, Poll, Stream};
use http::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Method, Request,
};
use rusoto_core::{
    request::{DispatchSignedRequest, Headers, HttpDispatchError, HttpResponse},
    signature::{SignedRequest, SignedRequestPayload},
    ByteStream,
};
use std::{io, time::Duration};
use tokio_buf::BufStream;
use tower_http::{Body, BodyExt, HttpService};

#[derive(Clone)]
pub struct HttpClient<T> {
    client: T,
}

pub struct RusotoBody {
    inner: Option<SignedRequestPayload>,
}

struct BodyStream<T> {
    body: T,
}

impl<T> HttpClient<T> {
    pub fn new(client: T) -> Self {
        HttpClient { client }
    }
}

impl<T> DispatchSignedRequest for HttpClient<T>
where
    T: HttpService<RusotoBody> + Clone,
    T::Future: Send + 'static,
    T::ResponseBody: Send + 'static,
    <T::ResponseBody as Body>::Error: Into<io::Error>,
    T::Error: Into<io::Error> + Send + 'static,
{
    type Future = Box<Future<Item = HttpResponse, Error = HttpDispatchError> + Send + 'static>;

    fn dispatch(&self, request: SignedRequest, timeout: Option<Duration>) -> Self::Future {
        assert!(timeout.is_none(), "timeout is not supported at this level");

        let method = match request.method().as_ref() {
            "POST" => Method::POST,
            "PUT" => Method::PUT,
            "DELETE" => Method::DELETE,
            "GET" => Method::GET,
            "HEAD" => Method::HEAD,
            v => unimplemented!(),
        };

        let mut headers = HeaderMap::new();
        for h in request.headers().iter() {
            let header_name = match h.0.parse::<HeaderName>() {
                Ok(name) => name,
                Err(err) => unimplemented!(),
            };
            for v in h.1.iter() {
                let header_value = match HeaderValue::from_bytes(v) {
                    Ok(value) => value,
                    Err(err) => unimplemented!(),
                };
                headers.append(&header_name, header_value);
            }
        }

        // TODO(lucio): set user-agent

        let mut uri = format!(
            "{}://{}{}",
            request.scheme(),
            request.hostname(),
            request.canonical_path()
        );

        if !request.canonical_query_string().is_empty() {
            uri += &format!("?{}", request.canonical_query_string());
        }

        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .body(RusotoBody::from(request.payload))
            .map_err(|e| format!("RequestBuildingError: {}", e))
            .unwrap();

        *request.headers_mut() = headers;

        let request = {
            let mut client = self.client.clone();
            client.call(request)
        };

        let fut = request
            .and_then(|response| {
                let status = response.status();
                let headers = Headers::new(response.headers().iter().map(|(h, v)| {
                    let value_string = v.to_str().unwrap().to_owned();
                    (h.as_str(), value_string)
                }));
                let body = response.into_body().into_buf_stream();
                let body = BodyStream { body };

                Ok(HttpResponse {
                    status: status,
                    headers: headers,
                    body: ByteStream::new(body),
                })
            })
            .map_err(|e| HttpDispatchError::new(format!("DispatchError: {}", e.into())));

        Box::new(fut)
    }
}

impl Body for RusotoBody {
    type Item = io::Cursor<Vec<u8>>;
    type Error = io::Error;

    fn poll_buf(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match &mut self.inner {
            Some(SignedRequestPayload::Buffer(buf)) => {
                if !buf.is_empty() {
                    let buf = io::Cursor::new(buf.split_off(0));
                    Ok(Async::Ready(Some(buf)))
                } else {
                    Ok(Async::Ready(None))
                }
            }
            Some(SignedRequestPayload::Stream(stream)) => match stream.poll()? {
                Async::Ready(Some(buffer)) => Ok(Async::Ready(Some(io::Cursor::new(buffer)))),
                Async::Ready(None) => Ok(Async::Ready(None)),
                Async::NotReady => Ok(Async::NotReady),
            },
            None => Ok(Async::Ready(None)),
        }
    }

    fn poll_trailers(&mut self) -> Poll<Option<HeaderMap>, Self::Error> {
        Ok(Async::Ready(None))
    }
}

impl From<Option<SignedRequestPayload>> for RusotoBody {
    fn from(inner: Option<SignedRequestPayload>) -> Self {
        RusotoBody { inner }
    }
}

impl<T> Stream for BodyStream<T>
where
    T: BufStream,
    T::Error: Into<io::Error>,
{
    type Item = Vec<u8>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.body.poll_buf().map_err(|e| e.into())? {
            Async::Ready(Some(buf)) => {
                let bytes = buf.collect::<Vec<u8>>();
                Ok(Async::Ready(Some(bytes)))
            }
            Async::Ready(None) => Ok(Async::Ready(None)),
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}
