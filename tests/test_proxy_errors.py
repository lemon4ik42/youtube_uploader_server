import pytest
from socksio.exceptions import ProtocolError


@pytest.mark.asyncio
async def test_proxy_check_treats_socks_protocol_error_as_failed(monkeypatch):
    import app.services.proxy_service as proxy_service

    class BrokenClient:
        async def get(self, *args, **kwargs):
            raise ProtocolError("Malformed reply")

    class FakeProxy:
        id = "proxy-id"

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, model, proxy_id):
            return None

        async def exec(self, query):
            class _Empty:
                def all(self):
                    return []

                def scalars(self):
                    return self

            return _Empty()

        async def commit(self):
            return None

    monkeypatch.setattr(proxy_service, "get_client", lambda _: BrokenClient())
    monkeypatch.setattr(proxy_service, "async_session_maker", lambda: FakeSession())

    result = await proxy_service._perform_shared_check(FakeProxy.id, "socks5://proxy")

    assert result is False
