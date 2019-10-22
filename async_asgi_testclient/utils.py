from asyncio import Future, Queue
from functools import partial

import asyncio
import sys
from typing import AsyncGenerator, Tuple, TypeVar, AsyncIterable, Coroutine, Optional, \
    Union, Type

from async_asgi_testclient.types import Send, Receive

T = TypeVar("T")


async def is_last_one(gen: AsyncIterable[bytes]) -> AsyncGenerator[Tuple[bool, bytes], bytes]:
    prev_el = None
    async for el in gen:
        prev_el = el
        async for el in gen:
            yield (False, prev_el)
            prev_el = el
        yield (True, prev_el)


class Message:
    def __init__(self, event: str, reason: Union[str, BaseException, Type[asyncio.TimeoutError]], task: Optional[asyncio.Task]) -> None:
        self.event = event
        self.reason = reason
        self.task = task


def create_monitored_task(coro: Coroutine, send: Send) -> Future[T]:
    future = asyncio.ensure_future(coro)
    future.add_done_callback(partial(_callback, send))
    return future


async def receive(ch: Queue, timeout: Optional[float]=None) -> Receive:
    fut = set_timeout(ch, timeout)
    msg = await ch.get()
    if not fut.cancelled():
        fut.cancel()
    if isinstance(msg, Message):
        if msg.event == "err":
            raise msg.reason
    return msg


def _callback(send, fut):
    try:
        fut.result()
    except asyncio.CancelledError:
        send(Message("exit", "killed", fut))
        raise
    except Exception as e:
        send(Message("err", e, fut))
    else:
        send(Message("exit", "normal", fut))


async def _send_after(timeout, queue, msg) -> None:
    if timeout is None:
        return
    await asyncio.sleep(timeout)
    await queue.put(msg)


def set_timeout(queue: Queue, timeout: Optional[float]) -> Future[T]:
    msg = Message("err", asyncio.TimeoutError, current_task())
    return asyncio.ensure_future(_send_after(timeout, queue, msg))


def current_task() -> Optional[asyncio.Task]:
    PY37 = sys.version_info >= (3, 7)
    if PY37:
        return asyncio.current_task()
    else:
        return asyncio.Task.current_task()
