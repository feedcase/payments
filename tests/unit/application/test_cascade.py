import asyncio

from modules.payments.application.cascade import StepResult
from modules.payments.domain.errors import FailureClass
from modules.payments.domain.statuses import AttemptStatus as A
from modules.payments.domain.statuses import PaymentStatus as P
from tests.fakes import FakeProvider, failure


async def test_first_provider_ok_depth_1(make_env):
    env = make_env(FakeProvider("p1"), FakeProvider("p2"))
    p = await env.new_payment()
    assert await env.cascade.run(p.id) == "completed"
    got = env.payment(p.id)
    assert got.status is P.AWAITING_CUSTOMER
    assert [a.provider for a in got.attempts] == ["p1"]
    assert env.breaker.records == [("p1", True)]


async def test_attempt_persisted_before_provider_call(make_env):
    p1 = FakeProvider("p1")
    p1.hold = asyncio.Event()
    env = make_env(p1, FakeProvider("p2"))
    p = await env.new_payment()
    task = asyncio.create_task(env.cascade.run(p.id))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert env.payment(p.id).attempts[0].status is A.CREATING
    p1.hold.set()
    await task


async def test_500_then_ok_second_provider(make_env):
    env = make_env(FakeProvider("p1", [failure(FailureClass.PROVIDER_ERROR)]), FakeProvider("p2"))
    p = await env.new_payment()
    assert await env.cascade.run(p.id) == "completed"
    got = env.payment(p.id)
    assert [(a.provider, a.status) for a in got.attempts] == [
        ("p1", A.CREATE_FAILED),
        ("p2", A.CREATED),
    ]
    assert got.status is P.AWAITING_CUSTOMER
    assert env.breaker.records == [("p1", False), ("p2", True)]


async def test_timeout_then_ok_first_superseded(make_env):
    env = make_env(FakeProvider("p1", [failure(FailureClass.TIMEOUT_UNKNOWN)]), FakeProvider("p2"))
    p = await env.new_payment()
    await env.cascade.run(p.id)
    got = env.payment(p.id)
    assert [a.status for a in got.attempts] == [A.SUPERSEDED, A.CREATED]


async def test_unsupported_not_penalized(make_env):
    env = make_env(FakeProvider("p1", [failure(FailureClass.UNSUPPORTED)]), FakeProvider("p2"))
    p = await env.new_payment()
    await env.cascade.run(p.id)
    assert env.breaker.records == [("p2", True)]


async def test_invalid_request_stops(make_env):
    p2 = FakeProvider("p2")
    env = make_env(FakeProvider("p1", [failure(FailureClass.INVALID_REQUEST, "http_401")]), p2)
    p = await env.new_payment()
    assert await env.cascade.run(p.id) == "completed"
    got = env.payment(p.id)
    assert got.status is P.FAILED and got.failure_reason == "INVALID_REQUEST:http_401"
    assert p2.created == []


async def test_all_fail_no_provider(make_env):
    env = make_env(
        FakeProvider("p1", [failure(FailureClass.PROVIDER_UNAVAILABLE)]),
        FakeProvider("p2", [failure(FailureClass.PROVIDER_ERROR)]),
    )
    p = await env.new_payment()
    await env.cascade.run(p.id)
    got = env.payment(p.id)
    assert got.status is P.FAILED and got.failure_reason == "no_provider_available"
    assert env.uow().events_of(p.id) == ["PaymentFailed"]


async def test_unexpected_exception_is_provider_error(make_env):
    class Boom(FakeProvider):
        async def create(self, cmd):
            raise KeyError("payment_id")

    env = make_env(Boom("p1"), FakeProvider("p2"))
    p = await env.new_payment()
    await env.cascade.run(p.id)
    got = env.payment(p.id)
    assert got.attempts[0].failure.cls is FailureClass.PROVIDER_ERROR
    assert got.status is P.AWAITING_CUSTOMER


async def test_deadline_defers(make_env):
    slow_fail = FakeProvider("p1", [("sleep", 13)])
    env = make_env(slow_fail, FakeProvider("p2"))

    async def create(cmd):
        env.clock.advance(13)
        raise failure(FailureClass.TIMEOUT_UNKNOWN)

    slow_fail.create = create
    p = await env.new_payment()
    assert await env.cascade.run(p.id) == "deferred"
    got = env.payment(p.id)
    assert got.status is P.ROUTING and len(got.attempts) == 1


async def test_timeout_passed_to_provider_is_bounded_by_deadline(make_env):
    p1 = FakeProvider("p1")
    env = make_env(p1)
    p = await env.new_payment()
    await env.cascade.run(p.id)
    assert p1.created[0].timeout_s == env.settings.read_timeout_s
    assert p1.created[0].callback_url == "http://cb/p1"
    assert p1.created[0].attempt_id == env.payment(p.id).attempts[0].id


async def test_bulkhead_full_local_limit_next(make_env):
    env = make_env(FakeProvider("p1"), FakeProvider("p2"))
    env.settings.bulkhead_per_provider = 1
    env.settings.bulkhead_wait_s = 0.01
    env.cascade._bulkheads.clear()
    sem = env.cascade._bulkhead("p1")
    await sem.acquire()
    p = await env.new_payment()
    await env.cascade.run(p.id)
    got = env.payment(p.id)
    assert got.attempts[0].failure.cls is FailureClass.LOCAL_LIMIT
    assert got.attempts[1].provider == "p2"
    assert ("p1", False) not in env.breaker.records


async def test_run_step_noop_for_finished_payment(make_env):
    env = make_env(FakeProvider("p1"))
    p = await env.new_payment()
    await env.cascade.run(p.id)
    assert await env.cascade.run_step(p.id, env.clock.monotonic() + 15) is StepResult.DONE
    assert len(env.payment(p.id).attempts) == 1
