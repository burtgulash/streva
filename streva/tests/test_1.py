from streva.actor import Actor
from streva.reactor import Reactor

import pytest


class Test(Actor):

    @handler_for("receive")
    def receive(self, msg):
        assert msg == "TEST"

def setup():
    return Test("tester"), Reactor()


@pytest.mark.timeout(1)
def test_send_set_start():
    actor, reactor = setup()

    actor.send("receive", "TEST")
    actor.set_reactor(reactor)
    actor.start()

    reactor.start()
    assert True


@pytest.mark.timeout(1)
def test_send_start_set():
    actor, reactor = setup()

    actor.send("receive", "TEST")
    actor.start()
    actor.set_reactor(reactor)

    reactor.start()
    assert True


@pytest.mark.timeout(1)
def test_start_send_set():
    actor, reactor = setup()

    actor.start()
    actor.send("receive", "TEST")
    actor.set_reactor(reactor)

    reactor.start()
    assert True

@pytest.mark.timeout(1)
def test_start_set_send():
    actor, reactor = setup()

    actor.start()
    actor.set_reactor(reactor)
    actor.send("receive", "TEST")

    reactor.start()
    assert True

@pytest.mark.timeout(1)
def test_set_send_start():
    actor, reactor = setup()

    actor.set_reactor(reactor)
    actor.send("receive", "TEST")
    actor.start()

    reactor.start()
    assert True

@pytest.mark.timeout(1)
def test_set_start_send():
    actor, reactor = setup()

    actor.set_reactor(reactor)
    actor.start()
    actor.send("receive", "TEST")

    reactor.start()
    assert True

