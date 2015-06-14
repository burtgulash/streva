from streva.actor import Process
from streva.reactor import Reactor

import pytest


class Test(Process):

    @handler_for("receive")
    def receive(self, msg):
        assert msg == "TEST"

def setup():
    return Test(), Reactor()


@pytest.mark.timeout(1)
def test_send_set_start():
    process, reactor = setup()

    process.send("receive", "TEST")
    process.set_reactor(reactor)
    process.start()

    reactor.start()
    assert True


@pytest.mark.timeout(1)
def test_send_start_set():
    process, reactor = setup()

    process.send("receive", "TEST")
    process.start()
    process.set_reactor(reactor)

    reactor.start()
    assert True


@pytest.mark.timeout(1)
def test_start_send_set():
    process, reactor = setup()

    process.start()
    process.send("receive", "TEST")
    process.set_reactor(reactor)

    reactor.start()
    assert True

@pytest.mark.timeout(1)
def test_start_set_send():
    process, reactor = setup()

    process.start()
    process.set_reactor(reactor)
    process.send("receive", "TEST")

    reactor.start()
    assert True

@pytest.mark.timeout(1)
def test_set_send_start():
    process, reactor = setup()

    process.set_reactor(reactor)
    process.send("receive", "TEST")
    process.start()

    reactor.start()
    assert True

@pytest.mark.timeout(1)
def test_set_start_send():
    process, reactor = setup()

    process.set_reactor(reactor)
    process.start()
    process.send("receive", "TEST")

    reactor.start()
    assert True

