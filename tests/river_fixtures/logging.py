import logging
from typing import Generator

import pytest
from pytest import LogCaptureFixture


class NoErrors:
    """
    A pivot point which can be used to assert that we have received
    no errors during a test run.
    """

    caplog: LogCaptureFixture

    def __init__(self, caplog: LogCaptureFixture):
        self.caplog = caplog

    def __call__(self) -> None:
        assert len(self.caplog.get_records("setup")) == 0
        assert len(self.caplog.get_records("call")) == 0
        assert len(self.caplog.get_records("teardown")) == 0


@pytest.fixture
def no_logging_error(caplog: LogCaptureFixture) -> Generator[NoErrors, None, None]:
    with caplog.at_level(logging.ERROR):
        yield NoErrors(caplog)
