import pytest

from ds_from_scratch.sim.testing import SimulationBuilder

@pytest.fixture
def simulation_builder():
    return SimulationBuilder()
