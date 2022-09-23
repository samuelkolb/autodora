from autodora.experiment import Experiment, Parameter, derived, Result


class InventoryExperiment(Experiment):
    input: str = "0x0"
    power: int = 2
    exp: int

    @derived(cache=True)
    def x(self):
        return int(self.input.split("x")[0])

    @derived(cache=True)
    def y(self):
        return int(self.input.split("x")[1])

    def run(self):
        return {"exp": (self.x * self.y) ** self.power}


InventoryExperiment.enable_cli(cmd="python -m use_cases.inventory.experiment")
