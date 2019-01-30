import sys
from subprocess import check_output

from autodora.experiment import Experiment, Parameter, derived, Result


def callback(x, y, count, power):
    check_output("for i in `seq 1 {count}`; do echo $i; done".format(count=count), shell=True)
    print("Done computing with BASH")
    print("No errors occurred", file=sys.stderr)
    return (x * y) ** power


class ProductExperiment(Experiment):
    input = Parameter(str, "0x0", "The input values to be multiplied (e.g. 0x10)")
    count = Parameter(int, 10, "The number of times to count in bash")
    power = Parameter(int, 2, "The power to apply at the end")
    product = Result(int, None, "Result of the product")

    @derived(cache=True)
    def derived_x(self):
        return int(self.get(self.input).split("x")[0])

    @derived(cache=True)
    def derived_y(self):
        return int(self.get(self.input).split("x")[1])

    @derived(cache=False)
    def derived_x_square(self):
        return self.get("x") ** 2

    def run_internal(self):
        x, y = self.get("x"), self.get("y")
        result = callback(x, y, self["count"], self["power"])
        self.result["product"] = result


ProductExperiment.enable_cli()
