from collections import defaultdict
from typing import Tuple, Union

# NOTE: it is assumed that old_value has been inserted in data previously for O(1) space complexity

# TODO: validate input with pydantic
# can check that "action" corresponds with existence of "old_value" and "new value"
# TODO: logging with chosen logging library
# log input payload, output values and exceptions


class MetricAggregation:
    def __init__(self) -> None:
        self.metrics = {}

    def handle_event(self, event: dict, context: dict) -> None:
        # make data class
        event = event["body"]
        variable = event["variable"]
        action = event["action"]
        value = event.get("value")
        old_value = event.get("old_value")
        self.update_metrics(action, variable, value, old_value)

        return None

    def get_metrics(self) -> dict:
        """return avg and std dev of all metrics"""
        out = {}
        for key in self.metrics:
            k = {
                k: v[0]
                for (k, v) in zip(self.metrics[key].keys(), self.metrics[key].values())
            }
            out[key] = k
        return out

    def update_metrics(
        self,
        action: str,
        variable: str,
        value: Union[float, None],
        old_value: Union[float, None],
    ) -> None:
        match action:
            case "insert":
                if self.metrics.get(variable) is None:
                    # initialize metric {metric_name: [value, value_count]}
                    self.metrics[variable] = defaultdict(lambda: [0, 0])
                return self.commit_updates(variable, value, action)
            case "modify":
                if self.metrics.get(variable) is None:
                    raise Exception("can not update non-existing variable")
                self.commit_updates(variable, old_value, "delete")
                self.commit_updates(variable, value, "insert")
            case "delete":
                if self.metrics.get(variable) is None:
                    raise Exception("can not delete non-existing variable")
                self.commit_updates(variable, old_value, action)

    def commit_updates(self, variable: str, value: float, action: str):
        old_avg, new_avg = self.update_avg(variable, value, action)
        self.update_std_dev(variable, value, old_avg, new_avg, action)

    def update_avg(
        self, variable: str, value: float, action: str
    ) -> Tuple[float, float]:
        if action == "delete":
            self.metrics[variable]["avg"][1] -= 1
            curr_avg = self.metrics[variable]["avg"][0]
            curr_cnt = self.metrics[variable]["avg"][1]
            new_avg = self.calculate_avg_delete(value, curr_avg, curr_cnt)
            self.metrics[variable]["avg"][0] = new_avg
        elif action == "insert":
            """insert"""
            self.metrics[variable]["avg"][1] += 1
            curr_avg = self.metrics[variable]["avg"][0]
            curr_cnt = self.metrics[variable]["avg"][1]
            new_avg = self.calculate_avg_insert(value, curr_avg, curr_cnt)

            self.metrics[variable]["avg"][0] = new_avg
        return curr_avg, new_avg

    def update_std_dev(
        self, variable: str, value: float, old_avg: float, new_avg: float, action: str
    ) -> None:
        if action == "delete":
            current_std_dev = self.metrics[variable]["std_dev"][0]
            current_count = self.metrics[variable]["std_dev"][1]
            new_std_dev = self.calculate_std_dev_delete(
                value, current_std_dev, current_count, old_avg, new_avg
            )
            self.metrics[variable]["std_dev"][1] -= 1
            self.metrics[variable]["std_dev"][0] = new_std_dev
        elif action == "insert":
            self.metrics[variable]["std_dev"][1] += 1
            current_std_dev = self.metrics[variable]["std_dev"][0]
            current_count = self.metrics[variable]["std_dev"][1]
            new_std_dev = self.calculate_std_dev_insert(
                value, current_std_dev, current_count, old_avg, new_avg
            )
            self.metrics[variable]["std_dev"][0] = new_std_dev

    @staticmethod
    def calculate_avg_insert(
        new_value: float, current_avg: float, value_cnt: int
    ) -> float:
        if value_cnt == 0:
            return 0
        if value_cnt == 1:
            return new_value
        return (current_avg * (value_cnt - 1) + new_value) / (value_cnt)

    @staticmethod
    def calculate_avg_delete(value: float, current_avg: float, value_cnt: int) -> float:
        if value_cnt == 0:
            return 0
        if value_cnt == 1:
            return value
        return (current_avg * (value_cnt + 1) - value) / (value_cnt)

    @staticmethod
    def calculate_std_dev_insert(
        new_value: float,
        curr_std_dev: float,
        value_cnt: int,
        old_avg: float,
        new_avg: float,
    ) -> float:
        if value_cnt == 0 or value_cnt == 1:
            return 0.0
        curr_variance = curr_std_dev**2
        term_1 = (value_cnt - 1) * curr_variance
        term_2 = (new_value - old_avg) * (new_value - new_avg)

        variance_n = (term_1 + term_2) / value_cnt

        std_dev_n = variance_n**0.5
        return std_dev_n

    @staticmethod
    def calculate_std_dev_delete(
        new_value: float,
        current_std_dev: float,
        value_cnt: int,
        old_avg: float,
        new_avg: float,
    ) -> float:
        if value_cnt == 0 or value_cnt == 1:
            return 0.0
        curr_variance = current_std_dev**2
        new_variance = (
            (curr_variance * value_cnt) - (new_value - new_avg) * (new_value - old_avg)
        ) / (value_cnt - 1)

        return new_variance**0.5
