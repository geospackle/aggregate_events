from app import MetricAggregation
import pytest

# TODO: proper unit testing for each function

event = {
    "body": {
        "action": "insert",  # one of: insert, delete, modify
        "value": 1,  # int, only present for "insert" and "modify" events
        "old_value": 2,  # int, only present for "delete" and "modify" events
        "variable": "foo",  # variable name
    },  # other event fields can be ignored
}


def test_inserts():
    aggregator = MetricAggregation()
    event["body"]["action"] = "insert"

    event["body"]["value"] = 0
    aggregator.handle_event(event, None)

    event["body"]["value"] = 1
    aggregator.handle_event(event, None)

    event["body"]["value"] = 2
    aggregator.handle_event(event, None)

    event["body"]["value"] = 3
    aggregator.handle_event(event, None)

    res = aggregator.get_metrics()
    assert res["foo"]["avg"] == 1.5
    expected = 1.118034
    actual = res["foo"]["std_dev"]
    # exact floating point depends on architecture
    assert round(expected, 6) == round(actual, 6)


def test_multi_variable():
    aggregator = MetricAggregation()
    event["body"]["action"] = "insert"
    event["body"]["variable"] = "foo"

    event["body"]["value"] = 0
    aggregator.handle_event(event, None)

    event["body"]["value"] = 1
    aggregator.handle_event(event, None)

    event["body"]["value"] = 2
    aggregator.handle_event(event, None)

    event["body"]["variable"] = "bar"

    event["body"]["value"] = 1
    aggregator.handle_event(event, None)

    event["body"]["value"] = 1
    aggregator.handle_event(event, None)

    event["body"]["value"] = 1
    aggregator.handle_event(event, None)

    res = aggregator.get_metrics()
    assert res["foo"]["avg"] == 1
    expected = 0.81649658092773
    actual = res["foo"]["std_dev"]
    # exact floating point depends on architecture
    assert round(expected, 6) == round(actual, 6)

    assert res["bar"]["avg"] == 1
    assert res["bar"]["std_dev"] == 0


def test_deletes():
    aggregator = MetricAggregation()

    event["body"]["variable"] = "foo"

    event["body"]["value"] = 1
    event["body"]["action"] = "insert"
    aggregator.handle_event(event, None)

    event["body"]["value"] = 2
    event["body"]["action"] = "insert"
    aggregator.handle_event(event, None)

    event["body"]["value"] = 3
    event["body"]["action"] = "insert"
    aggregator.handle_event(event, None)

    event["body"]["old_value"] = 1
    event["body"]["action"] = "delete"
    aggregator.handle_event(event, None)

    event["body"]["value"] = 4
    event["body"]["action"] = "insert"
    aggregator.handle_event(event, None)

    res = aggregator.get_metrics()
    assert res["foo"]["avg"] == 3
    actual = res["foo"]["std_dev"]
    expected = 0.81649658
    # exact floating point depends on architecture
    assert round(expected, 6) == round(actual, 6)


def test_modify():
    event["body"]["value"] = 1
    event["body"]["action"] = "insert"

    aggregator = MetricAggregation()
    aggregator.handle_event(event, None)

    event["body"]["value"] = 2
    event["body"]["old_value"] = 1
    event["body"]["action"] = "modify"
    aggregator.handle_event(event, None)

    event["body"]["value"] = 2
    event["body"]["action"] = "insert"
    aggregator.handle_event(event, None)

    res = aggregator.get_metrics()
    assert res["foo"]["avg"] == 2


def test_non_existing_variable_error():
    event["body"]["action"] = "modify"

    aggregator = MetricAggregation()

    with pytest.raises(Exception):
        aggregator.handle_event(event, None)

    event["body"]["action"] = "delete"
    with pytest.raises(Exception):
        aggregator.handle_event(event, None)
