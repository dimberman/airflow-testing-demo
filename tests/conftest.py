from pytest import fixture


@fixture
def check_dag_output(capsys):
    yield
    if "AssertionError" in capsys.readouterr().out:
        print(capsys.readouterr().out)
        raise AssertionError("Validation failed")
    print(capsys.readouterr().out)
