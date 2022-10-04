import shlex
from cosima_cookbook import database_update


def test_database_update(tmp_path):

    args = shlex.split(
        "-db {db} test/data/update/experiment_a test/data/update/experiment_b".format(
            db=tmp_path / "test.db"
        )
    )

    database_update.main(args)
