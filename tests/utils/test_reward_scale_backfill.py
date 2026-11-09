from datetime import date, datetime

import numpy as np
import pytest

from u19_pipeline.utils import reward_scale_backfill as rsb


def make_log(blocks, name="efonseca_ef01", start=(2024, 3, 5, 10, 30, 12.5)):
    log = {"block": blocks}
    if name is not None:
        log["animal"] = {"name": name}
    if start is not None:
        log["session"] = {"start": np.array(start, dtype=float)}
    return log


def make_block(scale=1.2, first_trial=1, n_trials=3):
    trials = [{"rewardScale": scale} for _ in range(n_trials)]
    return {"firstTrial": first_trial, "trial": np.array(trials, dtype=object)}


class TestRoundHalfAway:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (0.0, 0),
            (0.4, 0),
            (0.5, 1),
            (1.2, 1),
            (1.5, 2),
            (1.8, 2),
            (2.5, 3),
            (-0.5, -1),
            (-1.5, -2),
            (-0.4, 0),
        ],
    )
    def test_matches_matlab_and_mysql_coercion(self, value, expected):
        assert rsb.round_half_away(value) == expected


class TestScalar:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (1.2, 1.2),
            (0, 0.0),
            (np.float64(1.8), 1.8),
            (np.array(1.4), 1.4),
            (np.array([2.0]), 2.0),
        ],
    )
    def test_unwraps_numbers(self, value, expected):
        assert rsb.scalar(value) == pytest.approx(expected)

    @pytest.mark.parametrize(
        "value",
        [
            None,
            np.nan,
            np.inf,
            np.array([]),
            np.array([1.0, 2.0]),
            "1.2",
            {"a": 1},
        ],
    )
    def test_rejects_non_scalars(self, value):
        assert rsb.scalar(value) is None


class TestReadBlockValues:
    def test_multi_block_log(self):
        log = make_log(
            np.array([make_block(1.2, 1), make_block(1.8, 40)], dtype=object)
        )
        assert rsb.read_block_values(log, 2) == (pytest.approx(1.8), 40)

    def test_single_block_squeezed_to_dict(self):
        log = make_log(make_block(1.4, 1))
        assert rsb.read_block_values(log, 1) == (pytest.approx(1.4), 1)

    def test_single_trial_squeezed_to_dict(self):
        block = {"firstTrial": 7, "trial": {"rewardScale": 0.6}}
        assert rsb.read_block_values(make_log(block), 1) == (pytest.approx(0.6), 7)

    def test_uses_first_trial_scale_not_later_ones(self):
        block = make_block(1.0, 1)
        block["trial"][2]["rewardScale"] = 1.6
        assert rsb.read_block_values(make_log(block), 1)[0] == pytest.approx(1.0)

    @pytest.mark.parametrize("block_num", [0, -1, 3])
    def test_block_index_out_of_range(self, block_num):
        log = make_log(np.array([make_block(), make_block()], dtype=object))
        assert rsb.read_block_values(log, block_num) == (None, None)

    def test_missing_reward_scale_keeps_first_trial(self):
        block = {"firstTrial": 5, "trial": [{"choice": "L"}]}
        assert rsb.read_block_values(make_log(block), 1) == (None, 5)

    def test_empty_trials(self):
        block = {"firstTrial": 5, "trial": np.array([], dtype=object)}
        assert rsb.read_block_values(make_log(block), 1) == (None, 5)

    def test_no_blocks(self):
        assert rsb.read_block_values({"block": np.array([], dtype=object)}, 1) == (
            None,
            None,
        )
        assert rsb.read_block_values({}, 1) == (None, None)


class TestClassify:
    @pytest.mark.parametrize(
        "db, file", [(1, 1.2), (2, 1.8), (1, 1.8), (2, 1.5), (0, 0.4)]
    )
    def test_ok_when_db_is_rounded_or_truncated_file_value(self, db, file):
        assert rsb.classify(db, 1, file, 1) == rsb.OK

    @pytest.mark.parametrize("db, file", [(1.0, 1.0), (1.2, 1.2), (0.0, 0.0)])
    def test_unchanged_when_already_exact(self, db, file):
        assert rsb.classify(db, 1, file, 1) == rsb.UNCHANGED

    def test_zero_from_catch_branch_is_fallback(self):
        assert rsb.classify(0, 1, 1.4, 1) == rsb.FALLBACK

    def test_zero_that_is_a_valid_truncation_is_ok_not_fallback(self):
        # 0.6 truncates to 0, so a stored 0 is fully explained by the file.
        assert rsb.classify(0, 1, 0.6, 1) == rsb.OK

    def test_unrelated_value_is_mismatch(self):
        assert rsb.classify(3, 1, 1.2, 1) == rsb.MISMATCH

    def test_first_trial_disagreement_is_mismatch_even_if_scale_fits(self):
        assert rsb.classify(1, 1, 1.2, 40) == rsb.MISMATCH

    def test_missing_file_scale(self):
        assert rsb.classify(1, 1, None, 1) == rsb.NO_VALUE

    def test_missing_first_trial_on_either_side_skips_that_check(self):
        assert rsb.classify(1, None, 1.2, 1) == rsb.OK
        assert rsb.classify(1, 1, 1.2, None) == rsb.OK


class TestCheckIdentity:
    # Naive, like the MySQL datetime DataJoint returns.
    DB_START = datetime(2024, 3, 5, 10, 30)  # noqa: DTZ001

    def test_matching_log(self):
        ok, reason = rsb.check_identity(
            make_log([]), "efonseca_ef01", self.DB_START, require=True
        )
        assert ok and reason is None

    def test_wrong_subject(self):
        ok, reason = rsb.check_identity(
            make_log([], name="someone_else"),
            "efonseca_ef01",
            self.DB_START,
            require=True,
        )
        assert not ok and "someone_else" in reason

    def test_start_within_tolerance(self):
        log = make_log([], start=(2024, 3, 5, 10, 32, 0))
        assert rsb.check_identity(log, "efonseca_ef01", self.DB_START, require=True)[0]

    def test_start_outside_tolerance_catches_second_session_same_day(self):
        log = make_log([], start=(2024, 3, 5, 14, 0, 0))
        ok, reason = rsb.check_identity(
            log, "efonseca_ef01", self.DB_START, require=True
        )
        assert not ok and "start" in reason

    def test_different_day(self):
        log = make_log([], start=(2024, 3, 6, 10, 30, 0))
        assert not rsb.check_identity(
            log, "efonseca_ef01", self.DB_START, require=True
        )[0]

    def test_missing_fields_rejected_when_required(self):
        log = make_log([], name=None, start=None)
        ok, reason = rsb.check_identity(
            log, "efonseca_ef01", self.DB_START, require=True
        )
        assert not ok and "animal.name" in reason

    def test_missing_fields_tolerated_when_not_required(self):
        log = make_log([], name=None, start=None)
        assert rsb.check_identity(
            log, "efonseca_ef01", self.DB_START, require=False
        ) == (True, None)

    def test_present_but_wrong_still_fails_when_not_required(self):
        log = make_log([], name="someone_else")
        assert not rsb.check_identity(
            log, "efonseca_ef01", self.DB_START, require=False
        )[0]

    def test_numpy_string_name(self):
        log = make_log([], name=np.str_("efonseca_ef01"))
        assert rsb.check_identity(log, "efonseca_ef01", self.DB_START, require=True)[0]

    def test_missing_db_start_time_skips_time_check(self):
        assert rsb.check_identity(make_log([]), "efonseca_ef01", None, require=True)[0]


class TestBasenameIndex:
    def test_indexes_mat_files_recursively(self, tmp_path):
        (tmp_path / "a" / "b").mkdir(parents=True)
        target = tmp_path / "a" / "b" / "PoissonBlocks_ef01_T_20240305.mat"
        target.touch()
        (tmp_path / "a" / "notes.txt").touch()
        index = rsb.build_basename_index([tmp_path])
        assert index == {"PoissonBlocks_ef01_T_20240305.mat": [target]}

    def test_duplicate_names_are_all_kept(self, tmp_path):
        for sub in ("x", "y"):
            (tmp_path / sub).mkdir()
            (tmp_path / sub / "same.mat").touch()
        assert len(rsb.build_basename_index([tmp_path])["same.mat"]) == 2

    def test_missing_directory_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            rsb.build_basename_index([tmp_path / "nope"])

    def test_no_dirs(self):
        assert rsb.build_basename_index([]) == {}


class TestRecordedBasename:
    @pytest.mark.parametrize(
        "path, expected",
        [
            ("/braininit/RigData/training/x/file.mat", "file.mat"),
            ("C:\\Experiments\\data\\file.mat", "file.mat"),
            ("file.mat", "file.mat"),
        ],
    )
    def test_handles_both_separators(self, path, expected):
        assert rsb.recorded_basename(path) == expected

    @pytest.mark.parametrize("path", [None, "", "/some/dir/"])
    def test_empty(self, path):
        assert rsb.recorded_basename(path) is None


class TestPathMap:
    def test_round_trip(self, tmp_path):
        missing = [("efonseca_ef01", date(2024, 3, 5), 1, "/old/path/a.mat")]
        csv_path = tmp_path / "missing.csv"
        rsb.write_missing_csv(csv_path, missing)

        # User fills in the path column, leaves others unchanged.
        text = csv_path.read_text().replace(",,", ",/new/place/a.mat,", 1)
        csv_path.write_text(text)

        mapping = rsb.load_path_map(csv_path)
        assert mapping == {
            ("efonseca_ef01", "2024-03-05", 1): tmp_path.__class__("/new/place/a.mat")
        }

    def test_blank_paths_are_ignored(self, tmp_path):
        csv_path = tmp_path / "m.csv"
        csv_path.write_text(
            "subject_fullname,session_date,session_number,path,recorded_path\n"
            "efonseca_ef01,2024-03-05,1,,/old.mat\n"
        )
        assert rsb.load_path_map(csv_path) == {}

    def test_missing_required_column(self, tmp_path):
        csv_path = tmp_path / "m.csv"
        csv_path.write_text("subject_fullname,session_date,path\nx,2024-01-01,/a.mat\n")
        with pytest.raises(ValueError, match="session_number"):
            rsb.load_path_map(csv_path)

    def test_duplicate_session_rows_rejected(self, tmp_path):
        csv_path = tmp_path / "m.csv"
        csv_path.write_text(
            "subject_fullname,session_date,session_number,path\n"
            "x,2024-01-01,1,/a.mat\n"
            "x,2024-01-01,1,/b.mat\n"
        )
        with pytest.raises(ValueError, match="more than once"):
            rsb.load_path_map(csv_path)

    def test_session_key_normalises_types(self):
        assert rsb.session_key("x", date(2024, 1, 2), np.int64(3)) == (
            "x",
            "2024-01-02",
            3,
        )
        assert rsb.session_key("x", "2024-01-02", "3") == ("x", "2024-01-02", 3)
