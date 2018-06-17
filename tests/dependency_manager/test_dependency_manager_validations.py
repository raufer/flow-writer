import unittest
import pickle
import os
import shutil
import pandas as pd

from nose.tools import raises

from flow_writer.abstraction.pipeline import Pipeline
from flow_writer.abstraction.stage import Stage
from flow_writer.abstraction import pipeline_step
from flow_writer.dependency_manager import dependency_manager
from flow_writer.dependency_manager.exceptions import SourceArgumentsNotResolved, SinkArgumentsNotResolved


class TestDependencyManagerValidations(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        cls.path = os.path.join('.temp', 'data')

    @classmethod
    def teardown_class(cls):
        if os.path.exists(os.path.dirname(cls.path)):
            shutil.rmtree(os.path.dirname(cls.path))

    def setUp(self):
        if not os.path.exists(self.path):
            os.makedirs(self.path)

    def tearDown(self):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

    def test_lock_source_dependencies_validations(self):
        """
        'pipeline.lock_dependencies(data)' should throw an 'SourceArgumentsNotResolved' exception
        if all the needed arguments by the source handlers needed are not present
        """
        data = [
            ("Bob", 25, 19),
            ("Sue", 30, 16),
            ("Joe", 17, 20),
            ("Leo", 30, 29),
            ("Jay", 33, 10),
            ("Ali", 26, 25)
        ]

        df = pd.DataFrame(data, columns=["name", "age", "score"])

        def loader(path, g):
            with open(path, 'rb') as f:
                return g(pickle.load(f))

        def writer(path, tokenizer):
            with open(path, 'wb') as f:
                pickle.dump(tokenizer, f)

        dependencies = {"tokenizer": [loader, writer]}

        @pipeline_step(dm=dependencies)
        def step_tokenize(df, col, tokenizer=None):
            if not tokenizer:
                tokenizer = df.name.unique().tolist()
            df.loc[:, 'tokenizer'] = df.apply(lambda r: tokenizer.index(r[col]), axis=1)
            return df, tokenizer

        @pipeline_step()
        def step_filter_by_age(df, threshold):
            return pd.DataFrame(df[df['age'] > threshold])

        @pipeline_step()
        def step_name_length(df, col):
            df.loc[:, 'name_length'] = df.apply(lambda r: len(r[col]), axis=1)
            return df

        pipeline = Pipeline(
            'demo-pipeline',
            Stage('first-stage', step_tokenize(col='name'), step_filter_by_age(threshold=20)),
            Stage('second-stage', step_name_length(col='name'))
        )

        tokenizer_load_path = os.path.join(self.path, 'tokenizer_load.pickle')

        with open(tokenizer_load_path, 'wb') as f:
            pickle.dump(df.name.unique().tolist(), f)

        data = {
            'first-stage/step_tokenize/tokenizer/loader/path': tokenizer_load_path,
            'first-stage/step_tokenize/tokenizer/loader/g': lambda x: x[::-1]
        }

        pipeline = pipeline.lock_dependencies(data)

        result = pipeline.run(df)

        self.assertListEqual(list(result['tokenizer'].values), [0, 1, 2, 4, 5][::-1])

        with self.assertRaises(SourceArgumentsNotResolved):
            data = {'first-stage/step_tokenize/tokenizer/loader/g': lambda x: x[::-1]}
            _ = pipeline.lock_dependencies(data)

        with self.assertRaises(SourceArgumentsNotResolved):
            data = {'first-stage/step_tokenize/tokenizer/loader/path': tokenizer_load_path}
            _ = pipeline.lock_dependencies(data)

    def test_lock_source_dependencies_validations_multiple_steps(self):
        """
        'pipeline.lock_dependencies(data)' should throw an 'SourceArgumentsNotResolved' exception
        if all the needed arguments by the source handlers needed are not present. multiple dependencies test
        """
        data = [
            ("Bob", 25, 19),
            ("Sue", 30, 16),
            ("Joe", 17, 20),
            ("Leo", 30, 29),
            ("Jay", 33, 10),
            ("Ali", 26, 25)
        ]

        df = pd.DataFrame(data, columns=["name", "age", "score"])

        def loader(path, g):
            with open(path, 'rb') as f:
                return pickle.load(f)

        def writer(path, model):
            with open(path, 'wb') as f:
                pickle.dump(model, f)

        dependencies = {"tokenizer": [loader, writer]}

        @pipeline_step(dm=dependencies)
        def step_tokenize(df, col, tokenizer=None):
            if not tokenizer:
                tokenizer = df.name.unique().tolist()
            df.loc[:, 'tokenizer'] = df.apply(lambda r: tokenizer.index(r[col]), axis=1)
            return df, tokenizer

        @pipeline_step()
        def step_filter_by_age(df, threshold):
            return pd.DataFrame(df[df['age'] > threshold])

        @pipeline_step()
        def step_name_length(df, col):
            df.loc[:, 'name_length'] = df.apply(lambda r: len(r[col]), axis=1)
            return df

        dependencies = {"normalizer": [loader, writer]}

        @pipeline_step(dm=dependencies)
        def step_normalize(df, col, normalizer=None):
            if not normalizer:
                normalizer = (df.score.max(), df.score.min())

            df.loc[:, 'normalizer'] = df.apply(
                lambda r: (normalizer[0] - r[col]) / (normalizer[0] - normalizer[1]),
                axis=1
            )

            return df, normalizer

        pipeline = Pipeline(
            'demo-pipeline',
            Stage('first-stage', step_tokenize(col='name'), step_filter_by_age(threshold=20)),
            Stage('second-stage', step_name_length(col='name'), step_normalize(col='score'))
        )

        tokenizer_path = os.path.join(self.path, 'tokenizer.pickle')
        normalizer_path = os.path.join(self.path, 'normalizer.pickle')

        with open(tokenizer_path, 'wb') as f:
            pickle.dump(df.name.unique().tolist()[::-1], f)

        with open(normalizer_path, 'wb') as f:
            pickle.dump((10, 1), f)

        data = {
            'first-stage/step_tokenize/tokenizer/loader/path': tokenizer_path,
            'first-stage/step_tokenize/tokenizer/loader/g': 1,
            'second-stage/step_normalize/normalizer/loader/path': normalizer_path,
            'second-stage/step_normalize/normalizer/loader/g': 1
        }

        pipeline = pipeline.lock_dependencies(data)

        result = pipeline.run(df)

        self.assertListEqual(list(result['tokenizer'].values), [0, 1, 2, 4, 5][::-1])

        self.assertListEqual(list(result['normalizer'].values), [(10 - s) / (10 - 1) for s in result['score']])

        with self.assertRaises(SourceArgumentsNotResolved):
            data = {
                'first-stage/step_tokenize/tokenizer/loader/g': 1,
                'second-stage/step_normalize/normalizer/loader/path': normalizer_path,
                'second-stage/step_normalize/normalizer/loader/g': 1
            }
            _ = pipeline.lock_dependencies(data)

        with self.assertRaises(SourceArgumentsNotResolved):
            data = {
                'first-stage/step_tokenize/tokenizer/loader/path': tokenizer_path,
                'second-stage/step_normalize/normalizer/loader/path': normalizer_path,
                'second-stage/step_normalize/normalizer/loader/g': 1
            }
            _ = pipeline.lock_dependencies(data)

        with self.assertRaises(SourceArgumentsNotResolved):
            data = {
                'first-stage/step_tokenize/tokenizer/loader/path': tokenizer_path,
                'first-stage/step_tokenize/tokenizer/loader/g': 1,
                'second-stage/step_normalize/normalizer/loader/g': 1
            }
            _ = pipeline.lock_dependencies(data)

        with self.assertRaises(SourceArgumentsNotResolved):
            data = {
                'first-stage/step_tokenize/tokenizer/loader/path': tokenizer_path,
                'first-stage/step_tokenize/tokenizer/loader/g': 1,
                'second-stage/step_normalize/normalizer/loader/path': normalizer_path,
            }
            _ = pipeline.lock_dependencies(data)

    def test_lock_source_dependencies_validations_no_validation(self):
        """
        'pipeline.lock_dependencies(data, val_source_args=False)' should not perform any validation
        """
        data = [
            ("Bob", 25, 19),
            ("Sue", 30, 16),
            ("Joe", 17, 20),
            ("Leo", 30, 29),
            ("Jay", 33, 10),
            ("Ali", 26, 25)
        ]

        df = pd.DataFrame(data, columns=["name", "age", "score"])

        def loader(path, g):
            with open(path, 'rb') as f:
                return g(pickle.load(f))

        def writer(path, tokenizer):
            with open(path, 'wb') as f:
                pickle.dump(tokenizer, f)

        dependencies = {"tokenizer": [loader, writer]}

        @pipeline_step(dm=dependencies)
        def step_tokenize(df, col, tokenizer=None):
            if not tokenizer:
                tokenizer = df.name.unique().tolist()
            df.loc[:, 'tokenizer'] = df.apply(lambda r: tokenizer.index(r[col]), axis=1)
            return df, tokenizer

        @pipeline_step()
        def step_filter_by_age(df, threshold):
            return pd.DataFrame(df[df['age'] > threshold])

        @pipeline_step()
        def step_name_length(df, col):
            df.loc[:, 'name_length'] = df.apply(lambda r: len(r[col]), axis=1)
            return df

        pipeline = Pipeline(
            'demo-pipeline',
            Stage('first-stage', step_tokenize(col='name'), step_filter_by_age(threshold=20)),
            Stage('second-stage', step_name_length(col='name'))
        )

        tokenizer_load_path = os.path.join(self.path, 'tokenizer_load.pickle')

        with open(tokenizer_load_path, 'wb') as f:
            pickle.dump(df.name.unique().tolist(), f)

        data = {
            'first-stage/step_tokenize/tokenizer/loader/path': tokenizer_load_path,
            'first-stage/step_tokenize/tokenizer/loader/g': lambda x: x[::-1]
        }

        pipeline = pipeline.lock_dependencies(data)

        result = pipeline.run(df)

        self.assertListEqual(list(result['tokenizer'].values), [0, 1, 2, 4, 5][::-1])

        #  it should not raise an error in both cases, just checking that it doesnt
        data = {'first-stage/step_tokenize/tokenizer/loader/g': lambda x: x[::-1]}
        _ = pipeline.lock_dependencies(data, val_source_args=False)

        data = {'first-stage/step_tokenize/tokenizer/loader/path': tokenizer_load_path}
        _ = pipeline.lock_dependencies(data, val_source_args=False)

    def test_lock_sink_dependencies_validations(self):
        """
        'pipeline.lock_dependencies(data)' should throw an 'SinkArgumentsNotResolved' exception
        if all the needed arguments by the sink handlers needed are not present
        """
        data = [
            ("Bob", 25, 19),
            ("Sue", 30, 16),
            ("Joe", 17, 20),
            ("Leo", 30, 29),
            ("Jay", 33, 10),
            ("Ali", 26, 25)
        ]

        df = pd.DataFrame(data, columns=["name", "age", "score"])

        def loader(path):
            with open(path, 'rb') as f:
                return pickle.load(f)

        def writer(path, tokenizer):
            with open(path, 'wb') as f:
                pickle.dump(tokenizer, f)

        dependencies = {"tokenizer": [loader, writer]}

        @pipeline_step(dm=dependencies)
        def step_tokenize(df, col, tokenizer=None):
            if not tokenizer:
                tokenizer = df.name.unique().tolist()
            df.loc[:, 'tokenizer'] = df.apply(lambda r: tokenizer.index(r[col]), axis=1)
            return df, tokenizer

        @pipeline_step()
        def step_filter_by_age(df, threshold):
            return pd.DataFrame(df[df['age'] > threshold])

        def loader(path):
            with open(path, 'rb') as f:
                return pickle.load(f)

        def writer(path, normalizer):
            with open(path, 'wb') as f:
                pickle.dump(normalizer, f)

        dependencies = {"normalizer": [loader, writer]}

        @pipeline_step(dm=dependencies)
        def step_normalize(df, col, normalizer=None):
            if not normalizer:
                normalizer = (df.score.max(), df.score.min())

            df.loc[:, 'normalizer'] = df.apply(
                lambda r: (normalizer[0] - r[col]) / (normalizer[0] - normalizer[1]),
                axis=1
            )

            return df, normalizer

        @pipeline_step()
        def step_name_length(df):
            df.loc[:, 'name_length'] = df.apply(lambda r: len(r['name']), axis=1)
            return df

        first_stage = Stage(
            "first-stage",
            step_tokenize(col='name'),
            step_filter_by_age(threshold=18)
        )

        second_stage = Stage(
            "second-stage",
            step_normalize(col='score'),
            step_name_length
        )

        pipeline = Pipeline("Pipeline", first_stage, second_stage)

        df_run = pipeline.run(df)

        self.assertListEqual(
            df_run['name_length'].values.tolist(),
            [len(d[0]) for d in data if d[1] > 18]
        )

        tokenizer_path = os.path.join(self.path, 'tokenizer.pickle')
        normalizer_path = os.path.join(self.path, 'normalizer.pickle')

        data_ = {
            'first-stage/step_tokenize/tokenizer/writer/path': tokenizer_path,
            'second-stage/step_normalize/normalizer/writer/path': normalizer_path
        }
        df_estimator = pipeline.run_with_sinks(df, data_)

        self.assertListEqual(df_estimator.values.tolist(), df_run.values.tolist())

        with open(tokenizer_path, "rb") as f:
            self.assertListEqual(pickle.load(f), [d[0] for d in data])

        with open(normalizer_path, "rb") as f:
            self.assertEqual(pickle.load(f), (max([d[2] for d in data]), min([d[2] for d in data])))

        with self.assertRaises(SinkArgumentsNotResolved):
            data_ = {
                'first-stage/step_tokenize/tokenizer/writer/path': tokenizer_path,
            }
            _ = pipeline.run_with_sinks(df, data_)

        with self.assertRaises(SinkArgumentsNotResolved):
            data_ = {
                'second-stage/step_normalize/normalizer/writer/path': normalizer_path
            }
            _ = pipeline.run_with_sinks(df, data_)

    def test_lock_sink_dependencies_validations_more_than_one_dependency(self):
        """
        'pipeline.lock_dependencies(data)' should throw an 'SinkArgumentsNotResolved' exception
        if all the needed arguments by the sink handlers needed are not present
        """
        data = [
            ("Bob", 25, 19),
            ("Sue", 30, 16),
            ("Joe", 17, 20),
            ("Leo", 30, 29),
            ("Jay", 33, 10),
            ("Ali", 26, 25)
        ]

        df = pd.DataFrame(data, columns=["name", "age", "score"])

        def loader(path):
            with open(path, 'rb') as f:
                return pickle.load(f)

        def writer(path, model):
            with open(path, 'wb') as f:
                pickle.dump(model, f)

        dependencies = {
            "tokenizer": [loader, writer],
            "ones": [loader, writer]
        }

        @pipeline_step(dm=dependencies)
        def step_tokenize(df, col, tokenizer=None, ones=None):

            if not tokenizer:
                tokenizer = df.name.unique().tolist()

            if not ones:
                ones = [1] * len(df)

            df.loc[:, 'tokenizer'] = df.apply(lambda r: tokenizer.index(r[col]), axis=1)
            df.loc[:, 'ones'] = ones

            return df, tokenizer, ones

        @pipeline_step()
        def step_filter_by_age(df, threshold):
            return pd.DataFrame(df[df['age'] > threshold])

        def writer(path, df, model):
            model = [m + df.shape[0] for m in model]
            with open(path, 'wb') as f:
                pickle.dump(model, f)

        dependencies = {
            "normalizer": [loader, writer],
            "twos": [loader, writer]
        }

        @pipeline_step(dm=dependencies)
        def step_normalize(df, col, normalizer=None, twos=None):

            if not normalizer:
                normalizer = (df.score.max(), df.score.min())

            if not twos:
                twos = [2] * len(df)

            df.loc[:, 'normalizer'] = df.apply(
                lambda r: (normalizer[0] - r[col]) / (normalizer[0] - normalizer[1]),
                axis=1
            )
            df.loc[:, 'twos'] = twos

            return df, normalizer, twos

        @pipeline_step()
        def step_name_length(df):
            df.loc[:, 'name_length'] = df.apply(lambda r: len(r['name']), axis=1)
            return df

        first_stage = Stage(
            "first-stage",
            step_tokenize(col='name'),
            step_filter_by_age(threshold=18)
        )

        second_stage = Stage(
            "second-stage",
            step_normalize(col='score'),
            step_name_length
        )

        pipeline = Pipeline("Pipeline", first_stage, second_stage)

        df_run = pipeline.run(df)

        self.assertListEqual(
            df_run['name_length'].values.tolist(),
            [len(d[0]) for d in data if d[1] > 18]
        )

        tokenizer_path = os.path.join(self.path, 'tokenizer.pickle')
        normalizer_path = os.path.join(self.path, 'normalizer.pickle')
        ones_path = os.path.join(self.path, 'ones.pickle')
        twos_path = os.path.join(self.path, 'twos.pickle')

        data_ = {
            'first-stage/step_tokenize/tokenizer/writer/path': tokenizer_path,
            'first-stage/step_tokenize/ones/writer/path': ones_path,
            'second-stage/step_normalize/normalizer/writer/path': normalizer_path,
            'second-stage/step_normalize/twos/writer/path': twos_path
        }

        df_fit = pipeline.fit(df, data_)

        self.assertListEqual(df_fit.values.tolist(), df_run.values.tolist())

        #  first step with dependencies
        with open(tokenizer_path, "rb") as f:
            self.assertListEqual(pickle.load(f), [d[0] for d in data])

        with open(ones_path, "rb") as f:
            self.assertListEqual(pickle.load(f), [1] * len(data))

        #  second step dependencies
        with open(normalizer_path, "rb") as f:
            self.assertListEqual(
                pickle.load(f),
                [
                    max([d[2] + df_fit.shape[0] for d in data]),
                    min([d[2] + df_fit.shape[0] for d in data])
                ]
            )

        with open(twos_path, "rb") as f:
            self.assertListEqual(
                pickle.load(f),
                [2 + df_fit.shape[0]] * len([d for d in data if d[1] > 18])
            )

        with self.assertRaises(SinkArgumentsNotResolved):
            data_ = {
                'first-stage/step_tokenize/ones/writer/path': ones_path,
                'second-stage/step_normalize/normalizer/writer/path': normalizer_path,
                'second-stage/step_normalize/twos/writer/path': twos_path
            }
            _ = pipeline.run_with_sinks(df, data_)

        with self.assertRaises(SinkArgumentsNotResolved):
            data_ = {
                'first-stage/step_tokenize/tokenizer/writer/path': tokenizer_path,
                'second-stage/step_normalize/normalizer/writer/path': normalizer_path,
                'second-stage/step_normalize/twos/writer/path': twos_path
            }
            _ = pipeline.run_with_sinks(df, data_)

        with self.assertRaises(SinkArgumentsNotResolved):
            data_ = {
                'first-stage/step_tokenize/tokenizer/writer/path': tokenizer_path,
                'first-stage/step_tokenize/ones/writer/path': ones_path,
                'second-stage/step_normalize/normalizer/writer/path': normalizer_path,
            }
            _ = pipeline.run_with_sinks(df, data_)

        with self.assertRaises(SinkArgumentsNotResolved):
            data_ = {
                'first-stage/step_tokenize/tokenizer/writer/path': tokenizer_path,
                'first-stage/step_tokenize/ones/writer/path': ones_path,
                'second-stage/step_normalize/twos/writer/path': twos_path
            }
            _ = pipeline.run_with_sinks(df, data_)

    def test_lock_sink_dependencies_validations_args_order_invariance(self):
        """
        'pipeline.lock_dependencies(data)' should throw an 'SinkArgumentsNotResolved' exception
        if all the needed arguments by the sink handlers needed are not present
        """
        data = [
            ("Bob", 25, 19),
            ("Sue", 30, 16),
            ("Joe", 17, 20),
            ("Leo", 30, 29),
            ("Jay", 33, 10),
            ("Ali", 26, 25)
        ]

        df = pd.DataFrame(data, columns=["name", "age", "score"])

        @pipeline_step()
        def step_tokenize(df, col, tokenizer=None, ones=None):

            if not tokenizer:
                tokenizer = df.name.unique().tolist()

            if not ones:
                ones = [1] * len(df)

            df.loc[:, 'tokenizer'] = df.apply(lambda r: tokenizer.index(r[col]), axis=1)
            df.loc[:, 'ones'] = ones

            return df, tokenizer, ones

        @pipeline_step()
        def step_filter_by_age(df, threshold):
            return pd.DataFrame(df[df['age'] > threshold])

        def loader(path):
            with open(path, 'rb') as f:
                return pickle.load(f)

        def writer(path, model, df):
            model = [m + df.shape[0] for m in model]
            with open(path, 'wb') as f:
                pickle.dump(model, f)

        dependencies = {
            "normalizer": [loader, writer],
            "twos": [loader, writer]
        }

        @dependency_manager(dependencies)
        @pipeline_step()
        def step_normalize(df, col, normalizer=None, twos=None):

            if not normalizer:
                normalizer = (df.score.max(), df.score.min())

            if not twos:
                twos = [2] * len(df)

            df.loc[:, 'normalizer'] = df.apply(
                lambda r: (normalizer[0] - r[col]) / (normalizer[0] - normalizer[1]),
                axis=1
            )
            df.loc[:, 'twos'] = twos

            return df, normalizer, twos

        @pipeline_step()
        def step_name_length(df):
            df.loc[:, 'name_length'] = df.apply(lambda r: len(r['name']), axis=1)
            return df

        first_stage = Stage(
            "first-stage",
            step_tokenize(col='name'),
            step_filter_by_age(threshold=18)
        )

        second_stage = Stage(
            "second-stage",
            step_normalize(col='score'),
            step_name_length
        )

        pipeline = Pipeline("Pipeline", first_stage, second_stage)

        df_run = pipeline.run(df)

        self.assertListEqual(
            df_run['name_length'].values.tolist(),
            [len(d[0]) for d in data if d[1] > 18]
        )

        normalizer_path = os.path.join(self.path, 'normalizer.pickle')
        twos_path = os.path.join(self.path, 'twos.pickle')

        data_ = {
            'second-stage/step_normalize/normalizer/writer/path': normalizer_path,
            'second-stage/step_normalize/twos/writer/path': twos_path
        }

        df_fit = pipeline.fit(df, data_)

        self.assertListEqual(df_fit.values.tolist(), df_run.values.tolist())

        with open(normalizer_path, "rb") as f:
            self.assertListEqual(
                pickle.load(f),
                [
                    max([d[2] + df_fit.shape[0] for d in data]),
                    min([d[2] + df_fit.shape[0] for d in data])
                ]
            )

        with open(twos_path, "rb") as f:
            self.assertListEqual(
                pickle.load(f),
                [2 + df_fit.shape[0]] * len([d for d in data if d[1] > 18])
            )

        with self.assertRaises(SinkArgumentsNotResolved):
            data_ = {
                'second-stage/step_normalize/twos/writer/path': twos_path
            }
            _ = pipeline.run_with_sinks(df, data_)

        with self.assertRaises(SinkArgumentsNotResolved):
            data_ = {
                'second-stage/step_normalize/normalizer/writer/path': normalizer_path,
            }
            _ = pipeline.run_with_sinks(df, data_)

    def test_lock_sink_deppenencies_and_fit(self):
        """
        'pipeline.lock_dependencies(source_data) & pipeline.fit(df)' should constitute a valid sequence of steps
        """
        data = [
            ("Bob", 25, 19),
            ("Sue", 30, 16),
            ("Joe", 17, 20),
            ("Leo", 30, 29),
            ("Jay", 33, 10),
            ("Ali", 26, 25)
        ]

        df = pd.DataFrame(data, columns=["name", "age", "score"])

        def loader(path):
            with open(path, 'rb') as f:
                return pickle.load(f)

        def writer(path, tokenizer):
            with open(path, 'wb') as f:
                pickle.dump(tokenizer, f)

        dependencies = {"tokenizer": [loader, writer]}

        @pipeline_step(dm=dependencies)
        def step_tokenize(df, col, tokenizer=None):
            if not tokenizer:
                tokenizer = df.name.unique().tolist()
            df.loc[:, 'tokenizer'] = df.apply(lambda r: tokenizer.index(r[col]), axis=1)
            return df, tokenizer

        @pipeline_step()
        def step_filter_by_age(df, threshold):
            return pd.DataFrame(df[df['age'] > threshold])

        @pipeline_step()
        def step_name_length(df, col):
            df.loc[:, 'name_length'] = df.apply(lambda r: len(r[col]), axis=1)
            return df

        pipeline = Pipeline(
            'demo-pipeline',
            Stage('first-stage', step_tokenize(col='name'), step_filter_by_age(threshold=20)),
            Stage('second-stage', step_name_length(col='name'))
        )

        tokenizer_path = os.path.join(self.path, 'tokenizer_load.pickle')

        data = {
            'first-stage/step_tokenize/tokenizer/writer/path': tokenizer_path
        }

        pipeline = pipeline.lock_dependencies(data, val_source_args=False)

        result = pipeline.fit(df)

        self.assertListEqual(list(result['tokenizer'].values), [0, 1, 3, 4, 5])

        with open(tokenizer_path, 'rb') as f:
            self.assertListEqual(df.name.unique().tolist(), pickle.load(f))

