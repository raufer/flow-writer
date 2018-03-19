import unittest
import pickle
import os
import shutil
import pandas as pd

from flow_writer.abstraction.pipeline import Pipeline
from flow_writer.abstraction.stage import Stage
from flow_writer.abstraction import pipeline_step
from flow_writer.dependency_manager import DependencyEntry, dependency_manager
from flow_writer.ops.function_ops import lazy


class TestDependencyManagerModelPersistence(unittest.TestCase):

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

    def test_dependency_decorator_model_persistence(self):
        """
        The step dependency manager decorator should serve the usecase of model persistence.
        In estimating mode, we should train the models, capture them, and persist them on disk.
        In production mode, the load functions should run, restore the models and inject those dependencies into the appropriate steps.
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

        @lazy
        def loader(path):
            with open(path, 'rb') as f:
                return pickle.load(f)

        @lazy
        def writer(path, tokenizer):
            with open(path, 'wb') as f:
                pickle.dump(tokenizer, f)

        tokenizer_path = os.path.join(self.path, 'tokenizer.pickle')
        dependencies = {"tokenizer": [loader(tokenizer_path), writer(tokenizer_path)]}

        @dependency_manager(dependencies)
        @pipeline_step()
        def step_tokenize(df, col, tokenizer=None):
            if not tokenizer:
                tokenizer = df.name.unique().tolist()
            df.loc[:, 'tokenizer'] = df.apply(lambda r: tokenizer.index(r[col]), axis=1)
            return df, tokenizer

        @pipeline_step()
        def step_filter_by_age(df, threshold):
            return pd.DataFrame(df[df['age'] > threshold])

        @lazy
        def loader(path):
            with open(path, 'rb') as f:
                return pickle.load(f)

        @lazy
        def writer(path, normalizer):
            with open(path, 'wb') as f:
                pickle.dump(normalizer, f)

        normalizer_path = os.path.join(self.path, 'normalizer.pickle')
        dependencies = {"normalizer": [loader(normalizer_path), writer(normalizer_path)]}

        @dependency_manager(dependencies)
        @pipeline_step()
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

        #  profile estimator
        df_estimator = pipeline.run_with_sinks(df)

        self.assertListEqual(df_estimator.values.tolist(), df_run.values.tolist())

        with open(tokenizer_path, "rb") as f:
            self.assertListEqual(pickle.load(f), [d[0] for d in data])

        with open(normalizer_path, "rb") as f:
            self.assertEqual(pickle.load(f), (max([d[2] for d in data]), min([d[2] for d in data])))

    def test_dependency_decorator_model_persistence_fit_alias(self):
        """
        'pipeline.fit()' should be an alias to 'pipeline.run_with_sinks'
        This is useful to give more semantics to ML use cases
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

        @lazy
        def loader(path):
            with open(path, 'rb') as f:
                return pickle.load(f)

        @lazy
        def writer(path, tokenizer):
            with open(path, 'wb') as f:
                pickle.dump(tokenizer, f)

        tokenizer_path = os.path.join(self.path, 'tokenizer.pickle')
        dependencies = {"tokenizer": [loader(tokenizer_path), writer(tokenizer_path)]}

        @dependency_manager(dependencies)
        @pipeline_step()
        def step_tokenize(df, col, tokenizer=None):
            if not tokenizer:
                tokenizer = df.name.unique().tolist()
            df.loc[:, 'tokenizer'] = df.apply(lambda r: tokenizer.index(r[col]), axis=1)
            return df, tokenizer

        @pipeline_step()
        def step_filter_by_age(df, threshold):
            return pd.DataFrame(df[df['age'] > threshold])

        @lazy
        def loader(path):
            with open(path, 'rb') as f:
                return pickle.load(f)

        @lazy
        def writer(path, normalizer):
            with open(path, 'wb') as f:
                pickle.dump(normalizer, f)

        normalizer_path = os.path.join(self.path, 'normalizer.pickle')
        dependencies = {"normalizer": [loader(normalizer_path), writer(normalizer_path)]}

        @dependency_manager(dependencies)
        @pipeline_step()
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

        #  profile estimator
        df_fit = pipeline.fit(df)

        self.assertListEqual(df_fit.values.tolist(), df_run.values.tolist())

        with open(tokenizer_path, "rb") as f:
            self.assertListEqual(pickle.load(f), [d[0] for d in data])

        with open(normalizer_path, "rb") as f:
            self.assertEqual(pickle.load(f), (max([d[2] for d in data]), min([d[2] for d in data])))

    def test_dependencies_manager_more_than_one_dependency(self):
        """
        The dependency manager should allow for possibly more than one dependency in each step
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

        @lazy
        def loader(path):
            with open(path, 'rb') as f:
                return pickle.load(f)

        @lazy
        def writer(path, model):
            with open(path, 'wb') as f:
                pickle.dump(model, f)

        tokenizer_path = os.path.join(self.path, 'tokenizer.pickle')
        ones_path = os.path.join(self.path, 'ones.pickle')

        dependencies = {
            "tokenizer": [loader(tokenizer_path), writer(tokenizer_path)],
            "ones": [loader(ones_path), writer(ones_path)]
        }

        @dependency_manager(dependencies)
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

        normalizer_path = os.path.join(self.path, 'normalizer.pickle')
        twos_path = os.path.join(self.path, 'twos.pickle')

        @lazy
        def writer(path, df, model):
            model = [m + df.shape[0] for m in model]
            with open(path, 'wb') as f:
                pickle.dump(model, f)

        dependencies = {
            "normalizer": [loader(normalizer_path), writer(normalizer_path)],
            "twos": [loader(twos_path), writer(twos_path)]
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

        #  profile estimator
        df_fit = pipeline.fit(df)

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

    def test_dependencies_manager_writer_args_order_invariance(self):
        """
        The order by which the 'writer' should not have impact on the output.
        ie. both definitions 'def writer(model, df)' and 'def write(df, model)' should be equivalent at sink-node call time
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

        normalizer_path = os.path.join(self.path, 'normalizer.pickle')
        twos_path = os.path.join(self.path, 'twos.pickle')

        @lazy
        def loader(path):
            with open(path, 'rb') as f:
                return pickle.load(f)

        @lazy
        def writer(path, model, df):
            model = [m + df.shape[0] for m in model]
            with open(path, 'wb') as f:
                pickle.dump(model, f)

        dependencies = {
            "normalizer": [loader(normalizer_path), writer(normalizer_path)],
            "twos": [loader(twos_path), writer(twos_path)]
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

        #  profile estimator
        df_fit = pipeline.fit(df)

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
