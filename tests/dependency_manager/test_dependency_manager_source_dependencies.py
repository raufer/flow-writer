import unittest
import pickle
import os
import shutil
import pandas as pd

from flow_writer.abstraction.pipeline import Pipeline
from flow_writer.abstraction.stage import Stage
from flow_writer.abstraction import pipeline_step


class TestDependencyManagerSourceDependencies(unittest.TestCase):

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

    def test_placeholder_use_lock_dependencies(self):
        """
        'pipeline.lock_dependencies(data)' should accept a dict to resolve the pipeline's placeholders
        Given a step with external data dependencies we might just know the actual values at run time.
        For instance, a step that needs to load some pre-calculated data from s3.
        We should be able to define a placeholder for this value to be defined in a later stage.
        This value should be resolved by passing the necessary data to the call.
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

        tokenizer_load_path = os.path.join(self.path, 'tokenizer_load.pickle')

        with open(tokenizer_load_path, 'wb') as f:
            pickle.dump(df.name.unique().tolist()[::-1], f)

        data = {'first-stage/step_tokenize/tokenizer/loader/path': tokenizer_load_path}
        pipeline = pipeline.lock_dependencies(data)

        result = pipeline.run(df)

        self.assertListEqual(list(result['tokenizer'].values), [0, 1, 2, 4, 5][::-1])

    def test_placeholder_use_lock_dependencies_with_default_args(self):
        """
        'pipeline.lock_dependencies(data)' should accept a dict to resolve the pipeline's placeholders
        The loader function should be able to operate with default arguments
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

        def loader(path, reverse=False):
            with open(path, 'rb') as f:
                if reverse:
                    return pickle.load(f)[::-1]
                else:
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

        tokenizer_load_path = os.path.join(self.path, 'tokenizer_load.pickle')

        with open(tokenizer_load_path, 'wb') as f:
            pickle.dump(df.name.unique().tolist()[::-1], f)

        #  check that the default argument is picked up
        data = {'first-stage/step_tokenize/tokenizer/loader/path': tokenizer_load_path}
        pipeline_ = pipeline.lock_dependencies(data)
        result = pipeline_.run(df)

        self.assertListEqual(list(result['tokenizer'].values), [0, 1, 2, 4, 5][::-1])

        #  check that we can alter the default argument if needed
        data = {
            'first-stage/step_tokenize/tokenizer/loader/path': tokenizer_load_path,
            'first-stage/step_tokenize/tokenizer/loader/reverse': True
        }
        pipeline_ = pipeline.lock_dependencies(data)
        result = pipeline_.run(df)

        self.assertListEqual(list(result['tokenizer'].values), [0, 1, 3, 4, 5])

    def test_placeholder_use_lock_dependencies_multiple(self):
        """
        'pipeline.lock_dependencies(data)' should accept a dict to resolve the pipeline's placeholders
        We should cover the case where more than one step with dependencies exists
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
            'second-stage/step_normalize/normalizer/loader/path': normalizer_path
        }

        pipeline = pipeline.lock_dependencies(data)

        result = pipeline.run(df)

        self.assertListEqual(list(result['tokenizer'].values), [0, 1, 2, 4, 5][::-1])

        self.assertListEqual(list(result['normalizer'].values), [(10 - s) / (10 - 1) for s in result['score']])

