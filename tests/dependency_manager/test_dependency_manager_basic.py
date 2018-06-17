import unittest
import os
import shutil
import pandas as pd

from flow_writer.abstraction.pipeline import Pipeline
from flow_writer.abstraction.stage import Stage
from flow_writer.abstraction import pipeline_step
from flow_writer.dependency_manager import DependencyEntry, dependency_manager
from flow_writer.ops.function_ops import lazy


class TestDependencyDecorator(unittest.TestCase):

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

    def test_dependency_decorator(self):
        """
        '@dependency_manager(f, g)' should add a static attribute, a registry. where the two functions will be strored
        """

        def f():
            return 'f'

        def g():
            return 'g'

        @dependency_manager({"a": [f, g]})
        def step(a):
            return a

        dep = step.registry["dependencies"][0]
        self.assertEqual(dep.arg, 'a')
        self.assertEqual(dep.source(), 'f')
        self.assertEqual(dep.sink(), 'g')
        self.assertEqual(step.__name__, 'step')

    def test_dependency_decorator_basics_profile_default(self):
        """
        '@dependency_manager(f,g)' should not have effect if the profile is 'default'
        Neither 'f' nor 'g' should be invoked
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

        global f_mutable_state
        f_mutable_state = 0

        def f():
            global f_mutable_state
            f_mutable_state = 1

        global g_mutable_state
        g_mutable_state = {}

        def g(df, tokenizer, pipeline, stage, step):
            global g_mutable_state
            g_mutable_state = {
                'df': df,
                'tokenizer': tokenizer,
                'pipeline': pipeline.name,
                'stage': stage.name,
                'step': step.name
            }

        @dependency_manager({"tokenizer": [f, g]})
        @pipeline_step()
        def step_tokenize(df, col, tokenizer=None):
            if not tokenizer:
                tokenizer = df.name.unique().tolist()
            df.loc[:, 'tokenizer'] = df.apply(lambda r: tokenizer.index(r[col]), axis=1)
            return df, tokenizer

        @pipeline_step()
        def step_filter_by_age(df, threshold):
            return pd.DataFrame(df[df['age'] > threshold])

        @pipeline_step()
        def step_normalize(df, col, normalizer=None):
            if not normalizer:
                normalizer = (df.score.max(), df.score.min())
            df.loc[:, 'normalizer'] = df.apply(lambda r: (normalizer[0] - r[col]) / (normalizer[0] - normalizer[1]),
                                               axis=1)
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

        #  f was not invoked
        self.assertEqual(f_mutable_state, 0)

        #  g was invoked
        self.assertDictEqual(g_mutable_state, {})

