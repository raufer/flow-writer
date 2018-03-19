import unittest
import pyspark.sql.types as T

from flow_writer.abstraction import pipeline_step
from tests import spark as spark
from flow_writer.abstraction.stage import Stage


class TestStageExecCycle(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_after_each_step(self):
        """
        'stage.after_each_step(f)' should invoke some behaviour 'f' after each step
        That callable should be a function that receives a signal and does something with it
        """
        data = [
            ("Bob", 25, 1),
            ("Sue", 30, 0),
            ("Joe", 17, 0),
            ("Leo", 30, 1)
        ]

        df = spark.createDataFrame(data, ["name", "age", "employed"])

        @pipeline_step()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @pipeline_step()
        def step_stringify(df):
            return df.withColumn("age_str", df.age.cast(T.StringType()))

        @pipeline_step()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        stage = Stage("dummy stage",
          step_just_adults(threshold=18),
          step_stringify,
          step_rename
        )

        global mutable_state
        mutable_state = 0

        def f(df):
            global mutable_state
            mutable_state += 1
            return mutable_state

        stage = stage.after_each_step(f)
        _ = stage.run(df)
        self.assertEqual(mutable_state, len(stage.steps))

        gen = stage.run_iter(df)

        _ = next(gen)
        self.assertEqual(mutable_state, 4)
        _ = next(gen)
        self.assertEqual(mutable_state, 5)
        _ = next(gen)
        self.assertEqual(mutable_state, 6)

    def test_after_each_step_inject_obj(self):
        """
        'stage.after_each_step(f)' should make sure that the step object is also injected if requested by 'f'
        """
        data = [
            ("Bob", 25, 1),
            ("Sue", 30, 0),
            ("Joe", 17, 0),
            ("Leo", 30, 1)
        ]

        df = spark.createDataFrame(data, ["name", "age", "employed"])

        @pipeline_step()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @pipeline_step()
        def step_stringify(df):
            return df.withColumn("age_str", df.age.cast(T.StringType()))

        @pipeline_step()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        stage = Stage("dummy stage",
          step_just_adults(threshold=18),
          step_stringify,
          step_rename
        )

        global mutable_state
        mutable_state = []

        def f(df, step):
            global mutable_state
            mutable_state.append(step.name)

        stage = stage.after_each_step(f)

        _ = stage.run(df)

        self.assertListEqual(
            mutable_state,
            [
                'step_just_adults',
                'step_stringify',
                'step_rename'
            ]
        )

    def test_before_each_step(self):
        """
        'stage.before_each_step(f)' should invoke some behaviour 'f' immediately before calling each step
        That callable should be a function that receives a signal and does something with it
        """
        data = [
            ("Bob", 25, 1),
            ("Sue", 30, 0),
            ("Joe", 17, 0),
            ("Leo", 30, 1)
        ]

        df = spark.createDataFrame(data, ["name", "age", "employed"])

        @pipeline_step()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @pipeline_step()
        def step_stringify(df):
            return df.withColumn("age_str", df.age.cast(T.StringType()))

        @pipeline_step()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        stage = Stage("dummy stage",
          step_just_adults(threshold=18),
          step_stringify,
          step_rename
        )

        global mutable_state
        mutable_state = 0

        def f(df):
            global mutable_state
            mutable_state = df.count()

        stage = stage.before_each_step(f)
        gen = stage.run_iter(df)
        _ = next(gen)
        self.assertEqual(mutable_state, 4)

        stage = stage.after_each_step(f)
        gen = stage.run_iter(df)
        _ = next(gen)
        self.assertEqual(mutable_state, 3)

