import unittest
import math

from nose.tools import raises

from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import log, length

from flow_writer import node
from tests import spark
from flow_writer import Stage


class TestStageAbstraction(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        cls.spark = spark

    @classmethod
    def teardown_class(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_stage_build_list_of_steps(self):
        """
        We should be able to create a Stage object which composes different steps
        A step is just a transformation whose only requirement is to receive a Dataframe and return another
        """
        data = [
            ("Allowed", 71, 192),
            ("Blocked", 30, 1274),
            ("DENIED", 70, 192),
            ("Failed", 19, 417),
            ("TCP_AUTH_HIT", None, 192),
            ("TCP_AUTH_MISS", 33, 1411),
            ("FAILED", 101, 718),
            ("ALLOWED", 19, 643)
        ]

        df = self.spark.createDataFrame(data, ["dvc_action", "url_length", "LengthBytes"])

        @node()
        def step_scale_url_length(df, mul_factor=3):
            df = df.withColumn("url_length_scaled", df.url_length * mul_factor)
            return df

        @node()
        def step_filter_small_packets(df, column, threshold):
            df = df.filter(df[column] >= threshold)
            return df

        stage = Stage("preprocess",
            step_scale_url_length(mul_factor=3),
            step_filter_small_packets(column="LengthBytes", threshold=200)
        )

        self.assertEqual(stage.get_num_steps(), 2)

        self.assertListEqual(stage.get_step_names(), ["step_scale_url_length", "step_filter_small_packets"])

        df_result = stage.run(df)

        self.assertEqual(df_result.count(), 5)

        self.assertListEqual(
            [r.url_length_scaled for r in df_result.collect()],
            [90, 57, 99, 303, 57]
        )

    def test_run_lazy(self):
        """
        To increase flexibility or control over the execution abstraction the pipeline should allow the lazy execution
        of its steps
        """
        data = [
            ("Bob", 25, 1),
            ("Sue", 30, 0),
            ("Joe", 17, 0),
            ("Leo", 30, 1)
        ]

        df = self.spark.createDataFrame(data, ["name", "age", "employed"])

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_stringify(df):
            return df.withColumn("age_str", df.age.cast(StringType()))

        @node()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        stage = Stage("run_lazy_demo",
              step_just_adults(threshold=18),
              step_stringify,
              step_rename,
        )

        df_strict = stage.run(df)

        step_generator = stage.run_iter(df)

        df_lazy_1 = next(step_generator)
        df_lazy_2 = next(step_generator)
        df_lazy_3 = next(step_generator)

        self.assertListEqual(df_strict.collect(), df_lazy_3.collect())

        self.assertListEqual([r.age for r in df_lazy_1.collect()], [25, 30, 30])
        self.assertListEqual([r.age_str for r in df_lazy_2.collect()], ["25", "30", "30"])
        self.assertListEqual([r.is_employed for r in df_lazy_3.collect()], [1, 0 ,1])

    @raises(StopIteration)
    def test_run_lazy_exhausted(self):
        """
        An exhausted generator should raise an exception signaling it reached the end
        """
        data = [
            ("Bob", 25, 1),
            ("Sue", 30, 0),
            ("Joe", 17, 0),
            ("Leo", 30, 1),
        ]

        df = self.spark.createDataFrame(data, ["name", "age", "employed"])

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_stringify(df):
            return df.withColumn("age_str", df.age.cast(StringType()))

        stage = Stage("run_lazy_demo",
              step_just_adults(threshold=18),
              step_stringify,
        )

        step_generator = stage.run_iter(df)

        while True:
            df = next(step_generator)

    def test_run_particular_step(self):
        """
        'stage.run_step(step_name)' should run a particular step by name
        """
        data = [
            ("Bob", 25, 1),
            ("Sue", 30, 0),
            ("Joe", 17, 0),
            ("Leo", 30, 1)
        ]

        df = self.spark.createDataFrame(data, ["name", "age", "employed"])

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_stringify(df):
            return df.withColumn("age_str", df.age.cast(StringType()))

        @node()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        stage = Stage("run_lazy_demo",
                      step_just_adults(threshold=18),
                      step_stringify,
                      step_rename,
                      )
        self.assertListEqual(
            [r.age for r in stage.run_step(df, 'step_just_adults').collect()],
            [25, 30, 30]
        )

    def test_run_fixed_number_of_steps(self):
        """
        To improve the execution abstraction, we should also be able to run a fixed number of steps.
        """
        data = [
            ("Bob", 25, 1),
            ("Sue", 30, 0),
            ("Joe", 17, 0),
            ("Leo", 30, 1)
        ]

        df = self.spark.createDataFrame(data, ["name", "age", "employed"])

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_stringify(df):
            return df.withColumn("age_str", df.age.cast(StringType()))

        @node()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        stage = Stage("run_lazy_demo",
                      step_just_adults(threshold=18),
                      step_stringify,
                      step_rename,
                      )

        df_strict = stage.run(df)

        df_step1 = stage.run_n_steps(df, 1)
        df_step2 = stage.run_n_steps(df, 2)
        df_step3 = stage.run_n_steps(df, 3)

        self.assertListEqual(df_strict.collect(), df_step3.collect())

        self.assertListEqual([r.age for r in df_step1.collect()], [25, 30, 30])
        self.assertListEqual([r.age_str for r in df_step2.collect()], ["25", "30", "30"])
        self.assertListEqual([r.is_employed for r in df_step3.collect()], [1, 0 ,1])

    def test_rebind_parameters(self):
        """
        We should be able to rebind parameters of certain steps. This will allow to make fast modifications to the data abstraction.
        """
        data = [
            ("Bob", "25", 19),
            ("Sue", "30", 16),
            ("Joe", "17", 20),
            ("Leo", "30", 29),
            ("Jay", "33", 10),
            ("Ali", "26", 25),
        ]

        df = self.spark.createDataFrame(data, ["name", "age", "score"])

        @node()
        def step_cast_to_int(df, column):
            return df.withColumn(column, df.age.cast(IntegerType()))

        @node()
        def step_transform(df, method, scale_factor):
            if method == 'log':
                return df.withColumn("score", log(df.score) * scale_factor)
            else:
                return df.withColumn("score", df.score * scale_factor)

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_count_name_length(df):
            return df.withColumn("name_length", length(df.name))

        transform = Stage("transform",
            step_transform(method='linear')(scale_factor=10)
        )

        df_transform = transform.run(df)

        self.assertListEqual([r.score for r in df_transform.collect()], [s[2] * 10 for s in data])

        transform_rebound = transform.rebind({
            'step_transform': {
                'method': 'log'
            }
        })

        df_rebind = transform_rebound.run(df)
        self.assertEqual(df_rebind.first().score, math.log(data[0][2])*10)

        transform_rebound = transform.rebind({
            'step_transform': {
                'scale_factor': 100
            }
        })

        df_rebind = transform_rebound.run(df)
        self.assertEqual(df_rebind.first().score, data[0][2]*100)

        #  all of the other pipelines should remain intact
        self.assertEqual(df_transform.collect(), transform.run(df).collect())

    def test_run_until(self):
        """
        'stage.run_until(step)' should run the whole stage from beginning until 'step'
        """
        data = [
            ("Bob", 25, 1),
            ("Sue", 30, 0),
            ("Joe", 17, 0),
            ("Leo", 30, 1)
        ]

        df = self.spark.createDataFrame(data, ["name", "age", "employed"])

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_stringify(df):
            return df.withColumn("age_str", df.age.cast(StringType()))

        @node()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        stage = Stage("run_lazy_demo",
                      step_just_adults(threshold=18),
                      step_stringify,
                      step_rename,
                      )

        df_run = stage.run_until(df, 'step_just_adults')

        self.assertListEqual(
            [r.age for r in df_run.collect()],
            [25, 30, 30]
        )

        self.assertFalse('age_str' in df_run.columns)

        df_run = stage.run_until(df, 'step_stringify')

        self.assertListEqual(
            [r.age for r in df_run.collect()],
            [25, 30, 30]
        )

        self.assertTrue('age_str' in df_run.columns)
        self.assertFalse('is_employed' in df_run.columns)

        df_run = stage.run_until(df, 'step_rename')

        self.assertListEqual(
            [r.age for r in df_run.collect()],
            [25, 30, 30]
        )

        self.assertTrue('age_str' in df_run.columns)
        self.assertTrue('is_employed' in df_run.columns)
