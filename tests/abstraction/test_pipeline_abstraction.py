import unittest

import math
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import lit, log, length

from tests import spark
from flow_writer import Stage
from flow_writer import Pipeline
from flow_writer import node


class TestPipelineAbstraction(unittest.TestCase):

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

    def test_build_pipeline(self):
        """
        To construct a pipeline abstraction, one or more stages are needed.
        These will be composed into a execution abstraction that can be controlled by the caller.
        """

        @node()
        def step_a(df, tokenize):
            return df

        @node()
        def step_b(df, interpolate):
            return df

        @node()
        def step_c(df, groupby):
            return df

        @node()
        def step_d(df, keep_source, threshold):
            return df

        @node()
        def step_e(df, lookup):
            return df

        preprocess = Stage("preprocess",
            step_a(tokenize=True),
            step_b(interpolate='linear'),
        )

        extract_sesions = Stage("feature-extraction",
           step_c(groupby='src_user_name'),
           step_d(keep_source=False, threshold=100),
        )

        postprocess = Stage("postprocess",
            step_e(lookup_table={'A': 1, 'B': 0}),
        )

        pipeline = Pipeline("Pipeline A",
            preprocess,
            extract_sesions,
            postprocess
        )

        self.assertEqual(pipeline.get_num_stages(), 3)
        self.assertEqual(pipeline.get_num_steps(), 5)
        self.assertEqual(pipeline.get_stage_names(), ["preprocess", "feature-extraction", "postprocess"])

    def test_run_pipeline(self):
        """
        We should be able to run the whole pipeline
        """
        data = [
            ("Bob", "25", 1),
            ("Sue", "30", 0),
            ("Joe", "17", 0),
            ("Leo", "30", 1),
            ("Jay", "33", 1),
            ("Ali", "26", 1),
        ]

        df = self.spark.createDataFrame(data, ["name", "age_str", "employed"])

        @node()
        def step_stringify(df):
            return df.withColumn("age", df.age_str.cast(IntegerType()))

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_just_employed(df):
            return df.filter(df.employed > 0)

        @node()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        preprocess = Stage("preprocess", step_stringify)

        transform = Stage("transform",
            step_just_adults(threshold=18),
            step_just_employed
        )

        postprocess = Stage("postprocess", step_rename)

        pipeline = Pipeline("Pipeline A",
            preprocess,
            transform,
            postprocess
        )

        df = pipeline.run(df)

        self.assertEqual(df.count(), 4)

    def test_pipeline_add_stage(self):
        """
        We should be able to add new stages to a pipeline without changing the current objects in memory
        """
        data = [
            ("Bob", "25", 1),
            ("Sue", "30", 0),
            ("Joe", "17", 0),
            ("Leo", "30", 1),
            ("Jay", "33", 1),
            ("Ali", "26", 1),
        ]

        df = self.spark.createDataFrame(data, ["name", "age_str", "employed"])

        @node()
        def step_stringify(df):
            return df.withColumn("age", df.age_str.cast(IntegerType()))

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_just_employed(df):
            return df.filter(df.employed > 0)

        @node()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        preprocess = Stage("preprocess", step_stringify)

        transform = Stage("transform",
              step_just_adults(threshold=18),
              step_just_employed
        )

        postprocess = Stage("postprocess", step_rename)

        pipeline = Pipeline("Pipeline A",
            preprocess,
            transform
        )

        pipeline_extended = pipeline.with_stage(postprocess)

        df = pipeline_extended.run(df)

        self.assertEqual(df.count(), 4)
        self.assertListEqual([r.is_employed for r in df.collect()], [1, 1, 1, 1])

    def test_run_pipeline_lazy(self):
        """
        For improved workflow control, we should be able to run the pipeline in a lazy fashion, computing the stages as needed
        """
        data = [
            ("Bob", "25", 1),
            ("Sue", "30", 0),
            ("Joe", "17", 0),
            ("Leo", "30", 1),
            ("Jay", "33", 1),
            ("Ali", "26", 1),
        ]

        df = self.spark.createDataFrame(data, ["name", "age_str", "employed"])

        @node()
        def step_stringify(df):
            return df.withColumn("age", df.age_str.cast(IntegerType()))

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_just_employed(df):
            return df.filter(df.employed > 0)

        @node()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        preprocess = Stage("preprocess", step_stringify)

        transform = Stage("transform",
              step_just_adults(threshold=18),
              step_just_employed
        )

        postprocess = Stage("postprocess", step_rename)

        pipeline = Pipeline("Pipeline A",
            preprocess,
            transform,
            postprocess
        )

        df_strict = pipeline.run(df)

        self.assertEqual(df_strict.count(), 4)

        stage_generator = pipeline.run_iter_stages(df)

        df_lazy_1 = next(stage_generator)
        df_lazy_2 = next(stage_generator)
        df_lazy_3 = next(stage_generator)

        self.assertListEqual(df_strict.collect(), df_lazy_3.collect())

        self.assertListEqual([r.age for r in df_lazy_1.collect()], [25, 30, 17, 30, 33, 26])
        self.assertListEqual([r.age for r in df_lazy_2.collect()], [25, 30, 33, 26])

    def test_run_pipeline_n_stages(self):
        """
        For improved workflow control, we should be able to control the number of steps we run and collect intermediary results.
        """
        data = [
            ("Bob", "25", 1),
            ("Sue", "30", 0),
            ("Joe", "17", 0),
            ("Leo", "30", 1),
            ("Jay", "33", 1),
            ("Ali", "26", 1),
        ]

        df = self.spark.createDataFrame(data, ["name", "age_str", "employed"])

        @node()
        def step_stringify(df):
            return df.withColumn("age", df.age_str.cast(IntegerType()))

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_just_employed(df):
            return df.filter(df.employed > 0)

        @node()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        preprocess = Stage("preprocess", step_stringify)

        transform = Stage("transform",
              step_just_adults(threshold=18),
              step_just_employed
        )

        postprocess = Stage("postprocess", step_rename)

        pipeline = Pipeline("Pipeline A",
            preprocess,
            transform,
            postprocess
        )

        df = pipeline.run_n_stages(df, 2)

        self.assertEqual(df.count(), 4)
        self.assertListEqual([r.employed for r in df.collect()], [1, 1, 1, 1])

    def test_run_stage_by_name(self):
        """
        To improve the control over the execution abstraction and increase testability we should be able to run stages by name
        """
        data = [
            ("Bob", "25", 1),
            ("Sue", "30", 0),
            ("Joe", "17", 0),
            ("Leo", "30", 1),
            ("Jay", "33", 1),
            ("Ali", "26", 1),
        ]

        df = self.spark.createDataFrame(data, ["name", "age_str", "employed"])

        @node()
        def step_stringify(df):
            return df.withColumn("age", df.age_str.cast(IntegerType()))

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_just_employed(df):
            return df.filter(df.employed > 0)

        @node()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        preprocess = Stage("preprocess", step_stringify)

        transform = Stage("transform",
          step_just_adults(threshold=18),
          step_just_employed
        )

        postprocess = Stage("postprocess", step_rename)

        pipeline = Pipeline("Pipeline A",
            preprocess,
            transform,
            postprocess
        )

        df_s1 = pipeline.run_stage("preprocess", df)
        self.assertEqual(df_s1.count(), 6)
        self.assertListEqual([r.age for r in df_s1.collect()], [25, 30, 17, 30, 33, 26])

        df_s2 = pipeline.run_stage("postprocess", df)
        self.assertEqual(df_s2.count(), 6)
        self.assertListEqual([r.is_employed for r in df_s2.collect()], [1, 0, 0, 1, 1, 1])

        df_s3 = pipeline.run_stage("transform", pipeline.run_stage("preprocess", df))
        self.assertEqual(df_s3.count(), 4)

    def test_compose_pipelines_append(self):
        """
        We should be able to compose different pipelines, with a 'andThen' operator
        it should return a new pipeline that is the composition of the two
        """
        data = [
            ("Bob", "25", 2),
            ("Sue", "30", 2),
            ("Joe", "17", 2),
            ("Leo", "30", 2),
            ("Jay", "33", 2),
            ("Ali", "26", 2),
        ]

        df = self.spark.createDataFrame(data, ["name", "age_str", "employed"])

        @node()
        def step_zerify(df):
            return df.withColumn("employed", lit(0))

        @node()
        def step_onify(df):
            return df.withColumn("employed", lit(1))

        stage_map_zeros = Stage("zeros", step_zerify)
        stage_map_ones = Stage("ones", step_onify)

        pipeline_ones = Pipeline("Pipeline Map Ones", stage_map_ones)
        pipeline_zeros = Pipeline("Pipeline Map Zeros", stage_map_zeros)

        pipeline = pipeline_ones.andThen(pipeline_zeros)

        df = pipeline.run(df)
        self.assertListEqual([r.employed for r in df.collect()], [0]*len(data))

    def test_compose_pipelines_prepend(self):
        """
        We should be able to compose different pipelines, with a 'compose' operator
        it should return a new pipeline that is the composition of the two
        """
        data = [
            ("Bob", "25", 2),
            ("Sue", "30", 2),
            ("Joe", "17", 2),
            ("Leo", "30", 2),
            ("Jay", "33", 2),
            ("Ali", "26", 2),
        ]

        df = self.spark.createDataFrame(data, ["name", "age_str", "employed"])

        @node()
        def step_zerify(df):
            return df.withColumn("employed", lit(0))

        @node()
        def step_onify(df):
            return df.withColumn("employed", lit(1))

        stage_map_zeros = Stage("zeros", step_zerify)
        stage_map_ones = Stage("ones", step_onify)

        pipeline_ones = Pipeline("Pipeline Map Ones", stage_map_ones)
        pipeline_zeros = Pipeline("Pipeline Map Zeros", stage_map_zeros)

        pipeline = pipeline_ones.compose(pipeline_zeros)

        df = pipeline.run(df)
        self.assertListEqual([r.employed for r in df.collect()], [1]*len(data))

    def test_pipeline_with_annonymous_stage(self):
        """
        Since the Pipeline is the main data abstraction abstraction, we might want to construct a Pipeline without resorting to
        Stages, simply by passing a variable number of steps.
        This behaviour is also accepted and a anonymous default stage is created.
        """
        data = [
            ("Bob", "25", 2),
            ("Sue", "30", 2),
            ("Joe", "17", 2),
            ("Leo", "30", 2),
            ("Jay", "33", 2),
            ("Ali", "26", 2),
        ]

        df = self.spark.createDataFrame(data, ["name", "age_str", "employed"])

        @node()
        def step_zerify(df):
            return df.withColumn("employed", lit(0))

        @node()
        def step_onify(df):
            return df.withColumn("employed", lit(1))

        stage_map_zeros = Stage("zeros", step_zerify)
        stage_map_ones = Stage("ones", step_onify)

        pipeline_with_formal_stages = Pipeline("Pipeline Map Ones", stage_map_ones, stage_map_zeros)

        pipeline_with_anonymous_stage = Pipeline("Pipeline Map Ones", step_onify, step_zerify)

        self.assertEqual(
            pipeline_with_formal_stages.run(df).collect(),
            pipeline_with_anonymous_stage.run(df).collect()
        )

    def test_pipeline_run_n_steps_lazy(self):
        """
        We should also have control over the execution abstraction at the most granular operation, ie the `step`.
        The caller should be able to run a determined number of steps throughout the various stages that compose the pipeline on demand
        """
        data = [
            ("Bob", "25", 2),
            ("Sue", "30", 2),
            ("Joe", "17", 2),
            ("Leo", "30", 2),
            ("Jay", "33", 2),
            ("Ali", "26", 2),
        ]

        df = self.spark.createDataFrame(data, ["name", "age_str", "employed"])

        @node()
        def step_zerify(df):
            return df.withColumn("employed", lit(0))

        @node()
        def step_onify(df):
            return df.withColumn("employed", lit(1))

        @node()
        def step_twoify(df):
            return df.withColumn("employed", lit(2))

        @node()
        def step_threeify(df):
            return df.withColumn("employed", lit(3))

        stage_map_zeros = Stage("zeros", step_zerify)
        stage_map_ones_and_twos = Stage("ones and twos", step_onify, step_twoify)
        stage_map_threes = Stage("three", step_threeify)

        pipeline = Pipeline("workflow",
            stage_map_zeros,
            stage_map_ones_and_twos,
            stage_map_threes
        )

        df_strict = pipeline.run(df)

        step_generator = pipeline.run_iter_steps(df)

        for i in range(pipeline.get_num_steps()):
            df_gen_step = next(step_generator)
            self.assertListEqual([r.employed for r in df_gen_step.collect()], [i]*len(data))

        self.assertListEqual(df_gen_step.collect(), df_strict.collect())

    def test_pipeline_run_n_steps_strict(self):
        """
        We should also have control over the execution abstraction at the most granular operation, ie the `step`.
        The caller should be able to run a determined number of steps throughout the various stages that compose the pipeline, in a strict way
        """
        data = [
            ("Bob", "25", -1),
            ("Sue", "30", -1),
            ("Joe", "17", -1),
            ("Leo", "30", -1),
            ("Jay", "33", -1),
            ("Ali", "26", -1),
        ]

        df = self.spark.createDataFrame(data, ["name", "age_str", "employed"])

        @node()
        def step_zerify(df):
            return df.withColumn("employed", lit(0))

        @node()
        def step_onify(df):
            return df.withColumn("employed", lit(1))

        @node()
        def step_twoify(df):
            return df.withColumn("employed", lit(2))

        @node()
        def step_threeify(df):
            return df.withColumn("employed", lit(3))

        stage_map_zeros = Stage("zeros", step_zerify)
        stage_map_ones_and_twos = Stage("ones and twos", step_onify, step_twoify)
        stage_map_threes = Stage("three", step_threeify)

        pipeline = Pipeline("workflow",
            stage_map_zeros,
            stage_map_ones_and_twos,
            stage_map_threes
        )

        df_strict = pipeline.run(df)

        df_one_step = pipeline.run_n_steps(df, 1)
        self.assertListEqual([r.employed for r in df_one_step.collect()], [0]*len(data))

        df_two_steps = pipeline.run_n_steps(df, 2)
        self.assertListEqual([r.employed for r in df_two_steps.collect()], [1]*len(data))

        df_three_steps = pipeline.run_n_steps(df, 3)
        self.assertListEqual([r.employed for r in df_three_steps.collect()], [2]*len(data))

        self.assertEqual(df_strict.collect(), pipeline.run_n_steps(df, pipeline.get_num_steps()).collect())

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
        def step_process_score(df, method, scale_factor):
            if method == 'log':
                return df.withColumn("score", log(10.0, df.score) * scale_factor)
            else:
                return df.withColumn("score", df.score * scale_factor)

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_count_name_length(df):
            return df.withColumn("name_length", length(df.name))

        stage_preprocess = Stage("preprocess",
            step_cast_to_int(column='age'),
        )

        stage_transform = Stage("transform",
            step_process_score(method='linear')(scale_factor=10),
            step_just_adults(threshold=18)
        )

        stage_postprocess = Stage("postprocess",
            step_count_name_length
        )

        pipeline = Pipeline("Analyze users scores",
            stage_preprocess,
            stage_transform,
            stage_postprocess
        )

        df_original = pipeline.run(df)
        self.assertEqual(df_original.count(), 5)

        self.assertListEqual([r.score for r in df_original.collect()], [s[2] * 10 for s in data if int(s[1]) >= 18])

        custom_behaviour = {
            'transform': {
                'step_process_score': {
                    'method': 'log'
                },
                'step_just_adults': {
                    'threshold': 25
                }
            }
        }

        pipeline_log_transform = pipeline.rebind(custom_behaviour)

        df_log = pipeline_log_transform.run(df)

        self.assertEqual(df_log.count(), 4)
        self.assertListEqual(
            [round(r.score) for r in df_log.collect()],
            [round(math.log10(d[2])*10) for d in data if int(d[1]) > 25]
        )

        another_costum_behaviour = {
            'transform': {
                'step_process_score': {
                    'scale_factor': 100
                },
                'step_just_adults': {
                    'threshold': 31
                }
            }
        }

        pipeline_increase_threshold = pipeline_log_transform.rebind(another_costum_behaviour)

        df_increased_threshold = pipeline_increase_threshold.run(df)

        self.assertEqual(df_increased_threshold.count(), 1)
        self.assertListEqual(
            [round(r.score) for r in df_increased_threshold.collect()],
            [round(math.log10(d[2])*100) for d in data if int(d[1]) > 31]
        )

        #  ensure immutability of the pipelines that served as template
        self.assertEqual(df_original.collect(), pipeline.run(df).collect())
        self.assertEqual(df_log.collect(), pipeline_log_transform.run(df).collect())

    def test_pipeline_run_step(self):
        """
        'pipeline.run_step(step)' should run just the step specified by name
        """
        data = [
            ("Bob", "25", -1),
            ("Sue", "30", -1),
            ("Joe", "17", -1),
            ("Leo", "30", -1),
            ("Jay", "33", -1),
            ("Ali", "26", -1),
        ]

        df = self.spark.createDataFrame(data, ["name", "age_str", "employed"])

        @node()
        def step_zerify(df):
            return df.withColumn("employed", lit(0))

        @node()
        def step_onify(df):
            return df.withColumn("employed", lit(1))

        @node()
        def step_twoify(df):
            return df.withColumn("employed", df.employed + 1)

        @node()
        def step_threeify(df):
            return df.withColumn("employed", lit(3))

        stage_map_zeros = Stage("zeros", step_zerify)
        stage_map_ones_and_twos = Stage("ones and twos", step_onify, step_twoify)
        stage_map_threes = Stage("three", step_threeify)

        pipeline = Pipeline("workflow",
                            stage_map_zeros,
                            stage_map_ones_and_twos,
                            stage_map_threes
                            )

        df_run = pipeline.run_step(df, 'ones and twos/step_onify')
        self.assertEqual(df_run.first().employed, 1)

        df_run = pipeline.run_step(df, 'three/step_threeify')
        self.assertEqual(df_run.first().employed, 3)

    def test_pipeline_run_until(self):
        """
        'pipeline.run_until(step)' should pass the signal through all of the steps until 'step'
        """
        data = [
            ("Bob", "25", -1),
            ("Sue", "30", -1),
            ("Joe", "17", -1),
            ("Leo", "30", -1),
            ("Jay", "33", -1),
            ("Ali", "26", -1),
        ]

        df = self.spark.createDataFrame(data, ["name", "age_str", "employed"])

        @node()
        def step_zerify(df):
            return df.withColumn("employed", df.employed + 1)

        @node()
        def step_onify(df):
            return df.withColumn("employed", df.employed + 1)

        @node()
        def step_twoify(df):
            return df.withColumn("employed", df.employed + 1)

        @node()
        def step_threeify(df):
            return df.withColumn("employed", df.employed + 1)

        stage_map_zeros = Stage("zeros", step_zerify)
        stage_map_ones_and_twos = Stage("ones and twos", step_onify, step_twoify)
        stage_map_threes = Stage("three", step_threeify)

        pipeline = Pipeline("workflow",
                            stage_map_zeros,
                            stage_map_ones_and_twos,
                            stage_map_threes
                            )

        df_run = pipeline.run_until(df, 'zeros/step_zerify')
        self.assertEqual(df_run.first().employed, 0)

        df_run = pipeline.run_until(df, 'ones and twos/step_onify')
        self.assertEqual(df_run.first().employed, 1)

        df_run = pipeline.run_until(df, 'three/step_threeify')
        self.assertEqual(df_run.first().employed, 3)

        df_run = pipeline.run_until(df, 'three')
        self.assertEqual(df_run.first().employed, 3)
