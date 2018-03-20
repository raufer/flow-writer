import unittest

from nose.tools import raises

from flow_writer import Pipeline
from flow_writer import Stage
from flow_writer import node


class TestDataFlowAbstraction(unittest.TestCase):

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

    def test_dataflow_copy_descriptions(self):
        """
        'copy_dataflow' should extract the necessary fields to inject in the newly created derived flow_writer
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
            extract_sesions
        )

        pipeline = pipeline.with_description('This is a description')

        derived = pipeline.with_stage(postprocess)

        self.assertEqual(pipeline.description, derived.description)

    def test_dataflow_copy_side_effects(self):
        """
        'copy_dataflow' should extract the necessary fields to inject in the newly created derived flow_writer
        The side effects should also be coppied
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
                            extract_sesions
                            )

        pipeline = pipeline.with_description('This is a description')

        @node()
        def write(df, path):
            return df

        pipeline = pipeline.with_side_effect(write).after('preprocess/step_b')

        derived = pipeline.with_stage(postprocess)

        self.assertEqual(pipeline.description, derived.description)
        self.assertEqual(len(pipeline.side_effects), len(derived.side_effects))
