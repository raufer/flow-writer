import logging
from functools import reduce

from typing import Generator, Any, Dict, Callable, Iterator

from itertools import islice, chain

from flow_writer.abstraction import _call_with_requested_args
from flow_writer.logging import define_logging_file
from flow_writer.utils.print import color_text
from flow_writer.abstraction.dataflow import DataFlowAbstraction, copy_dataflow
from flow_writer.utils.recipes import consume

logger = logging.getLogger(__name__)
define_logging_file(logger)


class Pipeline(DataFlowAbstraction):
    """
    Abstraction for data abstraction execution
    This is the main abstraction for the a data abstraction pipeline

    A Pipeline represents a composition of stages that describe the transformations apply to a signal
    The only requirement of each Stage it is triggered by a signal and returns another one of the same type

    Pipelines can be composed in a immutable way by using the 'andThen' and the 'compose' operators
    """

    def __init__(self, name: str, *stages: Callable):
        super(Pipeline, self).__init__(name, *stages)
        self.stages = _parse_input_nodes(list(stages))
        self.stages_map = {n.name: i for i, n in enumerate(self.stages)}

    def get_nodes_map(self):
        return self.stages_map

    def get_num_stages(self):
        return len(self.stages)

    def get_num_steps(self):
        return sum([stage.get_num_steps() for stage in self.stages])

    def get_stage_names(self):
        return [stage.name for stage in self.stages]

    def with_description(self, description):
        """
        Add a description to the pipeline for documentation purposes.
        the operation is immutable, returning a new Pipeline with the extended data
        """
        pipeline = copy_dataflow(Pipeline(self.name, *self.stages), self)
        pipeline.description = description
        return pipeline

    def add_description(self, description):
        """
        Add a description to the pipeline for documentation purposes.
        This operator mutates the current pipeline. Don't use it
        """
        self.description = description

    def with_stage(self, stage, name=None):
        """
        Returns a new Pipeline object extended with the new stage
        'name' sets the name of the new pipeline, otherwise it stays the same
        """
        if not name:
            name = self.name

        stages = self.stages + [stage]
        pipeline = copy_dataflow(Pipeline(name, *stages), self)
        return pipeline

    def with_step(self, step):
        """
        Returns a step builder that can be used to select the insert location of the new step.
        The helper object has two location selectors
        - .after
        - .before

        Immutable operation.
        """
        import flow_writer.builders.step_builder as builder
        return builder.StepBuilder(self, step)

    def with_side_effect(self, side_effect):
        """
        Adds a controlled side effect to the stage
        This separates concerns and leaves the core of a given workflow, ie the transformations, with reduced noise
        We should be able to perform the side effect after a specific step, by name, or by position

        Returns a 'SideEffect' object which can be used to control where the side effect happens
        Immutable operation
        """
        import flow_writer.builders.side_effect_builder as builder
        return builder.SideEffectBuilder(self, side_effect)

    def rebind(self, rebindings: Dict[str, Any], name: str = None):
        """
        At construct time, the stages composing the pipeline have their steps bound to certain variables that parametrize their behaviour
        This bindings can be modified by passing a object with the new intended values, ie 'bindings'.

        'rebindings' format:
        {
            'stage_x': {
                'param_1': new_value,
                'param_2': new_value,
                ...
            },
            'stage_y': {
                'param_1': new_value,
                'param_2': new_value,
                ...
            },
        }

        Every closed value that is not referred in the new object, stays unmodified.
        This operation returns a new Pipeline object, leaving the base one intact.
        """
        if not name:
            name = self.name

        def loop(stages, acc):
            if not stages:
                return acc

            elif stages[0].name not in rebindings:
                return loop(stages[1:], acc + [stages[0]])

            else:
                stage_bindings = rebindings[stages[0].name]
                acc = acc + [stages[0].rebind(stage_bindings)]
                return loop(stages[1:], acc)

        rebounded_stages = loop(self.stages, list())
        pipeline = copy_dataflow(Pipeline(name, *rebounded_stages), self)
        return pipeline

    def andThen(self, pipeline, name: str = None):
        """
        Composes two pipelines. If 'p' and 'q' are pipelines, 'p.andThen(q)' results in a pipeline equivalent
        to the application of 'p' stages followed by 'q'
        """
        if not name:
            name = self.name

        stages = self.stages + pipeline.stages
        pipeline = copy_dataflow(Pipeline(name, *stages), self)
        return pipeline

    def compose(self, pipeline, name: str = None):
        """
        Composes two pipelines. If 'p' and 'q' are pipelines, 'p.compose(q)' results in a pipeline equivalent
        to the application of 'q' stages followed by 'p'
        """
        if not name:
            name = self.name

        stages = pipeline.stages + self.stages
        pipeline = copy_dataflow(Pipeline(name, *stages), self)
        return pipeline

    def after_each_step(self, f):
        """
        Registers a callable 'f' to be run immediately after each step.
        The data signal will be passed to 'f' as a kw arg 'df'
        Additionally, if 'f' asks for the step object, it will also be injected.
        """
        new_stages = [s.after_each_step(f) for s in self.stages]
        pipeline = copy_dataflow(Pipeline(self.name, *new_stages), self)
        return pipeline

    def before_each_step(self, f):
        """
        Registers a callable 'f' to be run immediately before each step.
        The data signal will be passed to 'f' as a kw arg 'df'
        Additionally, if 'f' asks for the step object, it will also be injected.
        """
        new_stages = [s.before_each_step(f) for s in self.stages]
        pipeline = copy_dataflow(Pipeline(self.name, *new_stages), self)
        return pipeline

    def after_each_stage(self, f):
        """
        Registers a callable 'f' to be run immediately after each the execution of each stage.
        The data signal will be passed to 'f' as a kw arg 'df'
        Additionally, if 'f' asks for the stage object, it will also be injected.
        """
        pipeline = copy_dataflow(Pipeline(self.name, *self.stages), self)
        pipeline.registry['after_each_stage'] = [f] if callable(f) else f
        return pipeline

    def before_each_stage(self, f):
        """
        Registers a callable 'f' to be run immediately before each execution of a stage.
        The data signal will be passed to 'f' as a kw arg 'df'
        Additionally, if 'f' asks for the stage object, it will also be injected.
        """
        pipeline = copy_dataflow(Pipeline(self.name, *self.stages), self)
        pipeline.registry['before_each_stage'] = [f] if callable(f) else f
        return pipeline

    def lock_dependencies(self, data=None, val_source_args=True, val_sink_args=False):
        """
        Locks all dependencies for which a callback was been registered.
        Returns a new Pipeline. This can be used to 'load' the pipeline with any external dependencies the steps may have.

        'data' should be a dict with the values to inject into the functions that are managing each dependency.
        data :: stage/step/step_arg/function/function_arg -> value

        'val_source_args' and 'val_sink_args' are flags argument validation should be performed.
        if true, an exception will be raised in some of the requirement arguments are not provided.
        """
        new_stages = [stage._lock_dependencies(data, val_source_args, val_sink_args) for stage in self.stages]
        pipeline = copy_dataflow(Pipeline(self.name, *new_stages), self)
        return pipeline

    def run(self, df):
        """
        Runs the pipeline by triggering the chain of stages with the signal 'df'
        With this version there is no control over the execution abstraction, the pipeline runs end to end.
        """
        logger.info("running Pipeline '{}' ...".format(self.name))
        return consume(_run_gen(self, self.stages, df))

    def run_nth(self, df, n: int):
        """
        Runs the nth stage of the pipeline, index zero based
        """
        logger.info("running Pipeline '{}' ... Stage nth: '{}'".format(self.name, n))
        return self.stages[n].run(df=df)

    def run_stage(self, stage: str, df):
        """
        Runs a particular stage by name
        """
        logger.info("running Pipeline '{}' ... Stage '{}'".format(self.name, stage))
        return self.run_nth(df, self.stages_map[stage])

    def run_until(self, df, location: str):
        """
        Runs the pipeline from the beginning until a particular location
        """
        path = _parse_path(location)
        last_stage_index = self.stages_map[path['stage']]

        logger.info("running Pipeline '{}' ... Stage '{}'".format(self.name, path['stage']))

        if 'step' in path:
            df_temp = self.run_n_stages(df, last_stage_index)
            return self.stages[last_stage_index].run_until(df_temp, path['step'])
        else:
            return self.run_n_stages(df, last_stage_index + 1)

    def run_step(self, df, location: str):
        """
        Runs a particular step with the path given by 'location'
        """
        path = _parse_path(location)
        logger.info("running Pipeline '{}' ... Stage '{}'".format(self.name, path['stage']))

        stage = self.stages[self.stages_map[path['stage']]]
        return stage.run_step(df, path['step'])

    def run_iter_stages(self, df) -> Iterator[Any]:
        """
        Runs the pipeline in a lazy fashion.
        The execution abstraction is under the control of the caller.
        Each stage execution is generated when demanded.

        Can be useful to decide midway if an execution should proceed or stop due to some condition.
        Note: the returned signal is immediately after each run, prior to the invocation of any execution cycle sinks

        #  TODO: stage.after_each_step is not being run with each lazy run of a stage. Just in the subsequent call to 'next'
        """
        logger.info("running Pipeline '{}' ...".format(self.name))
        gen = _run_gen(self, self.stages, df)
        it = reduce(
            lambda acc, x: chain(acc, [islice(gen, x)]),
            [len(s.steps) for s in self.stages],
            iter(())
        )
        return map(consume, it)

    def run_iter_steps(self, df) -> Generator[Any, None, None]:
        """
        Runs the pipeline in a lazy fashion, at the most granular level, the step.
        This is important to provide the caller a more fine control over the execution abstraction
        """
        logger.info("running Pipeline '{}' ...".format(self.name))
        return _run_gen(self, self.stages, df)

    def run_n_steps(self, df, n: int):
        """
        Runs strictly 'n' steps throughout the pipeline and returns the signal at that point
        """
        logger.info("running Pipeline '{}' ...".format(self.name))

        step_gen = self.run_iter_steps(df)

        def loop(n, acc):
            if n == 0:
                return acc
            else:
                return loop(n - 1, next(step_gen))

        return loop(n, df)

    def run_n_stages(self, df, n: int):
        """
        Runs a fixed number of steps, in a strict way.
        Increases the execution abstraction control
        """
        logger.info("running Pipeline '{}' ...".format(self.name))
        result = consume(_run_gen(self, self.stages[:max(n, 0)], df)) or df
        return result

    def run_iter_with_sinks(self, df, data=None):
        """
        Runs the pipeline with the post sinks registered
        These sinks are with respect to the step dependencies.
        We will add the registered step dependencies handlers to the list of functions to run in 'post-run' phase.

        We are setting "call='run_iter_with_sinks'" to tell the underlying stage objects
        to run the registered step dependencies managers

        'data' should be a dict with the values to inject into the functions that are managing each dependency.
        data :: stage/step/step_arg/function/function_arg -> value

        If data is available it means we need to bind arguments in the writer function
        This binding is expected to be partial.
        """
        logger.info("running Pipeline '{}' ...".format(self.name))
        if data:
            stages = [s._lock_dependencies(data, val_source_args=False, val_sink_args=True) for s in self.stages]
            return _run_gen(self, stages, df, call='run_iter_with_sinks')

        return _run_gen(self, self.stages, df, call='run_iter_with_sinks')

    def run_with_sinks(self, df, data=None):
        """
        Strict version of 'run_iter_with_sinks'
        """
        logger.info("running Pipeline '{}' ...".format(self.name))
        return consume(self.run_iter_with_sinks(df, data))

    def fit(self, df, data=None):
        """
        An alias to 'pipeline.run_with_sinks'. Can be used to give more semantics to ML use cases
        """
        logger.info("fitting Pipeline '{}' ...".format(self.name))
        return self.run_with_sinks(df, data)

    def __repr__(self):
        header = color_text("Pipeline: '{}'".format(self.name), 'red')
        description = color_text(("\n{}".format(self.description) if self.description else ""), 'white')
        separator = "\n---------\n---------\n"
        body = "\n".join(["{}".format(str(s)) for s in self.stages])
        return header + description + separator + body + '\n'


def _run_gen(pipeline, stages, df, call='run_iter') -> Generator[Any, None, None]:
    """
    Returns a generator that knows how to sequentially run through all of the computational steps
    'call' represents the underlying stage method that we want to be responsible for generating the intended computation
    It defaults to 'run_iter' to simply run the main data flow computations.
    For instance, call='run_iter_with_sinks' will tell the underlying stage objects to run the step dependencies managers registered

    'data' should be a dict with the values to inject into the functions that are managing each dependency.
    data :: stage/step/step_arg/function/function_arg -> value
    """
    if not stages:
        return None

    logger.info("running... PIPELINE('{}') / STAGE('{}')".format(pipeline.name, stages[0].name))
    _prerun(pipeline, stages[0], df)
    df_acc = yield from getattr(stages[0], call)(df)
    _postrun(pipeline, stages[0], df_acc)
    yield from _run_gen(pipeline, stages[1:], df_acc, call)
    return df_acc


def _parse_input_nodes(l):
    """
    Since the Pipeline is the main data abstraction abstraction, we might want to construct a Pipeline without resorting to
    Stages, simply by passing a variable number of steps.
    This behaviour is also accepted and a anonymous default stage is created.
    """
    import flow_writer.abstraction.stage

    if l[0].__class__.__name__ == 'Stage':
        return l
    else:
        anonymous_stage = flow_writer.abstraction.stage.Stage('Anonymous Stage', *l)
        return [anonymous_stage]


def _parse_path(path):
    """
    Splits the directory like node 'path' into the different components
    Returns a dict with all of the found components populated
    """
    if '/' in path:
        stage, step = path.strip().split('/')
        path = {
            'stage': stage.strip(),
            'step': step.strip()
        }
    else:
        stage = path.strip()
        path = {
            'stage': stage.strip()
        }

    return path


def _prerun(pipeline, stage, df):
    """Hook to run generic code immediately before the execution of each node"""
    logger.info("Calling: 'before each stage'")
    for f in pipeline.registry.get('before_each_stage', []):
        _call_with_requested_args(f, pipeline=pipeline, stage=stage, df=df)


def _postrun(pipeline, stage, df):
    """Hook to run generic code immediately after the execution of each node"""
    logger.info("Calling: after each stage'")
    for f in pipeline.registry.get('after_each_stage', []):
        _call_with_requested_args(f, pipeline=pipeline, stage=stage, df=df)
