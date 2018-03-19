import logging

from copy import deepcopy
from typing import Callable, Generator, Any, Dict

from flow_writer.abstraction import _call_with_requested_args
from flow_writer.abstraction.node import extract_signal
from flow_writer.dependency_manager import _step_dependencies_sinks
from flow_writer.abstraction.node.step import Step
from flow_writer.ops.function_ops import closed_bindings
from flow_writer.utils.print import color_text
from flow_writer.abstraction.dataflow import DataFlowAbstraction, copy_dataflow
from flow_writer.utils.recipes import consume

logger = logging.getLogger(__name__)


class Stage(DataFlowAbstraction):
    """
    Abstraction for data abstraction execution

    A Stage represents a composition of steps that describe the transformations steps to apply to a signal
    The only requirement of each step is that it consist of a callable receiving and returning a sql.DataFrame

    In order to avoid writing boilerplate code we need to parametrize the steps that operate a pipeline
    Each step needs to receive an input signal to operate on. All of the other parameters should be locked.

    We cannot lock them before hand because we will just have the signal at run time.
    So we need curried versions of the functions to lock the chosen arguments to operate later on.
    """

    def __init__(self, name: str, *steps: Callable):
        """
        At construction time the user is allowed to pass just simple curried functions.
        However, once we are in the data flow_writer context, pipelines cna be composed and thus we need to deal with this
        situation.

        In a way, we are emulating multiple constructors
        """
        super(Stage, self).__init__(name, *steps)

        self.steps = [
            Step(f) if not hasattr(f, 'id') else deepcopy(f)
            for f in steps
        ]

        self.steps_map = {s.name: i for i, s in enumerate(self.steps)}

    def get_nodes_map(self):
        return self.steps_map

    def get_num_steps(self):
        return len(self.steps)

    def get_step_names(self):
        return [s.name for s in self.steps]

    def with_description(self, description):
        """
        Add a description to the pipeline for documentation purposes.
        the operation is immutable, returning a new Pipeline with the extended data
        """
        stage = copy_dataflow(Stage(self.name, *self.steps), self)
        stage.description = description
        return stage

    def add_description(self, description):
        """
        Add a description to the Stage for documentation purposes.
        This operator mutates the current pipeline. Don't use it.
        """
        self.description = description

    def after_each_step(self, f):
        """
        Registers a callable 'f' to be run immediately after each step.
        The data signal will be passed to 'f' as a kw arg 'df'
        Additionally, if 'f' asks for the step object, it will also be injected.
        """
        stage = copy_dataflow(Stage(self.name, *self.steps), self)
        stage.registry['after_each_step'] = [f] if callable(f) else f
        return stage

    def before_each_step(self, f):
        """
        Registers a callable 'f' to be run immediately before each step.
        The data signal will be passed to 'f' as a kw arg 'df'
        Additionally, if 'f' asks for the step object, it will also be injected.
        """
        stage = copy_dataflow(Stage(self.name, *self.steps), self)
        stage.registry['before_each_step'] = [f] if callable(f) else f
        return stage

    def with_step(self, step: Callable):
        """
        Returns a new Stage object extended with the new step
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
        At construct time, the steps composing the stage are normally bound to operate under certain parameters
        This bindings can be modified by passing a object with the new intended values, ie 'bindings'.

        Every closed value that is not referred in the new object, stays unmodified.
        This operation returns a new Stage object, leaving the base one intact.
        """
        if not name:
            name = self.name

        def loop(steps, acc):
            if not steps:
                return acc

            elif steps[0].name not in rebindings:
                return loop(steps[1:], acc + [steps[0]])

            else:
                step = self.steps[self.steps_map[steps[0].name]]
                step_rebound = step(**rebindings[step.name])
                acc = acc + [step_rebound]
                return loop(steps[1:], acc)

        rebounded_steps = loop(self.steps, list())
        stage = copy_dataflow(Stage(name, *rebounded_steps), self)
        return stage

    def lock_dependencies(self):
        """
        Locks all dependencies for which a callback was been registered.
        Returns a new Stage. This can be used to 'load' the pipeline with any external dependencies the steps may have.
        """

        def _rebind_args(entries):
            return {entry.arg: entry.source() for entry in entries if entry.source}

        steps_with_cache = [(step, step.registry.get("dependencies", list())) for step in self.steps]

        rebind_dict = {step.name: _rebind_args(entries) for step, entries in steps_with_cache}

        rebind_dict = {k: v for k, v in rebind_dict.items() if v}

        new_stage = self.rebind(rebind_dict)

        return new_stage

    def run(self, df):
        """
        Runs the stage by triggering the sequential transformations with the signal 'df'
        With this version there is no control over the execution abstraction, the stage runs end to end.
        """
        logger.info("running Stage '{}' ...".format(self.name))
        return consume(_run_gen(self, self.steps, df))

    def run_step(self, df, step):
        """
        Runs a particular step by name
        """
        logger.info("running Stage '{}' ...".format(self.name))
        steps = [self.steps[self.steps_map[step]]]
        return consume(_run_gen(self, steps, df))

    def run_until(self, df, step):
        """
        Runs the pipeline from the beginning until 'step'
        """
        logger.info("running Stage '{}' ...".format(self.name))
        steps = self.steps[:self.steps_map[step] + 1]
        return consume(_run_gen(self, steps, df))

    def run_iter(self, df) -> Generator[Any, None, None]:
        """
        Runs the stage in a lazy fashion.
        The execution abstraction is under the control of the caller.
        Each step is generated when demanded.

        Can be useful to decide midway if an execution should proceed or stop due to some condition.
        """
        logger.info("running Stage '{}' ...".format(self.name))
        return _run_gen(self, self.steps, df)

    def run_n_steps(self, df, n: int):
        """
        Runs a fixed number of steps, in a strict way.
        Increases the execution abstraction control
        """
        logger.info("running Stage '{}' ...".format(self.name))
        return consume(_run_gen(self, self.steps[:n], df))

    def run_iter_with_sinks(self, df):
        """
        Runs the stage with the post sinks registered
        These sinks are with respect to the steps dependencies.
        We will add the registered step dependencies handlers to the list of functions to run in 'post-run' phase.
        """
        logger.info("running Stage '{}' ...".format(self.name))
        post_sinks = [_step_dependencies_sinks]
        return _run_gen(self, self.steps, df, post_sinks=post_sinks)

    def run_with_sinks(self, df):
        """
        Strict version of 'run_iter_with_sinks'
        """
        return consume(self.run_iter_with_sinks(df))

    def fit(self, df):
        """
        An alias to 'stage.run_with_sinks'. Can be used to give more semantics to ML use cases
        """
        return self.run_with_sinks(df)

    def __repr__(self):
        header = color_text('* ', 'green') + \
                 color_text("Stage", "blue") + \
                 color_text(": ", "green") + \
                 color_text("'{}'".format(self.name), 'blue')

        description = "\n* {} *".format(self.description) if self.description else ""

        separator = "\n------\n"

        step_names = [f.name if f.__class__.__name__ != 'SideEffect' else color_text(f.name, 'red') for f in self.steps]

        closed_vars = [closed_bindings(s.f) for s in self.steps]
        fdocs = [f.docs for f in self.steps]

        body = "\n".join([
            color_text("\tStep: {} {} \n\t{}".format(sname, _print_dict_as_arglist(cvars), doc), attrs=['bold'])
            for sname, cvars, doc in zip(step_names, closed_vars, fdocs)
        ])

        return header + description + separator + body + '\n'


def _run_gen(stage, steps, df, pre_sinks=None, post_sinks=None) -> Generator[Any, None, None]:
    """
    Lazy version of '_run'
    Returns a generator that describes the computation of the whole data flow_writer.
    There are three execution phases:
        - prerun
        - run
        - postrun

    'run' is the main execution flow_writer, wehre the singal is propagated through the circuit.
    'prerun' and 'postrun' will invoke generic code to be run immediately before or after the execution of each step, respectively.
    They consider the list of functions received, 'pre_sinks' and 'post_sinks'
    The nodes are sinks because the data flow_writer context does not look to their returned values, so they will be useful merely by their side effects.
    """
    if not steps:
        return df

    logger.info("running Stage '{}' / Step '{}'".format(stage.name, steps[0].name))

    _run_sinks(
        [_before_each_iteration] + (pre_sinks or []),
        stage=stage, step=steps[0], df=df
    )

    step_output = steps[0](df=df)
    #  just capture the data df 'df' if more than one value is returned
    df = extract_signal(step_output)

    _run_sinks(
        [_after_each_iteration] + (post_sinks or []),
        stage=stage, step=steps[0], df=df, step_output=step_output
    )

    yield df

    df = yield from _run_gen(stage, steps[1:], df, pre_sinks, post_sinks)

    return df


def _run_sinks(sinks, **kwargs):
    """
    Invokes all of the passed 'sinks'.
    'kwags' are assumed to be all the available at the respective execution time that '_run_sinks' is invoked.
    Each sink is required to ask (by name) the arguments it requires to be injected.
    """
    for sink in sinks:
        _call_with_requested_args(sink, **kwargs)


def _before_each_iteration(stage, step, df):
    """Callback to run generic code immediately before the execution of each node"""
    for f in stage.registry.get('before_each_step', []):
        _call_with_requested_args(f, stage=stage, step=step, df=df)


def _after_each_iteration(stage, step, df):
    """Callback to run generic code immediately after the execution of each node"""
    for f in stage.registry.get('after_each_step', []):
        _call_with_requested_args(f, stage=stage, step=step, df=df)


def _print_dict_as_arglist(d):
    comma_separated = ", ".join([
        (color_text("{}", 'green') + color_text("=", attrs=['bold']) + color_text("{}", 'magenta')).format(k, v)
        for k, v in d.items() if not k.startswith('__')
    ])
    arg_list = (color_text("(", attrs=['bold']) + "{}" + color_text(")", attrs=["bold"])).format(comma_separated)
    return arg_list
