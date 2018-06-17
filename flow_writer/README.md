
# Data Flow Abstractions
When constructing complex pipelines we need a programming model that lets us concentrate on the logical composition of the data transformations, rather than the details regarding execution or error handling.
The design should be as declarative as possible.

When designing and constructing data pipelines, there are some aspects that deserve our consideration:
- Ease of use
- Testability
- Execution workflow control
- Out of the box documentation

## The Problem
When gluing toguether complex transformations, with large number of steps, the code complexity can easily scale out to levels where the code just stops to be manageable.
```python
def pipeline(df, param1, param2, param3, ...):
    
    df_enriched = enrich_host_information(df)
    
    df_http = df_enriched.filter(lambda x: x['protocol'] == 'HTTP')
    
    df_traffic_statistics = calculate_statistics(df, ['mean', 'avg', 'max'], columns=param1)
    
    # ...
    
    df_infrequent_users = df_traffic_statistics.filter(lambda x: x['#requests'] < param3)
    
    # ...
    
    return df_result
```

Besides the fact that we are centralizing the parameters this style of code is still manageable.

It might however be hard to debug intermediary steps, but still testing is straightfoward.

But then the side effects come:
    - writing to storage
    - handling errors
    
```python   
def pipeline(df, param1, param2, param3, ...):
    
    #  validate schema of the incomig signal 'df'
    if df.columns != ['app_protocol', 'bytes_in', 'src_username', 'src_hostname']:
        raise Exception
        
    if df['bytes_in'].type != 'IntegerType':
        raise Exception
    
    df_enriched = enrich_host_information(df)
    
    df_http = df_enriched.filter(lambda x: x['protocol'] == 'HTTP')
    
    
    # write intermediary steps
    write_df('s3://pipeline_output/intermediary_http')
    
    
    df_traffic_statistics = calculate_statistics(df, ['mean', 'avg', 'max'], columns=param1)
    
    # ...
    
    df_infrequent_users = df_traffic_statistics.filter(lambda x: x['#requests'] < param3)
    
    # ...
    
    # write final result
    write_df('s3://pipeline_output/end_result')
    
    return df_result
```  
- Testing has become considerably more difficult, because now we need to manage these side effects when testing.

- Modularity and usability has decreased, since we are now fixing the places where the side effects occur.

- Readbility of the code is also affaceted.

- More generally this style of code moves us away from our primary goal: just focus on data transformations. 

There is a need of complexity control.

If we can restraint the programming model with certain constraints we can ensure some desired properties and factor out behaviour common to all pipelines.

## Abstractions

There are currently two main abstractions: `Pipeline` and `Stage`.

A `Stage` is the most granular unit and it serves the purpose of grouping a set of logical related steps. A `Pipeline` glues together various stages that belong to the same data flow.

The developer thus has two levels of abstracting to compose the flows.

## Basic usage

To reduce the boilerplate and avoid the use of classes that need to implement a certain contract everytime a new step is defined, we are making use of currying.

There is a required operation to lift each granular step with `@pipeline_step`. By doing so, each `step` is now allowed to be integrated with the data flow constructs.
```python
from src.flow import pipeline_step

@pipeline_step()
def step_a(df, param_1, param_2):
    pass

@pipeline_step()
def step_b(df, param_1):
    pass
```

The only constraints each `step` needs to respect is to be of a variant of the type:
```
Data[_] -> Data[_]
```
It should receive, at some point, a signal with, and in the end return a signal of the same type.

Where `Data[_]` is a type constructor for some type.
Additionally each step should receive extra arguments that parametrize its behaviour, positional or named.

Note that this implementation does not assume the use of a particular type constructor, that serves as a data container. `df` will could be `pyspark.sql.DataFrame` or `pandas.Dataframe` or any other data container, as long as the contract is respected.

```python
@pipeline_step()
def transform(df, method='linear', thresold=100):
    pass
```

By default each step allows the rebinding of its parameters. We can however disable this behavior by setting the flag `allow_rebinding` to false.
```python
@pipeline_step(allow_rebinding=False)
def transform(df, method='linear', thresold=100):
    pass
    
#  throws error
transform_log = transform(mehtod='log')
```

If the flag is not set, this behaviour is allowed.


After having defined steps we can further construct stages:
```python
@pipeline_step()
def step_a(df, tokenize):
    return df

@pipeline_step()
def step_b(df, interpolate):
    return df

@pipeline_step()
def step_c(df, groupby):
    return df

@pipeline_step()
def step_d(df, groupby):
    return df

preprocess = Stage("preprocess",
    step_a(tokenize=True),
    step_b(interpolate='linear'),
)

extract_sessions = Stage("feature-extraction",
   step_c(groupby='src_user_name'),
   step_d(keep_source=False, threshold=100),
)
```

Since the operators are curried, we can bind the arguments one each time, or multiple in one step

```pyhton
step_d(keep_source=False, threshold=100)
#  equivalent to
step_d(keep_source=False)(threshold=100)
```

When passing steps as arguments we should lock just only the relevant parameters, leaving the free variable `df` unbind.
That will eventually be used to design Pipelines at a higher level:

```python
pipeline = Pipeline("Pipeline AgainstHackers",
    preprocess,
    extract_sessions
)
```

the pipeline abstraction can be useful for documentation purposes:

```python
print(pipeline)
# output

Pipeline: 'Pipeline A'
---------
---------
Stage: 'preprocess'
------
        Step: 'step_a(tokenize=True)'
        Step: 'step_b(interpolate='linear')'
Stage: 'feature-extraction'
------
        Step: 'step_c(groupby='src_user_name')'
        Step: 'step_d(keep_source=False, threshold=100)'
```

```python
print(preprocess)
# output

Stage: 'preprocess'
------
        Step: 'step_a(tokenize=True)'
        Step: 'step_b(interpolate='linear')'
```

The documentation facilities should be enhanced in the future. If we can embed detailed documentation regarding the pipeline, the objects will turn out to be self documenting, which can be very useful to share information.

To add a description to the pipeline:

```python
pipeline = pipeline.with_description('Smart pipeline to keep the hackers at distance')
print(pipeline)
# output

Pipeline: 'Pipeline A'
* Smart pipeline to keep the hackers at distance *
---------
---------
Stage: 'preprocess'
# ...
```
There is also a mutable version `pipeline.add_description()`

The same applies to `Stage`.

```python
preprocess = preprocess.with_description('Data cleaning, removing NaNs')
print(preprocess)
# output

Stage: 'preprocess'
* Data cleaning, removing NaNs' *
------
        Step: 'step_a(tokenize=True)'
        Step: 'step_b(interpolate='linear')'
```

The pipeline supports other operations
```python
pipeline.get_num_stages() # 3
pipeline.get_num_steps() # 4
pipeline.get_stage_names() # ["preprocess", "feature-extraction", "postprocess"]
```

The `Stage` abstraction supports similar operations.

Since the `Pipeline` is the main data flow abstraction, we might want to construct a `Pipeline` without resorting to `Stages`, simply by passing a variable number of steps.
This behaviour is also accepted and a anonymous default stage is created.

```python
pipeline = Pipeline("Pipeline AgainstHackers",
    step_a,
    step_b
)

print(pipeline)
# output
Pipeline: 'Pipeline AgainstHackers'
---------
---------
Stage: 'Anonymous Stage'
------
	Step: 'step_a'
	Step: 'step_b'
```

## Composition

After having blocks of execution flow properly tested that are generic enough, we should be able to compose data flow abstractions into pipelines that glue toguether some higher level behaviour.

All of the available primitives to compose data flows assume immutable constructs, always returning a new object with the new requested behaviour, leaving the previous ones unaltered.

Add a new stage to the pipeline:
```python
preprocess = Stage("preprocess", step_stringify)
transform = Stage("transform", step_just_adults(threshold=18))

pipeline = Pipeline("Pipeline A",
    preprocess,
    transform
)

postprocess = Stage("postprocess", step_rename)

pipeline_extended = pipeline.with_stage(postprocess)
```

We then have two operators (`andThen` and `compose`) to compose two pipelines `p` and `q`
```python
p.andThen(q) # returns a new pipeline equivalent to q(p)
p.compose(q) # returns a new pipeline equivalent to p(q)
```

To create new pipelines with additional nodes, use `with_step` and pass the intended location path of the new node to be inserted.
```python 
pipeline_ = pipeline.with_step(step_five).after('stage two/step_three')

pipeline_ = pipeline.with_step(step_five).before('stage three/step_four')
```

The `Stage` construction has analogous features, at a more granular level.

## Execution flow

Control of the execution flow is a major concern in a data flow abstraction, since it greatly impacts the expressiveness and testability of the constructs.

To run the pipeline end to end:

```python
df_result = pipeline.run(df)
```

We can also run just a specific step of a pipeline.
```python
df_result = pipeline.run_step(df, 'step_a')
```

Or run from the beginning until the specified location on the data flow.

At the stage level:
```python
df_result = pipeline.run_until(df, 'stage preprocess/step_a')
```

Or alternatively, until a specific node.
```python
df_result = pipeline.run_until(df, 'stage preprocess/step_a')
```

For improved workflow control, we should be able to run the pipeline in a lazy fashion, computing the stages as needed.
```python
generator = pipeline.run_iter(df)

df_intermediary_1 = next(stage_generator)
df_intermediary_2 = next(stage_generator)
df_intermediary_3 = next(stage_generator)
```

The execution flow is under the control of the caller.
Each step is generated when demanded. Can be useful to decide midway if an execution should proceed or stop due to some condition.

To run just `n` stages (from 1 to n):
```python
df_result = pipeline.run_n_stages(df, n)
```

To run just a particular named stage (str):
```python
df_result = pipeline.run_stage(df, stage)
```
To run just a particular positional stage:
```python
df_result = pipeline.run_nth(df, n)
```

The `Stage` construction has analogous features, at a more granular level.

We should also have control over the execution flow at the most granular operation, ie the `step`.
The caller should be able to run a determined number of steps.

`run_iter_steps(df)` returns a generator that will iterate on demand through every granular step that compose the pipeline.

```python
step_generator = pipeline.run_iter_steps(df)

for intermediary_result in step_generator:
    # ...
```

The caller can also demand the strict evaluation of `n` steps:

```python
df_at_step_two = pipeline.run_n_steps(df, 2)

#  this equality should hold
assertEqual (
    pipeline.run(df),
    pipeline.run_n_steps(df, pipeline.get_num_steps())
)
```

It might also be useful to have control over all stages of the execution lifecycle.
We can execute generic behaviour immediately before or after each `step` or `stage`.

There are two places where this generic behaviour can be executed: `prerun` and `postrun`.
We can register one or more functions to be executed in accordance with this cycle.
A simple use case for this mechanism is to assess the schema at the end of each step
```python
def f(df):
    df.printSchema()
pipeline = pipeline.before_each_stage(f)
```
Or similarly, before each step,
 
```python
pipeline = pipeline.before_each_stage(f)
```
The signal `df` will be inject into the function `f` at execution time.
We can allow more complex behaviours by requesting the injection of both the `pipeline` and the `stage` object.

```python
def f(df, pipeline, stage):
    print(pipeline.name)
    print(stage.name)
    df.printSchema()
    
pipeline = pipeline.before_each_stage(f)
```
Similar methods can be found in a `Stage`.


## Templating

The modularity and reusability of a data flow pipeline hsa a major impact in the use of the library.

The ideal library would allow the designing of basic data flow templates that can serve as base to more custom behaviour as needed.
Some features towards this goal are already implemented.

Lets start with the Stage abstraction. 
At construct time, the steps composing the stage are normally bound to operate under certain parameters
This bindings can be modified by passing a object with the new intended values.

Every closed value that is not referred in the new object, stays unmodified.
This operation returns a new Stage object, leaving the base one intact.

The templating operating are thereby all immutable.

Say we have two pipeline steps already in place:

```python
@pipeline_step()
def clean_webproxy_data(df, filter_nulls=True):
    # ...

@pipeline_step()
def preprocess_traffic_distribution(df, method='linear', threshold=100):
    # ...
```

From here we can create a base `Stage` that will be used for all of the subsequent analysis concerning webproxy traffic distribution.

 ```python
traffic_analysis = Stage("prepare data for traffic distribution analysis",
   clean_webproxy_data,
   preprocess_traffic_distribution(method='linear', threshold=100)
)
```

We can work on top of this basic template by deriving a new `Stage` with the custom behaviour we need.

```python
log_traffic_analysis = traffic_analysis.rebind({
    'preprocess_traffic_distribution': {
        'method': 'log'
    }
})
```

This templating operations highly facilitate development and can even be used for use cases that require the spanning of hundreds of different data flows with small variations for tasks like hyperparameter tuning.

Note that the stage `traffic_analysis` remains unchanged, since the operations are all immutable.

## Validations
Each step can receive a set of constraints which the input signal must respect in order to proceed the execution flow.
There are some validations which may be ad hoc in nature and that don't make sense to be attached strictly to the step logical code.

Situations where the validation are general and apply to many steps might also constitute a valid use case and help to reduce code duplication.
In these situations we can inject validations into the data flow, that will check the input signal immediately before it enters the respective step and ensure that it gathers the necessary conditions. Failure to pass the validations will mean that a custom error will be thrown and the execution stopped, or the developer can declare which channel to run to handle the exceptional data.
By doing so, we shift some error handler control to the data flow layer.

The validations are `Callable`s whose only constraint is to be of the type:
```
(InputSignal) -> Boolean
```
Given some validations:
```python
def has_columns(df):
    return ['age', 'air_speed'] in df.schema.names

def has_min_partitions(df):
    return df.rdd.getNumPartitions() > 10
```
We can then add validations to any pipeline step 
```python
@pipeline_step(validate=[has_columns, has_min_partitions])
def step_cast_to_int(df, column):
    return df.withColumn(column, df.age.cast(IntegerType()))
```
When just a single validation is needed we remove the need for the enclosing list
 
```python
@pipeline_step(validate=has_columns)
def step_cast_to_int(df, column):
    return df.withColumn(column, df.age.cast(IntegerType()))
```

By default, a validation failure means the raise of a `SignalNotValid` exception and the termination of the execution.
This exception provides useful information to the caller, for debugging purposes:

```
exception SignalNotValid: {
    'message': 'Signal failed the validation check',
    'failed validation': "'has_age', nth validation '0'",
    'at node': "step 'step_cast_to_int'"
}
```

## Side Effects

It is also useful to be able to control the side effects of a pipeline. The focus of interest of a data flow is the actual transformations being performed. The addition of steps with side effects to just add noise to what is actually being done.
By doing so, we separate concerns and the management of the workflow gets easier.

Given a step with side effects:
```python
write_step = write_df(format='csv')
```

We can create a new version of a `Pipeline` or  `Stage`, that keeps track of the current steps with side effects.
This can be done by passing the step name:

```python
stage = stage.with_side_effect(write_step(path='s3n://...')).after('step_calculate_threshold')

stage = stage.with_side_effect(write_step(path='s3n://...')).before('step_calculate_threshold')
```

Or equivalently, by passing a positional index:

```python
stage = stage.with_side_effect(write_step(path='s3n://...')).after(nth_step=1)

stage = stage.with_side_effect(write_step(path='s3n://...')).before(nth_step=1)
```

Side effects to a `Pipeline` can be introduced after a `Stage`:
```python
pipeline = pipeline.with_side_effect(write_step(path='s3n://...')).after('stage calculate index')

pipeline = pipeline.with_side_effect(write_step(path='s3n://...')).before('stage calculate index')
```

Or if more finer control is needed, we can use a directory-like selector to specify where the side effect should be invoked.
```python
pipeline = pipeline.with_side_effect(write_step(path='s3n://...')).after('preprocess/step_a')

pipeline = pipeline.with_side_effect(write_step(path='s3n://...')).before('postprocess/step_rename_columns')
```

## Dependencies managing

There are computational steps that have dependencies. Consider a step that normalizes a column between 0 and 1. In production this steps needs to have available some pre-calculated data, in this case the maximum and minimum of the seen values for that column. Otherwise the step cannot have an overview of the historical data. We realize then that are some steps that will need calculate some data over the full historical dataset, i.e. a training phase.
We realize then that a `pipeline` should be able to run in two different modes:

- the default one, where dependencies are not being watched, they should be provided at call time, or injected if there are available handlers (source nodes) for the dependencies required. If dependencies handlers are not available, the dependencies will need to be calculated from the current data `df`: `pipeline.run(df)`

- a `fit` mode that will monitor the steps outputs in order to save the pre-calculated data. A handler (a sink node) must be given for each dependency: `pipeline.run(df)`. In this mode, `df` may refer to historical dataset

We should therefore, as noticed, be able to register dependencies handlers (source nodes) that will load a dependency by execution an arbitrary function and inject that dependency to the correspondent step.

Then we can call:

```
pipeline_ = pipeline.lock_dependencies(data)
```

This basically will traverse all of steps in `pipeline`, check if there are registered source nodes for each one and, in case there are, inject and bind those dependencies into the steps and return a new pipeline with the new data.
By doing so, the pipeline is *loaded* and should have available all the necessary pre-calculated data to transform data without any need for calling any sort of `fit` methods.

To register source/sinks handlers for each dependency we just need to create a `dict` with an entry for every dependency a step has.

```
dependencies = {
    "tokenizer": [loader, writer]
}
```

```
def loader(path):
    with open(path, 'rb') as f:
        return pickle.load(f)

def writer(path, model):
    with open(path, 'wb') as f:
        pickle.dump(model, f)
```

We can then pass these handlers to the lifting decorator:

```
@pipeline_step(dm=dependencies)
def step_tokenize(df, col, tokenizer=None):
    if not tokenizer:
        tokenizer = df.name.unique().tolist()
    df.loc[:, 'tokenizer'] = df.apply(lambda r: tokenizer.index(r[col]), axis=1)
    return df, tokenizer
```

If `tokenizer` is not provided, the step should calculate it and return it. If the pipeline is running with `fit` we will catch that specific output and invoke the associated handler `writer(path)(tokenizer)`.
When we load a pipeline `pipeline.lock_dependencies()`, the `loader(path)` is invoked and its returned value will be inject into `step_tokenize()`, overwriting `tokenizer=None`.

A step can also have more than one dependency:
```
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
```

For a step with one than more dependencies, if we need to call the pipeline with `pipeline.fit`, in order for the dataflow layer to call the correct sink nodes that are handling each dependency, we just require the step to return its fitted dependencies in the same order that they are declared in the function signature.

Valid:
```
def step_tokenize(df, col, tokenizer=None, ones=None):
    #  ...
    return df, tokenizer, ones
```

Invalid:
```
def step_tokenize(df, col, tokenizer=None, ones=None):
    #  ...
    return df, ones, tokenizer
```

If necessary the writer callback can also require the data signal `df` to be injected. In this case the function will have access to the signal as it returned by the node.
```
def writer(path, df, model):
    with open(path, 'wb') as f:
        pickle.dump(model, f)
```
or (order invariant)
```
def writer(path, model, df):
    with open(path, 'wb') as f:
        pickle.dump(model, f)
```
 In this case `writer` will be invoked with the dependency it is managing and with the data signal `df`.


 **Resolving the arguments**

 We have described the arguments in our flow that need handlers to manage their lifecycles.

 The arguments of these functions are similar to placeholders that will need a value at runtime.

To provide this data we will need to construct a dictionary `data`

`data` should be a dict with the values to inject into the functions that are managing each dependency.

It has the following structure:

```
data :: stage/step/step_arg/function/function_arg -> value
```

For example:

```
dependencies = {
    "tokenizer": [loader, writer]
}

def loader(path):
    with open(path, 'rb') as f:
        return pickle.load(f)

def writer(path, model):
    with open(path, 'wb') as f:
        pickle.dump(model, f)

@pipeline_step(dm=dependencies)
def step_tokenize(df, col, tokenizer=None):
    if not tokenizer:
        tokenizer = df.name.unique().tolist()
    df.loc[:, 'tokenizer'] = df.apply(lambda r: tokenizer.index(r[col]), axis=1)
    return df, tokenizer

pipeline = Pipeline("demo", Stage("stage", step_tokenize(col="col")))
```

Given this pipeline, we can call `pipeline.lock_dependencies` by providing a dictionary `data` whose values are used to resolve the needed dependencies:

```
data = {
    'stage/step_tokenize/tokenizer/loader/path': 's3://bucket/tokenizer',
    'stage/step_tokenize/tokenizer/writer/path': 's3://bucket/tokenizer/v1.0.1',
}

pipeline_ = pipeline.lock_dependencies(data)
```

Note that this `data` can receive arguments both for source and sink nodes.
usually the source nodes need the full argument list to be available before the actual run. Whereas a sink node might have partially available (for arguments external to the dataflow context) while others will just be available at run time after the execution of specific steps.

For instance, `writer(path, model)` is a sink node that needs a `path` (a location to write) and a `model` the calculated data structure to be persisted.
We can use `lock_dependencies` to partially lock this handler by doing:

```
data = {
    'stage/step_tokenize/tokenizer/writer/path': 's3://bucket/tokenizer/v1.0.1',
}

pipeline_ = pipeline.lock_dependencies(data)
```

So now the handler knows where it is supposed to write to. We cannot, however, lock the `model` argument. This can just be provided to the handler at run time and is,
therefore, responsibility of the dataflow system. A distinct situation is expected for the `loader` handler, since the flow needs to have all of its dependencies resolved before initiating the execution.

A sink definition is just useful for performing side effects with the result of a computation.

Nevertheless there may be use cases where you actually do not need to receive anything from the underlying dataflow, so we create, for the writer, a curried version of it where the execution must be explicit. By doing so, we are able to bind the argument list in its whole and still defer the execution to a later time.

Note that this is in contrast with a source node, that needs all its arguments so that we can retrieve its value and provide the computation with its external sources.

We might just want to lock one of the source dependencies and leave the other one to be calculated during the execution of the flow. To do so, we need to disable the validations mechanism:
```
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

pipeline = Pipeline("demo", Stage("stage", step_tokenize(col="col")))
```

```
#  all source dependencies resolved. the validations will check
data = {
    'stage/step_tokenize/tokenizer/loader/path': 's3://bucket/tokenizer',
    'stage/step_tokenize/ones/loader/path': 's3://bucket/ones',
    'stage/step_tokenize/tokenizer/writer/path': 's3://bucket/tokenizer/v1.0.1',
}

pipeline_ = pipeline.lock_dependencies(data)

#  raises an SourceArgumentsNotResolved('stage/step_tokenize/ones/loader/path') exception
data = {
    'stage/step_tokenize/tokenizer/loader/path': 's3://bucket/tokenizer',
    'stage/step_tokenize/tokenizer/writer/path': 's3://bucket/tokenizer/v1.0.1',
}

pipeline_ = pipeline.lock_dependencies(data)

#  disables validations. just binds the 'tokenizer' dependencies. the 'ones' will be calculated at run time.
data = {
    'stage/step_tokenize/tokenizer/loader/path': 's3://bucket/tokenizer',
    'stage/step_tokenize/tokenizer/writer/path': 's3://bucket/tokenizer/v1.0.1',
}

pipeline_ = pipeline.lock_dependencies(data, val_args_source=False)
```






