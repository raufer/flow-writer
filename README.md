## Flow Writer

### Proof of concept for constructing ML pipelines

This repository serves as first trial for a proof of concept for what I believe to be a proper way of designing and managing machine learning pipelines.
The proposed design pattern is framework agnostic makes extensive use of *dataflow programming* as well as *currying* concepts.

This blog post discusses the motivation behind the work with great detail and provides examples of usage.
Keeping data transformations in their natural form, as functions, results in a cleaner way of creating workflows.

We propose now a minimalist thin layer to support the development of ML pipelines. It should be framework agnostic and assume very little about the user might want to do. The abstractions should provide a simple and effective way to manage complex machine learning workflows.

#### Desirable properties

- Testability concerns should be addressed at the library design phase. The pipeline and associated components should be easily tested.

- The caller should have complete control over the execution workflow. This is specially important for debugging, where we might need to probe the signal at an intermediary step for a closer inspection. A lazy execution mode should also be available for a finer execution control.  

- Side effects managing. Error handling and persistence are two important in a production environment and they should be handling at the library layer, which creates the opportunity to factor out behaviour common to all of the pipelines. The data scientist ideally should not need to worry about these boring details.

- Templating. We should be able to start from a base pipeline and derive new ones by making changes to the parameters of the base computational description. A possible scenario of this feature in ML pipelines would be parameter tuning. You should be able to easily span new similar pipelines by applying small variations to a base one.

- Self documenting. To share with other team members, or for more generic documentation purposes, the pipelines should ideally carry with them a description of the transformations being applied.

Visually, these are the first different modules of the proposed pipeline design pattern.

[IMG]

Ideally, a data scientist should be focused almost entirely on the *data transformations* layer, defining simple, testable functions. These functions should then be effortless *lifted* to the pipelining context, which adds the required extra functionality, respecting the desirable design properties.
