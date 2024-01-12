Terms used in ert
=================

Parameter
---------
A parameter is a numeric quantity we are trying to estimate. When we try to estimate a parameter we always know a little bit about it, but with some uncertainty. For example, we can't say for sure that it is 10 degrees outside, but we might be able to say it is 10 degrees +/- 2 degrees outside. 

Realization
-----------
A realization contains specific "realized" numeric values for multiple parameters. I.e., it now gives concrete values to parameters. In other words, a realization can be seen as an educated guess of what reality looks like. Example: If we know the temperature outside is 10 degrees +/- 2 degrees, some "realizations" of this can be that it is 8 degrees, 8.5 degrees, 11 degrees, 12 degrees, 10.5 degrees and so on. A realization of N parameters will thus contain N "guesses" of what those numbers might be, within the constraints of our uncertainty model.

Ensembles
---------
When we run ERT, it creates one or more ensembles. Each ensemble is nothing more than a set of realizations. The most important ensemble to remember is the prior ensemble. This is the first ensemble that is sampled from our specified parameters. Ert then generates new ensembles, each ensemble based on the one generated before. The first created ensemble is always called the prior, and corresponds to iteration 0. The last ensemble 

Iteration
---------
This denotes the "index" of the ensembles and is tied to the iteration of the ert run. It is a very general term used for many other things, but in this case it refers to which iteration of the ensemble kalman filter the ensemble pertains to.

Experiment
----------
An experiment refers to all results of running an experiment in ert once. The run results will always be a set of ensembles.
