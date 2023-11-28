# OpenDSS Simulation Ingestor

## What is OpenDSS?
[OpenDSS](https://www.epri.com/pages/sa/opendss) is an open source tool for simulating electrical distribution networks. For us, it may be useful for generating realistic grid data from particular physical contexts---for example, measurements on two ends of a line, or two ends of a transformer---without any data privacy concerns. In this sense, it is especially useful for the Dominion Apps project, for which we would otherwise need to prototype and test on Dominion's PMU data, access to which is restricted.

## This Repo
This repo contains notebooks demonstrating how to run simulations (ie powerflow solutions) and retrieve data via OpenDSS's Python API, called [`OpenDSSDirect`](https://dss-extensions.org/OpenDSSDirect.py/index.html). The notebooks work with IEEE network models that come prepackaged with OpenDSS and have also been committed to this repo under the `Models` folder.

### Getting Started
To get started, install OpenDSS on your local machine from [here](https://sourceforge.net/projects/electricdss/files/). Next, install `OpenDSSDirect` following the instructions [here](https://dss-extensions.org/OpenDSSDirect.py/notebooks/Installation.html). If installation has been successful, you should be able to run the notebooks in this repo. It is recommended you begin with `Intro to Simulation with OpenDSS` which will introduce the main steps in any simulation, and demonstrate how to obtain the resulting data.
