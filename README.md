# A Novel Graph-Based Approach to Identify Opinion Leaders in Twitter

This repository contains the code accompanying the paper **"A Novel Graph-Based Approach to Identify Opinion Leaders in Twitter."** The paper presents a methodology for detecting influential users on Twitter using graph analysis techniques. Our approach focuses on identifying opinion leaders based on their influence during specific timeframes, independent of when their tweets were originally generated.

## Overview

In our study, we aimed to understand the dynamics and influential actors on Twitter by leveraging graph-based methods. By analyzing the network structure and interactions, we identified key opinion leaders whose influence is significant within a given month. This repository includes the scripts and tools necessary to reproduce our results and apply the methodology to other datasets.

## Features

- **Graph Construction:** Tools to construct interaction graphs from Twitter data.
- **Influence Analysis:** Algorithms to identify and rank opinion leaders based on their influence within specific timeframes.
- **Psycho-Linguistics Analysis:** Code to perform Psycho-Linguistics Analysis using opinion leader's tweets.
- **Data Handling:** Scripts for preprocessing and handling Twitter data.
- **Visualization:** Functions to visualize the interaction graphs and the identified opinion leaders.

## Installation

To get started, clone the repository and install the required dependencies:

```bash
git clone https://github.com/yourusername/graph-based-opinion-leaders.git
cd graph-based-opinion-leaders
```

## Usage

1) Downloads X's Tweets referring using their API.
2) Split Tweets in files month-by-month.
3) Execute 'Codice_Preparazione_Dati.py'
4) Create a Neo4j project and execute queries contained in 'Query Creazione Grafo Neo4j.txt' and then in 'Query Algoritmi Neo4j.txt'.
5) Execute 'Query Conversione Neo4j a Csv.txt' to convert Neo4j graph in csv format to be imported in NetworkX.
6) Execute 'Codice_Analisi_con_Risultati.ipynb' to calculate results.

## Data

This repository does not include the raw Twitter data due to privacy and usage restrictions.

## Contributing

We welcome contributions from the community. Please refer to the [Contributing Guidelines](CONTRIBUTING.md) for more details.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

For any questions or issues, please open an issue in this repository or contact us at [luca.mariotti@unimore.it](mailto:luca.mariotti@unimore.it).
