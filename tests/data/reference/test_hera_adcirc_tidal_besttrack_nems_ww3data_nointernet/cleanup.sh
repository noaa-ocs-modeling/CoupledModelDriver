#!/bin/bash
DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

# clean spinup files
rm -rf ${DIRECTORY}/spinup/PE* ${DIRECTORY}/spinup/ADC_* ${DIRECTORY}/spinup/max* ${DIRECTORY}/spinup/partmesh.txt ${DIRECTORY}/spinup/metis_graph.txt ${DIRECTORY}/spinup/fort.16 ${DIRECTORY}/spinup/fort.80 ${DIRECTORY}/spinup/fort.6*

# clean run configurations
rm -rf ${DIRECTORY}/runs/*/PE* ${DIRECTORY}/runs/*/ADC_* ${DIRECTORY}/runs/*/max* ${DIRECTORY}/runs/*/partmesh.txt ${DIRECTORY}/runs/*/metis_graph.txt ${DIRECTORY}/runs/*/fort.16 ${DIRECTORY}/runs/*/fort.80 ${DIRECTORY}/runs/*/fort.61* ${DIRECTORY}/runs/*/fort.62* ${DIRECTORY}/runs/*/fort.63* ${DIRECTORY}/runs/*/fort.64*
