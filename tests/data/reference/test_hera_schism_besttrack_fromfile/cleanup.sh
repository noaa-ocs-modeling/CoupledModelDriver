#!/bin/bash
DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

# clean spinup files
rm -rf ${DIRECTORY}/spinup/outputs/*

# clean run configurations
rm -rf ${DIRECTORY}/runs/*/outputs/*
