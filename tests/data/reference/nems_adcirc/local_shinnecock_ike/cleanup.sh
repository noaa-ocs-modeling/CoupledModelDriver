DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

# clean spinup files
pushd ${DIRECTORY}/spinup >/dev/null 2>&1
rm -rf PE* ADC_*
rm max* partmesh.txt metis_graph.txt
rm fort.16 fort.6* fort.80
popd >/dev/null 2>&1

# clean run configurations
for hotstart in ${DIRECTORY}/runs/*/; do
    pushd ${hotstart} >/dev/null 2>&1
    rm -rf PE* ADC_*
    rm max* partmesh.txt metis_graph.txt
    rm fort.16 fort.6* fort.80
    popd >/dev/null 2>&1
done
