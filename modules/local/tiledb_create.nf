process TILEDB_CREATE {
    tag "$s3_url"
    label 'process_medium'


    conda "conda-forge::tiledb tiledb::tiledbsoma-py"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/tiledb-soma:latest' :
        'dockerhub/tiledbsoma-py:latest' }"

    input:
    val s3_url
    tuple val(meta), val (h5ad_file)
    path rd

    output:
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    """
    #!/usr/bin/env python3
    import scanpy as sc
    #import tiledb.cloud
    import tiledbsoma
    import tiledbsoma.io
    import tiledbsoma.logging

    #sc.pp.calculate_qc_metrics(${h5ad_file}, inplace=True)

    import json
    with open("${rd}", "r") as f:
        rd_json = f.read()
    
    rd = tiledbsoma.io.ExperimentAmbientLabelMapping.from_json(rd_json)

    # if filtered in h5ad_file
    if "filtered" in "${h5ad_file}":
        tiledbsoma.io.from_h5ad(experiment_uri="${s3_url}", input_path="${h5ad_file}", measurement_name="RNA",registration_mapping=rd)

    with open("versions.yml", "w") as f:
        f.write("'${task.process}':\\n")
        f.write(f"    tiledbsoma: {tiledbsoma.__version__}\\n")
        f.write(f"    scanpy: {sc.__version__}\\n")
    """
}
